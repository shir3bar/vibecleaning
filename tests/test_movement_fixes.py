from pathlib import Path
import struct
import sys
import base64
import csv
import io
import json
from math import isclose

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.execution import create_analysis, create_step
from app.state import get_dataset_artifact, load_project_state
from app.web import create_app
from examples.movement.routes import (
    ANNOTATE_SCOPE_TEMPLATE_PATH,
    GENERATE_REPORT_SCRIPT,
    REPORT_ANALYSIS_TEMPLATE_PATH,
    _reviewed_csv_artifact_name,
    register_movement_routes,
)
from examples.movement.report_analysis_template import (
    build_html_report,
    build_individual_profile_html_report,
    build_individual_profile_sections,
    build_issue_sections,
    format_monitoring_span,
    format_temporal_resolution,
    load_rows_with_context,
    recompute_analytical_movement_context,
)
from examples.movement.review_annotations import (
    apply_review_annotations,
    effective_issues_for_fix,
    effective_review_status,
    export_reviewed_csv,
    normalize_annotation,
    point_in_polygon,
    resolve_filter_row_ranges,
)
from examples.movement.bursts import build_auto_bursts
from examples.movement.movement_features import (
    STEP_FEATURE_FIELDS,
    centered_turn_angle_degrees,
    compute_track_movement,
    geodesic_distance_meters,
    initial_bearing_radians,
)
import examples.movement.summary as movement_summary
import examples.movement.review_annotations as movement_review_annotations
from examples.movement.summary import build_movement_fixes, build_movement_overview, diagnose_track_topology

CSV_CONTENT = """eventid,individual,timestamp,longitude,latitude,set,outlier_status,outlier_issue_type,outlier_comments,visible,manually-marked-outlier,algorithm-marked-outlier
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train,suspected,drift,first alpha issue,true,false,true
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train,,,,true,false,false
fix_b_1,beta,2024-01-01T00:30:00Z,-71.0,41.0,test,confirmed,spike,beta confirmed issue,false,false,true
fix_b_2,beta,2024-01-01T01:30:00Z,-71.1,41.1,test,suspected,loop,beta suspected issue,true,true,false
fix_c_1,gamma,2024-01-01T00:45:00Z,-72.0,42.0,train,,,,true,false,false
"""

PROFILE_CSV_CONTENT = """eventid,individual-local-identifier,timestamp,longitude,latitude,study-name,study-id,individual-taxon-canonical-name,source,burst-id,outlier_status,outlier_issue_type,outlier_comments
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,Study A,study_001,Cervus elaphus,movebank.mar2025,burst_a,suspected,drift,Alpha issue
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,Study A,study_001,Cervus elaphus,movebank.mar2025,burst_a,,,
fix_a_3,alpha,2024-01-01T02:00:00Z,-70.2,40.2,Study A,study_001,Cervus elaphus,movebank.mar2025,burst_b,,,
fix_b_1,beta,2024-01-02T00:00:00Z,-71.0,41.0,Study A,study_001,Cervus elaphus,movebank.mar2025,,confirmed,loop,Beta issue
fix_b_2,beta,2024-01-02T01:00:00Z,-71.1,41.1,Study A,study_001,Cervus elaphus,movebank.mar2025,,,,
fix_c_1,gamma,2024-01-03T00:00:00Z,-72.0,42.0,Study A,study_001,Cervus elaphus,movebank.mar2025,,,,
fix_c_2,gamma,2024-01-03T01:00:00Z,-72.1,42.1,Study A,study_001,Cervus elaphus,movebank.mar2025,,,,
"""


def write_movement_csv(path: Path) -> Path:
    path.write_text(CSV_CONTENT, encoding="utf-8")
    return path


def write_profile_csv(path: Path) -> Path:
    path.write_text(PROFILE_CSV_CONTENT, encoding="utf-8")
    return path


def test_compute_track_movement_exposes_only_canonical_step_features():
    records_by_group = {
        ("alpha", "train"): [
            {
                "row_index": 2,
                "fix_key": "late",
                "individual": "alpha",
                "time_ms": 3_600_000,
                "lon": -70.1,
                "lat": 40.1,
            },
            {
                "row_index": 1,
                "fix_key": "early",
                "individual": "alpha",
                "time_ms": 0,
                "lon": -70.0,
                "lat": 40.0,
            },
        ],
    }

    features, stats = compute_track_movement(records_by_group)

    assert tuple(features["early"]) == STEP_FEATURE_FIELDS
    assert features["early"]["time_delta_s"] == 3600.0
    assert isclose(
        features["early"]["speed_mps"],
        features["early"]["step_length_m"] / 3600.0,
        rel_tol=1e-12,
    )
    assert features["late"] == {
        "step_length_m": None,
        "speed_mps": None,
        "time_delta_s": None,
        "turn_angle_deg": None,
    }
    assert not any("anomaly" in key.lower() for row in features.values() for key in row)
    assert stats["alpha"]["seen_step"] == 1


def test_centered_turn_angle_is_signed_and_attached_to_middle_fix():
    previous = {"time_ms": 0, "lon": 0.0, "lat": 0.0}
    center = {"time_ms": 1_000, "lon": 1.0, "lat": 0.0}
    north = {"time_ms": 2_000, "lon": 1.0, "lat": 1.0}
    reverse = {"time_ms": 2_000, "lon": 0.0, "lat": 0.0}

    assert initial_bearing_radians(0.0, 0.0, 1.0, 0.0) == pytest.approx(
        1.5707963267948966
    )
    assert centered_turn_angle_degrees(previous, center, north) == pytest.approx(-90.0)
    assert centered_turn_angle_degrees(previous, center, reverse) == pytest.approx(180.0)
    assert centered_turn_angle_degrees(previous, center, {**north, "lon": 1.0, "lat": 0.0}) is None

    records = {
        ("alpha", "train"): [
            {**previous, "row_index": 1, "fix_key": "a", "individual": "alpha"},
            {**center, "row_index": 2, "fix_key": "b", "individual": "alpha"},
            {**north, "row_index": 3, "fix_key": "c", "individual": "alpha"},
        ]
    }
    features, _stats = compute_track_movement(records)

    assert features["a"]["turn_angle_deg"] is None
    assert features["b"]["turn_angle_deg"] == pytest.approx(-90.0)
    assert features["c"]["turn_angle_deg"] is None


def test_wgs84_inverse_matches_lwgeom_reference_and_handles_high_latitudes():
    # The first pair is the published lwgeom::st_geod_azimuth example.
    assert initial_bearing_radians(7.0, 52.0, 8.0, 53.0) == pytest.approx(
        0.5410385,
        abs=5e-8,
    )
    assert geodesic_distance_meters(7.0, 52.0, 8.0, 53.0) == pytest.approx(
        130_359.3,
        abs=0.1,
    )
    # A route close to the pole and a dateline crossing remain finite geodesics.
    assert geodesic_distance_meters(0.0, 89.0, 90.0, 89.0) == pytest.approx(
        157_954.97,
        abs=0.02,
    )
    assert geodesic_distance_meters(179.0, 70.0, -179.0, 70.0) == pytest.approx(
        76_369.66,
        abs=0.02,
    )


def test_centered_turn_angle_matches_move2_for_equal_timestamps():
    previous = {"time_ms": 0, "lon": 0.0, "lat": 0.0}
    center = {"time_ms": 0, "lon": 1.0, "lat": 0.0}
    following = {"time_ms": 1_000, "lon": 1.0, "lat": 1.0}

    assert centered_turn_angle_degrees(previous, center, following) == pytest.approx(-90.0)


def test_build_auto_bursts_uses_strict_gap_threshold_and_preserves_mapping():
    records = [
        {
            "row_index": 3,
            "fix_key": "fix_2",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 7_201_000,
            "position": [-70.2, 40.2],
        },
        {
            "row_index": 1,
            "fix_key": "fix_0",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 0,
            "position": [-70.0, 40.0],
        },
        {
            "row_index": 2,
            "fix_key": "fix_1",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 3_600_000,
            "position": [-70.1, 40.1],
        },
    ]

    bursts = build_auto_bursts(records, burst_gap_seconds=3600)

    assert bursts[0] == {
        "burst_id": "alpha:train:burst_000000",
        "burst_idx": 0,
        "individual": "alpha",
        "set_name": "train",
        "start_fix_key": "fix_0",
        "end_fix_key": "fix_1",
        "start_time_ms": 0,
        "end_time_ms": 3_600_000,
        "fix_count": 2,
        "burst_gap_seconds": 3600.0,
        "fix_keys": ["fix_0", "fix_1"],
        "path": [[-70.0, 40.0], [-70.1, 40.1]],
        "path_length_m": pytest.approx(14003.34, rel=1e-4),
        "median_step_m": pytest.approx(14003.34, rel=1e-4),
    }
    assert bursts[1]["burst_id"] == "alpha:train:burst_000001"
    assert bursts[1]["fix_keys"] == ["fix_2"]


def test_build_auto_bursts_resets_index_for_each_track():
    records = [
        {
            "row_index": 2,
            "fix_key": "alpha_train_1",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 7_200_000,
            "position": [-70.1, 40.1],
        },
        {
            "row_index": 1,
            "fix_key": "alpha_train_0",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 0,
            "position": [-70.0, 40.0],
        },
        {
            "row_index": 3,
            "fix_key": "alpha_test_0",
            "individual": "alpha",
            "set_name": "test",
            "time_ms": 0,
            "position": [-70.2, 40.2],
        },
        {
            "row_index": 4,
            "fix_key": "beta_train_0",
            "individual": "beta",
            "set_name": "train",
            "time_ms": 0,
            "position": [-71.0, 41.0],
        },
    ]

    bursts = build_auto_bursts(records, burst_gap_seconds=3600)

    assert [(burst["burst_id"], burst["burst_idx"], burst["fix_keys"]) for burst in bursts] == [
        ("alpha:test:burst_000000", 0, ["alpha_test_0"]),
        ("alpha:train:burst_000000", 0, ["alpha_train_0"]),
        ("alpha:train:burst_000001", 1, ["alpha_train_1"]),
        ("beta:train:burst_000000", 0, ["beta_train_0"]),
    ]


def test_build_movement_fixes_filters_multiple_individuals(tmp_path):
    csv_path = write_movement_csv(tmp_path / "movement.csv")

    payload = build_movement_fixes(csv_path, individuals=["beta", "alpha"])

    assert payload["detail_scope"]["individuals"] == ["alpha", "beta"]
    assert payload["detail_scope"]["individual"] == ""
    assert payload["returned_fix_count"] == 4
    assert {fix["individual"] for fix in payload["fixes"]} == {"alpha", "beta"}
    review_by_key = {fix["fix_key"]: fix["review"] for fix in payload["fixes"] if "review" in fix}
    assert review_by_key["id:fix_a_1#row:1"]["issue_type"] == "drift"
    assert review_by_key["id:fix_a_1#row:1"]["comments"] == "first alpha issue"
    assert review_by_key["id:fix_b_2#row:4"]["issue_type"] == "loop"


def test_build_movement_fixes_supports_single_individual_and_truncation(tmp_path):
    csv_path = write_movement_csv(tmp_path / "movement.csv")

    payload = build_movement_fixes(csv_path, individual="beta", limit=1)

    assert payload["detail_scope"]["individual"] == "beta"
    assert payload["detail_scope"]["individuals"] == ["beta"]
    assert payload["matching_fix_count"] == 2
    assert payload["returned_fix_count"] == 1
    assert payload["truncated"] is True
    assert {fix["individual"] for fix in payload["fixes"]} == {"beta"}


def test_movement_summary_does_not_write_full_response_disk_caches(tmp_path):
    project_dir = tmp_path / "study"
    (project_dir / "scrubdata").mkdir(parents=True)
    csv_path = write_movement_csv(project_dir / "movement.csv")

    build_movement_overview(csv_path)
    build_movement_fixes(csv_path, individual="alpha")

    assert not (project_dir / "scrubdata" / "cache" / "movement_summary").exists()


def test_movement_summary_memory_caches_are_strictly_bounded():
    assert movement_summary._prepare_scan_context_cached.cache_info().maxsize == 4
    assert movement_summary._build_movement_overview_cached.cache_info().maxsize == 1
    assert (
        movement_summary._build_compact_movement_overview_cached.cache_info().maxsize
        == movement_summary.COMPACT_OVERVIEW_CACHE_SIZE
        == 4
    )
    assert not hasattr(movement_summary._build_movement_fixes, "cache_info")


def test_build_movement_fixes_loads_all_individuals_without_filter(tmp_path):
    csv_path = write_movement_csv(tmp_path / "movement.csv")

    payload = build_movement_fixes(csv_path)

    assert payload["detail_scope"]["individual"] == ""
    assert payload["detail_scope"]["individuals"] == []
    assert payload["returned_fix_count"] == 5
    assert {fix["individual"] for fix in payload["fixes"]} == {"alpha", "beta", "gamma"}


def test_build_movement_overview_includes_default_auto_bursts(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
fix_a_3,alpha,2024-01-01T02:00:01Z,-70.2,40.2,train
fix_a_4,alpha,2024-01-01T02:20:00Z,-70.3,40.3,train
""",
        encoding="utf-8",
    )

    payload = build_movement_overview(csv_path)

    assert [burst["fix_count"] for burst in payload["auto_bursts"]] == [2, 2]
    assert payload["auto_bursts"][0]["burst_gap_seconds"] == 3600.0


def test_build_movement_fixes_auto_bursts_respect_custom_gap(tmp_path):
    csv_path = write_movement_csv(tmp_path / "movement.csv")

    default_payload = build_movement_fixes(csv_path, individual="alpha")
    custom_payload = build_movement_fixes(csv_path, individual="alpha", burst_gap_seconds=3599)

    assert [burst["fix_count"] for burst in default_payload["auto_bursts"]] == [2]
    assert [burst["fix_count"] for burst in custom_payload["auto_bursts"]] == [1, 1]


def test_quantile_burst_gap_uses_source_wide_grouped_track_gaps(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_a_2,alpha,2024-01-01T00:00:10Z,-70.1,40.1,train
fix_a_3,alpha,2024-01-01T00:00:40Z,-70.2,40.2,train
fix_a_4,alpha,2024-01-02T00:00:00Z,-70.3,40.3,test
fix_a_5,alpha,2024-01-02T02:00:00Z,-70.4,40.4,test
fix_b_1,beta,2024-01-01T00:00:00Z,-71.0,41.0,train
fix_b_2,beta,2024-01-01T01:00:00Z,-71.1,41.1,train
""",
        encoding="utf-8",
    )

    payload = build_movement_fixes(
        csv_path,
        individual="alpha",
        burst_gap_mode="quantile",
        burst_gap_seconds=60,
        burst_gap_quantile=0.5,
    )

    assert payload["burst_gap_mode"] == "quantile"
    assert payload["burst_gap_quantile"] == 0.5
    assert payload["burst_gap_gap_count"] == 4
    assert payload["burst_gap_seconds"] == 1815.0
    assert payload["detail_scope"]["burst_gap_seconds"] == 1815.0
    assert [burst["burst_gap_seconds"] for burst in payload["auto_bursts"]] == [1815.0, 1815.0, 1815.0]
    assert sorted(burst["fix_count"] for burst in payload["auto_bursts"]) == [1, 1, 3]


def test_movement_fixes_reuses_resolved_quantile_gap_for_selected_tracks(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_a_2,alpha,2024-01-01T00:00:10Z,-70.1,40.1,train
fix_a_3,alpha,2024-01-01T00:00:40Z,-70.2,40.2,train
fix_b_1,beta,2024-01-01T00:00:00Z,-71.0,41.0,train
fix_b_2,beta,2024-01-01T01:00:00Z,-71.1,41.1,train
""",
        encoding="utf-8",
    )

    payload = build_movement_fixes(
        csv_path,
        individual="alpha",
        burst_gap_mode="quantile",
        burst_gap_seconds=60,
        burst_gap_quantile=0.5,
        burst_gap_effective_seconds=20,
    )

    assert payload["burst_gap_mode"] == "quantile"
    assert payload["burst_gap_seconds"] == 20.0
    assert payload["burst_gap_gap_count"] == 0
    assert [burst["fix_count"] for burst in payload["auto_bursts"]] == [2, 1]


def test_build_movement_fixes_derives_steps_in_sorted_track_order(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_2,alpha,2024-01-01T02:00:00Z,-70.2,40.2,train
fix_0,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_1,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
fix_b,beta,2024-01-01T00:00:00Z,-71.0,41.0,train
""",
        encoding="utf-8",
    )

    payload = build_movement_fixes(csv_path, individual="alpha")
    fixes = payload["fixes"]

    assert [fix["fix_key"] for fix in fixes] == [
        "id:fix_0#row:2",
        "id:fix_1#row:3",
        "id:fix_2#row:1",
    ]
    assert fixes[0]["attributes"]["time_delta_s"] == 3600.0
    assert fixes[1]["attributes"]["time_delta_s"] == 3600.0
    assert "time_delta_s" not in fixes[2].get("attributes", {})
    assert "turn_angle_deg" not in fixes[0].get("attributes", {})
    assert fixes[1]["attributes"]["turn_angle_deg"] == pytest.approx(0.0372, abs=0.01)
    assert "turn_angle_deg" not in fixes[2].get("attributes", {})
    overview = build_movement_overview(csv_path)
    assert any(field["key"] == "turn_angle_deg" for field in overview["color_fields"])
    assert isclose(
        fixes[1]["attributes"]["speed_mps"],
        fixes[1]["attributes"]["step_length_m"] / 3600.0,
        rel_tol=1e-12,
    )
    assert all(
        not any(key.endswith("anomaly_score") for key in fix.get("attributes", {}))
        for fix in fixes
    )


def test_confirmed_fix_is_excluded_without_forcing_a_new_burst(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,visible,outlier_status
fix_a,alpha,2024-01-01T00:00:00Z,-70.0,40.0,true,
fix_b,alpha,2024-01-01T01:00:00Z,-75.0,45.0,false,confirmed
fix_c,alpha,2024-01-01T02:00:00Z,-70.2,40.2,true,
""",
        encoding="utf-8",
    )

    same_burst = build_movement_fixes(
        csv_path,
        burst_gap_mode="manual",
        burst_gap_seconds=10_800,
    )
    by_key = {fix["fix_key"]: fix for fix in same_burst["fixes"]}
    excluded = by_key["id:fix_b#row:2"]
    before_exclusion = by_key["id:fix_a#row:1"]

    assert excluded["analytically_excluded"] is True
    assert "step_length_m" not in excluded.get("attributes", {})
    assert before_exclusion["attributes"]["time_delta_s"] == 7200.0
    assert isclose(
        before_exclusion["attributes"]["speed_mps"],
        before_exclusion["attributes"]["step_length_m"] / 7200.0,
        rel_tol=1e-12,
    )
    assert len(same_burst["auto_bursts"]) == 1
    assert same_burst["auto_bursts"][0]["fix_keys"] == [
        "id:fix_a#row:1",
        "id:fix_c#row:3",
    ]

    split_burst = build_movement_fixes(
        csv_path,
        burst_gap_mode="manual",
        burst_gap_seconds=3600,
    )
    assert [burst["fix_keys"] for burst in split_burst["auto_bursts"]] == [
        ["id:fix_a#row:1"],
        ["id:fix_c#row:3"],
    ]


def test_legacy_invisible_suspected_fix_remains_in_track_analysis(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,visible,outlier_status
fix_a,alpha,2024-01-01T00:00:00Z,-70.0,40.0,true,
fix_b,alpha,2024-01-01T01:00:00Z,-70.1,40.1,false,suspected
fix_c,alpha,2024-01-01T02:00:00Z,-70.2,40.2,true,
""",
        encoding="utf-8",
    )

    payload = build_movement_fixes(
        csv_path,
        burst_gap_mode="manual",
        burst_gap_seconds=10_800,
    )
    by_key = {fix["fix_key"]: fix for fix in payload["fixes"]}

    assert "analytically_excluded" not in by_key["id:fix_b#row:2"]
    assert by_key["id:fix_b#row:2"]["attributes"]["time_delta_s"] == 3600.0
    assert "time_delta_s" not in by_key["id:fix_c#row:3"].get("attributes", {})
    assert payload["auto_bursts"][0]["fix_keys"] == [
        "id:fix_a#row:1",
        "id:fix_b#row:2",
        "id:fix_c#row:3",
    ]


def test_source_flags_remain_in_analysis_until_app_confirmation(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,visible,manually-marked-outlier,algorithm-marked-outlier,outlier_status
fix_a,alpha,2024-01-01T00:00:00Z,-70.0,40.0,true,false,false,
fix_b,alpha,2024-01-01T01:00:00Z,-70.1,40.1,false,true,false,
fix_c,alpha,2024-01-01T02:00:00Z,-70.2,40.2,true,false,true,
""",
        encoding="utf-8",
    )

    payload = build_movement_fixes(
        csv_path,
        burst_gap_mode="manual",
        burst_gap_seconds=10_800,
    )
    by_key = {fix["fix_key"]: fix for fix in payload["fixes"]}

    assert "analytically_excluded" not in by_key["id:fix_b#row:2"]
    assert "analytically_excluded" not in by_key["id:fix_c#row:3"]
    assert by_key["id:fix_b#row:2"]["source_flags"] == [
        "visible=false",
        "manually-marked-outlier=true",
    ]
    assert by_key["id:fix_c#row:3"]["source_flags"] == [
        "algorithm-marked-outlier=true",
    ]
    assert by_key["id:fix_b#row:2"]["attributes"]["time_delta_s"] == 3600.0
    assert "time_delta_s" not in by_key["id:fix_c#row:3"].get("attributes", {})
    assert payload["auto_bursts"][0]["fix_keys"] == [
        "id:fix_a#row:1",
        "id:fix_b#row:2",
        "id:fix_c#row:3",
    ]


def test_build_movement_overview_auto_bursts_use_sorted_track_order(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_2,alpha,2024-01-01T02:00:01Z,-70.2,40.2,train
fix_0,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_1,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
fix_3,alpha,2024-01-01T02:20:00Z,-70.3,40.3,train
""",
        encoding="utf-8",
    )

    payload = build_movement_overview(csv_path)

    assert [burst["fix_keys"] for burst in payload["auto_bursts"]] == [
        ["id:fix_0#row:2", "id:fix_1#row:3"],
        ["id:fix_2#row:1", "id:fix_3#row:4"],
    ]


def test_review_annotations_do_not_mutate_cached_movement_payload(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude
fix_a,alpha,2024-01-01T00:00:00Z,-70.0,40.0
fix_b,alpha,2024-01-01T01:00:00Z,-70.1,40.1
""",
        encoding="utf-8",
    )
    base = build_movement_overview(csv_path)
    annotation = normalize_annotation({
        "annotation_id": "issue_child",
        "source_artifact": "movement.csv",
        "status": "suspected",
        "origin": "manual",
        "issue_type": "drift",
        "scope": {"kind": "fix", "fix_keys": ["id:fix_a#row:1"]},
    })

    child_payload = apply_review_annotations(
        base,
        [annotation],
        source_artifact="movement.csv",
    )
    ancestor_payload = build_movement_overview(csv_path)

    assert child_payload["fixes"][0]["review"]["status"] == "suspected"
    assert not ancestor_payload["fixes"][0].get("review")
    assert ancestor_payload.get("review_annotations") in (None, [])


def test_effective_issues_resolve_duplicate_parents_independently_with_confirmation_precedence():
    annotations = [
        normalize_annotation({
            "annotation_id": "filter_run_1",
            "status": "suspected",
            "origin": "algorithm",
            "issue_type": "fast fix",
            "scope": {"kind": "fix", "row_ranges": [[2, 2]]},
        }),
        normalize_annotation({
            "annotation_id": "filter_run_2",
            "status": "suspected",
            "origin": "algorithm",
            "issue_type": "fast fix rerun",
            "scope": {"kind": "fix", "row_ranges": [[2, 2]]},
        }),
        normalize_annotation({
            "annotation_id": "dismiss_1",
            "annotation_kind": "dismissal",
            "parent_annotation_id": "filter_run_1",
            "status": "dismissed",
            "scope": {"kind": "dismissal", "row_ranges": [[2, 2]]},
        }),
    ]

    effective = effective_issues_for_fix(
        annotations,
        fix_key="id:fix_a_2#row:2",
        individual="alpha",
        set_name="train",
    )
    assert {item["parent_issue_id"]: item["status"] for item in effective} == {
        "filter_run_1": "dismissed",
        "filter_run_2": "suspected",
    }
    assert effective_review_status(effective) == "suspected"

    annotations.extend([
        normalize_annotation({
            "annotation_id": "confirm_2",
            "annotation_kind": "confirmation",
            "parent_annotation_id": "filter_run_2",
            "status": "confirmed",
            "scope": {"kind": "confirmation", "row_ranges": [[2, 2]]},
        }),
        normalize_annotation({
            "annotation_id": "dismiss_2",
            "annotation_kind": "dismissal",
            "parent_annotation_id": "filter_run_2",
            "status": "dismissed",
            "scope": {"kind": "dismissal", "row_ranges": [[2, 2]]},
        }),
    ])
    effective = effective_issues_for_fix(
        annotations,
        fix_key="id:fix_a_2#row:2",
        individual="alpha",
        set_name="train",
    )
    assert {item["parent_issue_id"]: item["status"] for item in effective} == {
        "filter_run_1": "dismissed",
        "filter_run_2": "confirmed",
    }
    assert effective_review_status(effective) == "confirmed"


def test_duplicate_timestamps_use_row_order_without_per_fix_branching(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_0,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_1,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
fix_2,alpha,2024-01-01T01:00:00Z,-70.2,40.2,train
fix_3,alpha,2024-01-01T02:00:00Z,-70.3,40.3,train
""",
        encoding="utf-8",
    )

    payload = build_movement_fixes(csv_path, individual="alpha")
    fixes_by_key = {fix["fix_key"]: fix for fix in payload["fixes"]}
    diagnostics = diagnose_track_topology(csv_path)

    assert [fix["fix_key"] for fix in payload["fixes"]] == [
        "id:fix_0#row:1",
        "id:fix_1#row:2",
        "id:fix_2#row:3",
        "id:fix_3#row:4",
    ]
    assert fixes_by_key["id:fix_1#row:2"]["attributes"]["time_delta_s"] == 0.0
    assert "speed_mps" not in fixes_by_key["id:fix_1#row:2"]["attributes"]
    assert fixes_by_key["id:fix_2#row:3"]["attributes"]["time_delta_s"] == 3600.0
    assert "time_delta_s" not in fixes_by_key["id:fix_3#row:4"].get("attributes", {})
    assert diagnostics["duplicate_track_timestamp_count"] == 1
    assert diagnostics["max_fix_topological_degree"] == 2


def test_repeated_coordinates_can_create_visual_degree_above_two(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_0,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_1,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
fix_2,alpha,2024-01-01T02:00:00Z,-70.0,40.0,train
fix_3,alpha,2024-01-01T03:00:00Z,-70.2,40.2,train
fix_4,alpha,2024-01-01T04:00:00Z,-70.0,40.0,train
fix_5,alpha,2024-01-01T05:00:00Z,-70.3,40.3,train
""",
        encoding="utf-8",
    )

    diagnostics = diagnose_track_topology(csv_path)

    assert diagnostics["repeated_coordinate_count"] == 1
    assert diagnostics["max_fixes_at_coordinate"] == 3
    assert diagnostics["coordinate_degree_gt2_count"] == 1
    assert diagnostics["max_coordinate_degree"] == 3
    assert diagnostics["max_fix_topological_degree"] == 2


def test_build_movement_overview_includes_fix_points_not_just_reviewed_rows(tmp_path):
    csv_path = write_movement_csv(tmp_path / "movement.csv")

    payload = build_movement_overview(csv_path)

    assert payload["detail_loaded"] is False
    assert len(payload["fixes"]) == 5
    assert {fix["individual"] for fix in payload["fixes"]} == {"alpha", "beta", "gamma"}
    assert "auto_bursts" in payload


def test_build_movement_overview_suppresses_initial_payload_without_dropping_individuals(tmp_path, monkeypatch):
    csv_path = write_movement_csv(tmp_path / "movement.csv")
    monkeypatch.setattr(movement_summary, "DEFAULT_OVERVIEW_FIX_LIMIT", 2)

    payload = movement_summary.build_movement_overview(csv_path)

    assert payload["total_rows"] == 5
    assert payload["individuals"] == ["alpha", "beta", "gamma"]
    assert payload["overview_truncated"] is True
    assert payload["auto_bursts_truncated"] is True
    assert payload["overview_fix_limit"] == 2
    assert len(payload["fixes"]) == 2
    assert [fix["individual"] for fix in payload["fixes"]] == ["alpha", "alpha"]
    assert payload["auto_bursts"] == []


def test_build_movement_overview_supports_compact_viewer_profile(tmp_path):
    csv_path = write_movement_csv(tmp_path / "movement.csv")

    payload = build_movement_overview(
        csv_path,
        overview_fix_limit=0,
        max_series_points=1,
    )

    assert payload["total_rows"] == 5
    assert payload["individuals"] == ["alpha", "beta", "gamma"]
    assert payload["overview_truncated"] is True
    assert payload["overview_fix_limit"] == 0
    assert payload["overview_series_point_limit"] == 1
    assert payload["fixes"] == []
    assert all(
        len(series["positions"]) <= 1
        for sets in payload["series_by_individual"].values()
        for series in sets.values()
    )


def test_manual_segments_and_auto_bursts_are_separate(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_a_2,alpha,2024-01-01T00:30:00Z,-70.1,40.1,train
fix_a_3,alpha,2024-01-01T02:00:01Z,-70.2,40.2,train
""",
        encoding="utf-8",
    )

    payload = apply_review_annotations(
        build_movement_overview(csv_path),
        [
            normalize_annotation(
                {
                    "annotation_id": "manual_segment",
                    "source_artifact": "movement.csv",
                    "status": "suspected",
                    "issue_type": "collar",
                    "scope": {
                        "kind": "segment",
                        "fix_keys": ["id:fix_a_1#row:1", "id:fix_a_2#row:2"],
                    },
                }
            )
        ],
        source_artifact="movement.csv",
    )

    assert payload["segments"][0]["segment_id"] == "manual_segment"
    assert [burst["burst_idx"] for burst in payload["auto_bursts"]] == [0, 1]
    assert "segment_id" not in payload["auto_bursts"][0]


def test_build_movement_overview_includes_height_above_msl_in_color_fields(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,height-above-msl
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,100
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,120
""",
        encoding="utf-8",
    )

    payload = build_movement_overview(csv_path)

    assert any(field["key"] == "height-above-msl" for field in payload["color_fields"])


def test_gps_quality_color_fields_are_shared_by_overview_and_detail(tmp_path):
    csv_path = tmp_path / "movement.csv"
    rows = [
        "eventid,individual,timestamp,longitude,latitude,gps:satellite-count,gps-time-to-fix,gps:fix-type-raw,gps:maximum-signal-strength,eobs:horizontal-accuracy-estimate,location-error-text"
    ]
    for index in range(14):
        rows.append(
            f"fix_{index},alpha,2024-01-01T00:{index:02d}:00Z,-70.{index},40.{index},"
            f"{8 + index},{10 + index},{['2d', '3d', 'dgps'][index % 3]},{30 + index},{2 + index / 10},error-{index}"
        )
    csv_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    overview = build_movement_overview(csv_path)
    detail = build_movement_fixes(csv_path)
    overview_fields = {field["key"]: field["kind"] for field in overview["color_fields"]}
    detail_attributes = detail["fixes"][0]["attributes"]

    expected = {
        "gps:satellite-count": "numeric",
        "gps-time-to-fix": "numeric",
        "gps:fix-type-raw": "categorical",
        "gps:maximum-signal-strength": "numeric",
        "eobs:horizontal-accuracy-estimate": "numeric",
    }
    assert {key: overview_fields[key] for key in expected} == expected
    assert all(key in detail_attributes for key in expected)
    assert "location-error-text" not in overview_fields
    assert "location-error-text" not in detail_attributes


def test_build_movement_fixes_ignores_cleared_issue_metadata(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set,outlier_status,outlier_issue_type,outlier_comments
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train,,drift,stale metadata
""",
        encoding="utf-8",
    )

    payload = build_movement_fixes(csv_path)

    assert payload["returned_fix_count"] == 1
    assert "review" not in payload["fixes"][0]


def test_overview_primes_schema_for_single_pass_detail_loading(tmp_path, monkeypatch):
    csv_path = write_movement_csv(tmp_path / "movement.csv")
    overview = build_movement_overview(csv_path)

    def reject_redundant_schema_scan(*_args, **_kwargs):
        raise AssertionError("detail loading repeated the full schema scan")

    monkeypatch.setattr(
        movement_summary,
        "_prepare_scan_context_cached",
        reject_redundant_schema_scan,
    )
    detail = build_movement_fixes(
        csv_path,
        individuals=["alpha"],
        burst_gap_effective_seconds=overview["burst_gap_seconds"],
    )

    assert detail["returned_fix_count"] == 2
    assert {item["individual"] for item in detail["fixes"]} == {"alpha"}


def create_movement_test_client(tmp_path: Path, *, csv_content: str = CSV_CONTENT) -> tuple[TestClient, str]:
    data_root = tmp_path / "data"
    study_dir = data_root / "movement_clean" / "test_study"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(csv_content, encoding="utf-8")

    app = create_app(
        data_root=data_root,
        static_root=REPO_ROOT / "examples" / "movement" / "static",
    )
    register_movement_routes(app, data_root=data_root)

    dataset_id = load_project_state(study_dir)["current_dataset_id"]
    client = TestClient(app)
    return client, dataset_id


def parse_movement_binary_payload(content: bytes) -> tuple[dict, int]:
    assert content[:4] == b"VCM1"
    header_length = struct.unpack_from("<I", content, 4)[0]
    header = json.loads(content[8 : 8 + header_length])
    return header, ((8 + header_length + 7) // 8) * 8


def test_csv_exact_binary_subset_matches_canonical_fixes(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    params = [("logical_name", "movement.csv"), ("individuals", "alpha")]
    binary_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes-binary",
        params=params,
    )
    json_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params=params,
    )

    assert binary_response.status_code == 200, binary_response.text
    assert json_response.status_code == 200, json_response.text
    header, data_offset = parse_movement_binary_payload(binary_response.content)
    canonical = json_response.json()["fixes"]
    assert header["version"] == 2
    assert header["source_format"] == "csv"
    assert header["loaded_individuals"] == ["alpha"]
    assert header["row_count"] == len(canonical) == 2
    assert header["line_count"] == 1
    assert header["individual_point_ranges"] == {"alpha": [0, 2]}

    offset_meta = header["arrays"]["fix_key_offsets"]
    bytes_meta = header["arrays"]["fix_key_bytes"]
    offsets = struct.unpack_from(
        "<3I", binary_response.content, data_offset + offset_meta["offset"]
    )
    key_bytes = binary_response.content[
        data_offset + bytes_meta["offset"] :
        data_offset + bytes_meta["offset"] + bytes_meta["length"]
    ]
    binary_keys = [
        key_bytes[offsets[index] : offsets[index + 1]].decode("utf-8")
        for index in range(2)
    ]
    assert binary_keys == [fix["fix_key"] for fix in canonical]


def test_fix_annotation_template_uses_row_range_fast_path():
    source = ANNOTATE_SCOPE_TEMPLATE_PATH.read_text(encoding="utf-8")
    fix_branch = source.split('if kind in {"fix", "segment"}:', 1)[1].split(
        'elif kind == "individual":', 1
    )[0]

    assert "build_movement_fixes" not in fix_branch
    assert "resolved_fix_count = sum(" in fix_branch


def test_annotate_scope_records_167_fixes_as_one_row_range_annotation(tmp_path):
    rows = ["eventid,individual,timestamp,longitude,latitude,set"]
    fix_keys = []
    for row_number in range(1, 201):
        rows.append(
            f"fix_{row_number},alpha,2024-01-01T00:00:00Z,-70.0,40.0,train"
        )
        if row_number <= 167:
            fix_keys.append(f"id:fix_{row_number}#row:{row_number}")
    client, dataset_id = create_movement_test_client(
        tmp_path,
        csv_content="\n".join(rows) + "\n",
    )

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "expected_current_dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "fix", "fix_keys": fix_keys},
            "status": "suspected",
            "origin": "threshold",
            "issue_type": "unreasonable speed",
            "comment": "Review this batch",
            "owner_question": "Are these fixes valid?",
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["step"]["summary"]["resolved_fix_count"] == 167
    assert payload["step"]["parameters"]["scope"]["row_ranges"] == [[1, 167]]


def test_review_projection_reuses_tracks_for_suspicions_and_reports_visible_reviews(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    root_overview = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/overview",
        params={"logical_name": "movement.csv"},
    ).json()

    flagged = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "expected_current_dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "fix", "fix_keys": ["id:fix_a_2#row:2"]},
            "status": "suspected",
            "origin": "manual",
            "issue_type": "manual check",
            "comment": "Review this fix",
            "user": "reviewer",
        },
    )
    assert flagged.status_code == 200
    flagged_dataset_id = flagged.json()["dataset"]["dataset_id"]

    projection_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{flagged_dataset_id}/review-projection",
        params=[("logical_name", "movement.csv"), ("individuals", "alpha")],
    )
    assert projection_response.status_code == 200
    projection = projection_response.json()

    assert projection["source_signature"] == root_overview["source_signature"]
    assert projection["exclusion_signature"] == root_overview["exclusion_signature"]
    assert projection["dataset"]["dataset_id"] == flagged_dataset_id
    assert projection["review_counts"] == {"suspected": 3, "confirmed": 1}
    projected_by_key = {item["fix_key"]: item for item in projection["fixes"]}
    assert projected_by_key["id:fix_a_2#row:2"]["review"]["status"] == "suspected"
    assert all(item["individual"] == "alpha" for item in projection["fixes"])


def test_review_projection_marks_confirmations_as_track_changes(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    flagged = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "expected_current_dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "fix", "fix_keys": ["id:fix_a_2#row:2"]},
            "status": "suspected",
            "origin": "manual",
            "issue_type": "manual check",
            "comment": "Review this fix",
            "user": "reviewer",
        },
    ).json()
    flagged_dataset_id = flagged["dataset"]["dataset_id"]
    parent_annotation_id = flagged["step"]["summary"]["annotation_id"]
    suspected_projection = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{flagged_dataset_id}/review-projection",
        params={"logical_name": "movement.csv"},
    ).json()

    confirmed = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/confirm-issues",
        json={
            "dataset_id": flagged_dataset_id,
            "expected_current_dataset_id": flagged_dataset_id,
            "logical_name": "movement.csv",
            "confirmations": [{
                "parent_annotation_id": parent_annotation_id,
                "fix_keys": ["id:fix_a_2#row:2"],
            }],
            "user": "reviewer",
        },
    )
    assert confirmed.status_code == 200
    confirmed_dataset_id = confirmed.json()["dataset"]["dataset_id"]
    confirmed_projection = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{confirmed_dataset_id}/review-projection",
        params={"logical_name": "movement.csv"},
    ).json()

    assert confirmed_projection["source_signature"] == suspected_projection["source_signature"]
    assert confirmed_projection["exclusion_signature"] != suspected_projection["exclusion_signature"]


def test_review_projection_reuses_the_overview_row_lookup(tmp_path, monkeypatch):
    movement_review_annotations._review_row_contexts_cached.cache_clear()
    client, dataset_id = create_movement_test_client(tmp_path)
    movement_csv_opens = 0
    original_open = Path.open

    def tracked_open(path, *args, **kwargs):
        nonlocal movement_csv_opens
        if path.name == "movement.csv":
            movement_csv_opens += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", tracked_open)
    overview = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/overview",
        params={"logical_name": "movement.csv"},
    )
    assert overview.status_code == 200
    opens_after_overview = movement_csv_opens

    projection = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/review-projection",
        params=[("logical_name", "movement.csv"), ("individuals", "alpha")],
    )
    assert projection.status_code == 200
    assert movement_csv_opens == opens_after_overview


def test_segment_annotation_persists_track_identity_and_selection_method(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {
                "kind": "segment",
                "fix_keys": ["id:fix_a_1#row:1", "id:fix_a_2#row:2"],
                "start_fix_key": "id:fix_a_1#row:1",
                "end_fix_key": "id:fix_a_2#row:2",
                "individual": "alpha",
                "set_name": "train",
                "selection_method": "map_double_click",
            },
            "status": "suspected",
            "origin": "manual",
            "issue_type": "track segment",
            "comment": "Review this selected track segment",
            "owner_question": "Does this movement section look valid?",
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    scope = payload["step"]["parameters"]["scope"]
    assert scope["kind"] == "segment"
    assert scope["individual"] == "alpha"
    assert scope["set_name"] == "train"
    assert scope["selection_method"] == "map_double_click"
    assert scope["row_ranges"] == [[1, 2]]

    study_dir = tmp_path / "data" / "movement_clean" / "test_study"
    _, sidecar_path = get_dataset_artifact(
        study_dir,
        payload["dataset"]["dataset_id"],
        "movement_review_annotations.json",
    )
    annotation = json.loads(sidecar_path.read_text(encoding="utf-8"))["annotations"][-1]
    assert annotation["step_id"] == payload["step"]["step_id"]
    assert annotation["scope"]["individual"] == "alpha"
    assert annotation["scope"]["set_name"] == "train"
    assert annotation["scope"]["selection_method"] == "map_double_click"

    detail = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{payload['dataset']['dataset_id']}/fixes",
        params={"logical_name": "movement.csv", "individuals": "alpha"},
    )
    assert detail.status_code == 200
    segment = next(
        item for item in detail.json()["segments"]
        if item["segment_id"] == payload["step"]["step_id"]
    )
    assert segment["individual"] == "alpha"
    assert segment["set_name"] == "train"
    assert segment["selection_method"] == "map_double_click"
    assert segment["start_fix_key"] == "id:fix_a_1#row:1"
    assert segment["end_fix_key"] == "id:fix_a_2#row:2"


def test_threshold_annotation_evaluates_the_full_csv_not_checked_preview(tmp_path):
    csv_content = """eventid,individual,timestamp,longitude,latitude,set,quality
fix_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train,1
fix_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train,10
fix_3,beta,2024-01-01T02:00:00Z,-70.2,40.2,test,20
"""
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=csv_content)
    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "expected_current_dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {
                "kind": "filter",
                "filter": {
                    "field_key": "quality",
                    "field_kind": "numeric",
                    "operator": "gt",
                    "threshold_value": 5,
                },
            },
            "status": "suspected",
            "origin": "threshold",
            "issue_type": "quality",
            "issue_field": "quality",
            "issue_threshold": "> 5",
            "comment": "Review every match",
            "owner_question": "Are these valid?",
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["step"]["summary"]["resolved_fix_count"] == 2
    assert payload["step"]["parameters"]["scope"]["kind"] == "filter"
    _, sidecar_path = get_dataset_artifact(
        tmp_path / "data" / "movement_clean" / "test_study",
        payload["dataset"]["dataset_id"],
        "movement_review_annotations.json",
    )
    annotation = json.loads(sidecar_path.read_text(encoding="utf-8"))["annotations"][0]
    assert annotation["scope"]["row_ranges"] == [[2, 3]]
    assert annotation["scope"]["filter"]["field_key"] == "quality"
    assert annotation["issue_field"] == "quality"
    assert annotation["issue_threshold"] == "> 5"


def test_is_outlier_filter_keeps_selected_individual_scope(tmp_path):
    csv_content = """eventid,individual,timestamp,longitude,latitude,set,is_outlier
fix_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train,true
fix_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,test,false
fix_3,beta,2024-01-01T02:00:00Z,-70.2,40.2,train,true
"""
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=csv_content)
    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "expected_current_dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {
                "kind": "filter",
                "filter": {
                    "field_key": "is_outlier",
                    "field_kind": "boolean",
                    "selected_levels": ["True"],
                    "individuals": ["alpha"],
                    "set_names": [],
                },
            },
            "status": "suspected",
            "origin": "threshold",
            "issue_type": "source outlier",
            "issue_field": "is_outlier",
            "issue_threshold": "is True",
            "comment": "Review all source outliers for selected animals",
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["step"]["summary"]["resolved_fix_count"] == 1
    persisted_filter = payload["step"]["parameters"]["scope"]["filter"]
    assert persisted_filter["individuals"] == ["alpha"]
    assert persisted_filter["set_names"] == []
    _, sidecar_path = get_dataset_artifact(
        tmp_path / "data" / "movement_clean" / "test_study",
        payload["dataset"]["dataset_id"],
        "movement_review_annotations.json",
    )
    annotation = json.loads(sidecar_path.read_text(encoding="utf-8"))["annotations"][0]
    assert annotation["scope"]["row_ranges"] == [[1, 1]]
    assert annotation["owner_question"] == ""


def test_derived_threshold_filter_preserves_chronological_track_semantics(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set
late,alpha,2024-01-01T02:00:00Z,-70.2,40.2,train
early,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
middle,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
""",
        encoding="utf-8",
    )

    ranges, count = resolve_filter_row_ranges(
        csv_path,
        {
            "field_key": "time_delta_s",
            "field_kind": "numeric",
            "operator": "gt",
            "threshold_value": 3000,
        },
    )

    assert count == 2
    assert ranges == [[2, 3]]


def test_derived_threshold_filter_keeps_visible_individual_scope(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        "eventid,individual,timestamp,longitude,latitude,set\n"
        "a1,alpha,2024-01-01T00:00:00Z,0,0,train\n"
        "a2,alpha,2024-01-01T01:00:00Z,1,0,train\n"
        "b1,beta,2024-01-01T00:00:00Z,0,1,train\n"
        "b2,beta,2024-01-01T01:00:00Z,1,1,train\n",
        encoding="utf-8",
    )

    ranges, count = resolve_filter_row_ranges(csv_path, {
        "field_key": "time_delta_s",
        "field_kind": "numeric",
        "operator": "gt",
        "threshold_value": 0,
        "individuals": ["alpha"],
        "set_names": ["train"],
    })

    assert count == 1
    assert ranges == [[1, 1]]


def test_turn_angle_threshold_filter_targets_the_center_fix(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        "eventid,individual,timestamp,longitude,latitude\n"
        "fix_a,alpha,2024-01-01T00:00:00Z,0,0\n"
        "fix_b,alpha,2024-01-01T01:00:00Z,1,0\n"
        "fix_c,alpha,2024-01-01T02:00:00Z,0,0\n",
        encoding="utf-8",
    )

    ranges, count = resolve_filter_row_ranges(
        csv_path,
        {
            "field_key": "turn_angle_deg",
            "field_kind": "numeric",
            "operator": "gt",
            "threshold_value": 150,
        },
    )

    assert ranges == [[2, 2]]
    assert count == 1


def test_gps_spike_filter_combines_step_turn_and_visible_scope(tmp_path, monkeypatch):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        "eventid,individual,timestamp,longitude,latitude,set\n"
        "a_1,alpha,2024-01-01T00:00:00Z,0,0,train\n"
        "a_2,alpha,2024-01-01T01:00:00Z,1,0,train\n"
        "a_3,alpha,2024-01-01T02:00:00Z,0,0,train\n"
        "b_1,beta,2024-01-01T00:00:00Z,0,0,test\n"
        "b_2,beta,2024-01-01T01:00:00Z,2,0,test\n"
        "b_3,beta,2024-01-01T02:00:00Z,0,0,test\n",
        encoding="utf-8",
    )
    filter_spec = {
        "kind": "gps_spike",
        "step_length_threshold_m": 100_000,
        "minimum_abs_turn_angle_deg": 150,
        "individuals": ["alpha"],
        "set_names": ["train"],
    }
    real_open = Path.open
    source_open_count = 0

    def counted_open(path, *args, **kwargs):
        nonlocal source_open_count
        if path == csv_path:
            source_open_count += 1
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)

    ranges, count = resolve_filter_row_ranges(csv_path, filter_spec)
    excluded_ranges, excluded_count = resolve_filter_row_ranges(
        csv_path,
        filter_spec,
        confirmed_fix_keys={"id:a_2#row:2"},
    )

    assert ranges == [[2, 2]]
    assert count == 1
    assert excluded_ranges == []
    assert excluded_count == 0
    assert source_open_count == 2


def test_gps_spike_flagging_creates_one_scoped_annotation_step(tmp_path):
    csv_content = (
        "eventid,individual,timestamp,longitude,latitude,set\n"
        "a_1,alpha,2024-01-01T00:00:00Z,0,0,train\n"
        "a_2,alpha,2024-01-01T01:00:00Z,1,0,train\n"
        "a_3,alpha,2024-01-01T02:00:00Z,0,0,train\n"
    )
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=csv_content)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "expected_current_dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {
                "kind": "filter",
                "filter": {
                    "kind": "gps_spike",
                    "step_length_threshold_m": 100_000,
                    "minimum_abs_turn_angle_deg": 150,
                    "individuals": ["alpha"],
                    "set_names": ["train"],
                },
            },
            "status": "suspected",
            "origin": "threshold",
            "issue_type": "GPS spike",
            "issue_field": "step_length_m + abs(turn_angle_deg)",
            "issue_threshold": "step > 100000 m and |turn| >= 150°",
            "comment": "Review the backtracking jump",
            "owner_question": "Is this a GPS spike?",
            "workflow_context": {
                "entry_point": "movement_map",
                "scope_kind": "filter",
                "selection_methods": ["color_threshold"],
            },
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert "analysis" not in payload
    assert payload["step"]["summary"]["resolved_fix_count"] == 1
    assert payload["step"]["parameters"]["scope"]["filter"]["kind"] == "gps_spike"
    _, sidecar_path = get_dataset_artifact(
        tmp_path / "data" / "movement_clean" / "test_study",
        payload["dataset"]["dataset_id"],
        "movement_review_annotations.json",
    )
    annotation = json.loads(sidecar_path.read_text(encoding="utf-8"))["annotations"][0]
    assert annotation["scope"]["row_ranges"] == [[2, 2]]
    assert annotation["scope"]["filter"]["individuals"] == ["alpha"]
    assert annotation["workflow_context"]["selection_methods"] == ["color_threshold"]


def test_gps_spike_filter_validation_rejects_invalid_thresholds(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    base_payload = {
        "dataset_id": dataset_id,
        "logical_name": "movement.csv",
        "scope": {
            "kind": "filter",
            "filter": {
                "kind": "gps_spike",
                "step_length_threshold_m": -1,
                "minimum_abs_turn_angle_deg": 181,
                "individuals": ["alpha"],
                "set_names": ["train"],
            },
        },
        "status": "suspected",
        "issue_type": "GPS spike",
        "comment": "Review",
        "owner_question": "Valid?",
        "user": "reviewer",
    }

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json=base_payload,
    )

    assert response.status_code == 400
    assert "step threshold must be nonnegative" in response.json()["error"]


def test_movement_history_locks_undo_and_resume_across_persistent_routes(tmp_path):
    clean_csv = """eventid,individual,timestamp,longitude,latitude,set
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
fix_b_1,beta,2024-01-01T00:30:00Z,-71.0,41.0,test
"""
    client, root_id = create_movement_test_client(tmp_path, csv_content=clean_csv)
    base_url = "/api/apps/movement/family/movement_clean/study/test_study"

    suspected = client.post(
        f"{base_url}/actions/annotate-scope",
        json={
            "dataset_id": root_id,
            "expected_current_dataset_id": root_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "fix", "fix_keys": ["id:fix_a_2#row:2"]},
            "status": "suspected",
            "origin": "manual",
            "issue_type": "speed review",
            "comment": "Check this fix",
            "owner_question": "Is this movement plausible?",
            "user": "reviewer",
        },
    )
    assert suspected.status_code == 200
    suspected_dataset_id = suspected.json()["dataset"]["dataset_id"]
    suspected_annotation_id = suspected.json()["step"]["step_id"]

    confirmed = client.post(
        f"{base_url}/actions/confirm-issues",
        json={
            "dataset_id": suspected_dataset_id,
            "expected_current_dataset_id": suspected_dataset_id,
            "logical_name": "movement.csv",
            "confirmations": [{
                "parent_annotation_id": suspected_annotation_id,
                "fix_keys": ["id:fix_a_2#row:2"],
            }],
            "user": "reviewer",
        },
    )
    assert confirmed.status_code == 200
    confirmed_dataset_id = confirmed.json()["dataset"]["dataset_id"]
    graph_before = client.get(f"{base_url}/graph").json()

    blocked_requests = [
        client.post(
            f"{base_url}/actions/annotate-scope",
            json={
                "dataset_id": root_id,
                "expected_current_dataset_id": confirmed_dataset_id,
                "logical_name": "movement.csv",
                "scope": {"kind": "fix", "fix_keys": ["id:fix_a_1#row:1"]},
                "status": "suspected",
                "origin": "manual",
                "issue_type": "historical edit",
                "comment": "This must not create a branch",
                "owner_question": "Please review",
                "user": "reviewer",
            },
        ),
        client.post(
            f"{base_url}/actions/review-individual",
            json={
                "dataset_id": root_id,
                "expected_current_dataset_id": confirmed_dataset_id,
                "logical_name": "movement.csv",
                "decision": {
                    "individual": "alpha",
                    "review_decision": "ok",
                    "needs_check": False,
                },
                "user": "reviewer",
            },
        ),
        client.post(
            f"{base_url}/actions/confirm-issues",
            json={
                "dataset_id": suspected_dataset_id,
                "expected_current_dataset_id": confirmed_dataset_id,
                "logical_name": "movement.csv",
                "confirmations": [{
                    "parent_annotation_id": suspected_annotation_id,
                    "fix_keys": ["id:fix_a_2#row:2"],
                }],
                "user": "reviewer",
            },
        ),
    ]
    assert [response.status_code for response in blocked_requests] == [423, 423, 423]
    assert all(
        response.json()["edit_profile"]["blockers"][0]["code"] == "historical_version"
        for response in blocked_requests
    )
    assert client.get(f"{base_url}/graph").json() == graph_before

    overview = client.get(
        f"{base_url}/dataset/{root_id}/overview",
        params={"logical_name": "movement.csv"},
    )
    exported = client.post(
        f"{base_url}/actions/export-reviewed-csv",
        json={
            "dataset_id": root_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
        },
    )
    assert overview.status_code == 200
    assert exported.status_code == 200

    first_undo = client.post(
        f"{base_url}/undo",
        json={"expected_current_dataset_id": confirmed_dataset_id},
    )
    assert first_undo.status_code == 200
    assert first_undo.json()["dataset"]["dataset_id"] == suspected_dataset_id
    rewound_profile = client.get(
        f"{base_url}/edit-profile",
        params={"dataset_id": suspected_dataset_id},
    ).json()
    assert rewound_profile["editable"] is True
    assert rewound_profile["blockers"] == []
    assert rewound_profile["resume"]["allowed"] is False

    restored_head = client.post(
        f"{base_url}/head",
        json={
            "dataset_id": confirmed_dataset_id,
            "expected_current_dataset_id": suspected_dataset_id,
        },
    )
    assert restored_head.status_code == 200
    assert restored_head.json()["dataset"]["dataset_id"] == confirmed_dataset_id
    assert restored_head.json()["edit_profile"]["editable"] is True
    assert restored_head.json()["edit_profile"]["resume"]["allowed"] is False
    assert client.get(f"{base_url}/graph").json() == graph_before

    undo_restored_head = client.post(
        f"{base_url}/undo",
        json={"expected_current_dataset_id": confirmed_dataset_id},
    )
    assert undo_restored_head.status_code == 200
    assert undo_restored_head.json()["dataset"]["dataset_id"] == suspected_dataset_id

    second_undo = client.post(
        f"{base_url}/undo",
        json={"expected_current_dataset_id": suspected_dataset_id},
    )
    assert second_undo.status_code == 200
    assert second_undo.json()["dataset"]["dataset_id"] == root_id
    root_profile = client.get(
        f"{base_url}/edit-profile",
        params={"dataset_id": root_id},
    ).json()
    assert root_profile["editable"] is True
    assert root_profile["resume"]["allowed"] is False

    restore_before_explicit_resume = client.post(
        f"{base_url}/head",
        json={
            "dataset_id": confirmed_dataset_id,
            "expected_current_dataset_id": root_id,
        },
    )
    assert restore_before_explicit_resume.status_code == 200
    historical_root_profile = client.get(
        f"{base_url}/edit-profile",
        params={"dataset_id": root_id},
    ).json()
    assert historical_root_profile["editable"] is False
    assert historical_root_profile["resume"]["allowed"] is True
    assert historical_root_profile["resume"]["discard_dataset_count"] == 2
    assert historical_root_profile["resume"]["discard_step_count"] == 2

    resumed = client.post(
        f"{base_url}/resume",
        json={
            "dataset_id": root_id,
            "expected_current_dataset_id": confirmed_dataset_id,
            "resume_token": historical_root_profile["resume"]["token"],
            "user": "reviewer",
        },
    )
    assert resumed.status_code == 200
    assert resumed.json()["profile"]["editable"] is True
    active_graph = client.get(f"{base_url}/graph").json()
    assert [dataset["dataset_id"] for dataset in active_graph["datasets"]] == [root_id]
    assert active_graph["steps"] == []


def test_movement_fixes_route_accepts_repeated_individuals(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params=[
            ("logical_name", "movement.csv"),
            ("individuals", "beta"),
            ("individuals", "alpha"),
        ],
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["detail_scope"]["individuals"] == ["alpha", "beta"]
    assert {fix["individual"] for fix in payload["fixes"]} == {"alpha", "beta"}


def test_movement_fixes_route_accepts_resolved_burst_gap(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params={
            "logical_name": "movement.csv",
            "individual": "alpha",
            "burst_gap_mode": "quantile",
            "burst_gap_seconds": "60",
            "burst_gap_quantile": "0.5",
            "burst_gap_effective_seconds": "20",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["burst_gap_mode"] == "quantile"
    assert payload["burst_gap_seconds"] == 20.0
    assert payload["burst_gap_gap_count"] == 0


def test_movement_fixes_route_supports_legacy_individual_query(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params={"logical_name": "movement.csv", "individual": "beta", "burst_gap_seconds": "3599"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["detail_scope"]["individual"] == "beta"
    assert payload["detail_scope"]["individuals"] == ["beta"]
    assert payload["burst_gap_mode"] == "manual"
    assert payload["burst_gap_seconds"] == 3599.0
    assert {fix["individual"] for fix in payload["fixes"]} == {"beta"}


def test_movement_fixes_route_loads_only_requested_review_status(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params={"logical_name": "movement.csv", "review_status": "suspected"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["matching_fix_count"] == 2
    assert payload["returned_fix_count"] == 2
    assert payload["truncated"] is False
    assert payload["segments"] == []
    assert payload["auto_bursts"] == []
    assert {
        fix["fix_key"]
        for fix in payload["fixes"]
    } == {"id:fix_a_1#row:1", "id:fix_b_2#row:4"}
    assert all(fix["review"]["status"] == "suspected" for fix in payload["fixes"])


def test_movement_overview_route_accepts_quantile_burst_gap_params(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/overview",
        params={
            "logical_name": "movement.csv",
            "burst_gap_mode": "quantile",
            "burst_gap_quantile": "1",
            "burst_gap_seconds": "99",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["burst_gap_mode"] == "quantile"
    assert payload["burst_gap_quantile"] == 1.0
    assert payload["burst_gap_fallback_seconds"] == 99.0
    assert payload["burst_gap_seconds"] == 3600.0
    assert payload["burst_gap_gap_count"] == 1


def create_saved_movement_analysis(
    tmp_path: Path,
    *,
    dataset_id: str,
    action: str = "run_burst_anomaly_ranking",
    output_name: str = "burst_anomaly_ranking.json",
    feature_set: str = "movement_only",
    ranking_method: str = "isolation_forest",
):
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"
    script = f'''import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
output = next(item for item in spec["output_artifacts"] if item["logical_name"] == "{output_name}")
result = {{"run_status": "completed", "ranked_individuals": [], "points": []}}
Path(output["path"]).write_text(json.dumps(result))
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text(json.dumps({{"run_status": "completed"}}))
'''
    return create_analysis(
        study_dir,
        {
            "user": "reviewer",
            "title": "Saved movement analysis",
            "kind": "python",
            "script": script,
            "dataset_id": dataset_id,
            "input_artifacts": ["movement.csv"],
            "output_artifacts": [output_name],
            "parameters": {
                "app": "movement",
                "action": action,
                "burst_feature_signature": "adjacent_retained_fixes:wgs84:v2",
                "target_artifact": "movement.csv",
                "burst_gap_mode": "manual",
                "burst_gap_seconds": 60,
                "burst_gap_quantile": 0.75,
                "feature_set": feature_set,
                "ranking_method": ranking_method,
            },
        },
    )


def test_movement_analysis_history_finds_latest_compatible_saved_run(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    first = create_saved_movement_analysis(tmp_path, dataset_id=dataset_id)
    second = create_saved_movement_analysis(tmp_path, dataset_id=dataset_id)
    ignored = create_saved_movement_analysis(
        tmp_path,
        dataset_id=dataset_id,
        action="generate_report",
        output_name="movement_report.json",
    )
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"
    second_summary_path = study_dir / second["analysis"]["summary_path"]
    second_summary_path.write_text(
        json.dumps({
            "run_status": "completed",
            "ranked_individuals": [{"payload": "large"}] * 100,
        }),
        encoding="utf-8",
    )

    response = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/analyses",
        params={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "60",
            "burst_gap_quantile": "0.75",
            "feature_set": "movement_only",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert [item["analysis_id"] for item in payload["items"]] == [
        second["analysis"]["analysis_id"],
        first["analysis"]["analysis_id"],
    ]
    assert all(item["compatible"] for item in payload["items"])
    assert payload["latest_compatible_by_action"]["run_burst_anomaly_ranking"] == second["analysis"]["analysis_id"]
    assert payload["items"][0]["summary"] == {"run_status": "completed"}
    assert ignored["analysis"]["analysis_id"] not in {
        item["analysis_id"] for item in payload["items"]
    }


def test_movement_analysis_history_explains_parameter_mismatches(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    saved = create_saved_movement_analysis(tmp_path, dataset_id=dataset_id)

    response = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/analyses",
        params={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "burst_gap_mode": "quantile",
            "burst_gap_seconds": "3600",
            "burst_gap_quantile": "0.999",
            "feature_set": "movement_plus_context",
        },
    )

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["analysis_id"] == saved["analysis"]["analysis_id"]
    assert item["compatible"] is False
    assert set(item["compatibility_reasons"]) == {
        "burst gap mode differs",
        "burst gap fallback differs",
        "burst gap quantile differs",
        "feature set differs",
    }


def test_movement_analysis_history_distinguishes_ranking_aggregation(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    saved = create_saved_movement_analysis(tmp_path, dataset_id=dataset_id)

    response = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/analyses",
        params={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "60",
            "burst_gap_quantile": "0.75",
            "feature_set": "movement_only",
            "ranking_method": "isolation_forest_decision_margin",
        },
    )

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["analysis_id"] == saved["analysis"]["analysis_id"]
    assert item["compatible"] is False
    assert item["compatibility_reasons"] == ["ranking method differs"]


def test_movement_analysis_history_survives_sidecar_only_dataset_steps(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    saved = create_saved_movement_analysis(tmp_path, dataset_id=dataset_id)
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"
    step = create_step(
        study_dir,
        {
            "user": "reviewer",
            "title": "Add review sidecar",
            "kind": "python",
            "script": '''import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
Path(spec["output_artifacts"][0]["path"]).write_text("[]\\n")
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text("{}\\n")
''',
            "parent_dataset_id": dataset_id,
            "input_artifacts": [],
            "output_artifacts": ["movement_review_annotations.json"],
            "parameters": {"app": "movement", "action": "add_review_sidecar"},
        },
    )

    response = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/analyses",
        params={
            "dataset_id": step["dataset"]["dataset_id"],
            "logical_name": "movement.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "60",
            "burst_gap_quantile": "0.75",
            "feature_set": "movement_only",
        },
    )

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["analysis_id"] == saved["analysis"]["analysis_id"]
    assert item["compatible"] is True


def test_movement_analysis_history_survives_individual_review_decision_steps(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    saved = create_saved_movement_analysis(tmp_path, dataset_id=dataset_id)
    analysis_id = saved["analysis"]["analysis_id"]
    descendant_ids = []

    for individual, review_decision in (
        ("alpha", "ok"),
        ("beta", "fix_keep"),
        ("gamma", "remove"),
    ):
        response = client.post(
            "/api/apps/movement/family/movement_clean/study/test_study/actions/review-individual",
            json={
                "dataset_id": dataset_id,
                "logical_name": "movement.csv",
                "user": "reviewer",
                "decision": {
                    "individual": individual,
                    "review_decision": review_decision,
                    "needs_check": False,
                    "comment": "",
                },
            },
        )
        assert response.status_code == 200, response.text
        dataset_id = response.json()["dataset"]["dataset_id"]
        descendant_ids.append(dataset_id)

    for descendant_id in descendant_ids:
        history = client.get(
            "/api/apps/movement/family/movement_clean/study/test_study/analyses",
            params={
                "dataset_id": descendant_id,
                "logical_name": "movement.csv",
                "burst_gap_mode": "manual",
                "burst_gap_seconds": "60",
                "burst_gap_quantile": "0.75",
                "feature_set": "movement_only",
                "ranking_method": "isolation_forest",
            },
        )
        assert history.status_code == 200, history.text
        payload = history.json()
        assert payload["latest_compatible_by_action"][
            "run_burst_anomaly_ranking"
        ] == analysis_id
        inherited = next(
            item for item in payload["items"] if item["analysis_id"] == analysis_id
        )
        assert inherited["dataset_id"] != descendant_id
        assert inherited["compatible"] is True


def test_movement_analysis_history_invalidates_saved_run_after_confirmation(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    saved = create_saved_movement_analysis(tmp_path, dataset_id=dataset_id)
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"
    step = create_step(
        study_dir,
        {
            "user": "reviewer",
            "title": "Add confirmed review exclusion",
            "kind": "python",
            "script": '''import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
payload = {
    "schema_version": 2,
    "annotations": [{
        "annotation_id": "confirmation_1",
        "parent_annotation_id": "issue_1",
        "annotation_kind": "confirmation",
        "source_artifact": "movement.csv",
        "status": "confirmed",
        "origin": "manual",
        "issue_type": "drift",
        "scope": {"kind": "confirmation", "fix_keys": ["id:fix_a_1#row:1"]},
    }],
}
Path(spec["output_artifacts"][0]["path"]).write_text(json.dumps(payload))
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text("{}\\n")
''',
            "parent_dataset_id": dataset_id,
            "input_artifacts": [],
            "output_artifacts": ["movement_review_annotations.json"],
            "parameters": {"app": "movement", "action": "confirm_issues"},
        },
    )

    response = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/analyses",
        params={
            "dataset_id": step["dataset"]["dataset_id"],
            "logical_name": "movement.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "60",
            "burst_gap_quantile": "0.75",
            "feature_set": "movement_only",
        },
    )

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["analysis_id"] == saved["analysis"]["analysis_id"]
    assert item["compatible"] is False
    assert item["compatibility_reasons"] == ["confirmed exclusion state differs"]


def test_movement_analysis_history_does_not_leak_descendant_run_into_ancestor(tmp_path):
    client, root_dataset_id = create_movement_test_client(tmp_path)
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"
    step = create_step(
        study_dir,
        {
            "user": "reviewer",
            "title": "Create child dataset",
            "kind": "python",
            "script": '''import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
Path(spec["output_artifacts"][0]["path"]).write_text('{"annotations": []}\\n')
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text("{}\\n")
''',
            "parent_dataset_id": root_dataset_id,
            "input_artifacts": [],
            "output_artifacts": ["movement_review_annotations.json"],
            "parameters": {"app": "movement", "action": "child_dataset"},
        },
    )
    child_dataset_id = step["dataset"]["dataset_id"]
    child_analysis = create_saved_movement_analysis(
        tmp_path,
        dataset_id=child_dataset_id,
    )

    ancestor_response = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/analyses",
        params={
            "dataset_id": root_dataset_id,
            "logical_name": "movement.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "60",
            "burst_gap_quantile": "0.75",
            "feature_set": "movement_only",
        },
    )
    child_response = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/analyses",
        params={
            "dataset_id": child_dataset_id,
            "logical_name": "movement.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "60",
            "burst_gap_quantile": "0.75",
            "feature_set": "movement_only",
        },
    )

    analysis_id = child_analysis["analysis"]["analysis_id"]
    assert ancestor_response.status_code == 200
    assert analysis_id not in {
        item["analysis_id"] for item in ancestor_response.json()["items"]
    }
    assert child_response.status_code == 200
    assert analysis_id in {
        item["analysis_id"] for item in child_response.json()["items"]
    }


def test_movement_analysis_history_does_not_leak_across_sibling_branches(tmp_path):
    client, root_dataset_id = create_movement_test_client(tmp_path)
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"

    def child_dataset(title: str) -> str:
        result = create_step(
            study_dir,
            {
                "user": "reviewer",
                "title": title,
                "kind": "python",
                "script": '''import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
Path(spec["output_artifacts"][0]["path"]).write_text('{"annotations": []}\\n')
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text("{}\\n")
''',
                "parent_dataset_id": root_dataset_id,
                "input_artifacts": [],
                "output_artifacts": ["movement_review_annotations.json"],
                "parameters": {"app": "movement", "action": "branch"},
                "set_as_head": False,
            },
        )
        return result["dataset"]["dataset_id"]

    left_dataset_id = child_dataset("Left branch")
    right_dataset_id = child_dataset("Right branch")
    left_analysis = create_saved_movement_analysis(
        tmp_path,
        dataset_id=left_dataset_id,
    )

    response = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/analyses",
        params={
            "dataset_id": right_dataset_id,
            "logical_name": "movement.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "60",
            "burst_gap_quantile": "0.75",
            "feature_set": "movement_only",
            "ranking_method": "isolation_forest",
        },
    )

    assert response.status_code == 200, response.text
    assert left_analysis["analysis"]["analysis_id"] not in {
        item["analysis_id"] for item in response.json()["items"]
    }


def test_export_reviewed_csv_combines_portable_and_sidecar_annotations(tmp_path):
    source_path = tmp_path / "movement.csv"
    source_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set,visible,outlier_status,outlier_issue_type,manually-marked-outlier,algorithm-marked-outlier,outlier_comments
fix_1,alpha,2024-01-01T00:00:00Z,-70,40,train,true,suspected,drift,false,false,manual note
fix_2,beta,2024-01-01T01:00:00Z,-71,41,test,true,,,true,false,existing source note
fix_3,gamma,2024-01-01T02:00:00Z,-72,42,train,false,,,false,true,
""",
        encoding="utf-8",
    )
    sidecar_path = tmp_path / "movement_review_annotations.json"
    sidecar_path.write_text(
        json.dumps(
            {
                "annotations": [
                    {
                        "annotation_id": "analysis_1",
                        "source_artifact": "movement.csv",
                        "status": "confirmed",
                        "origin": "algorithm",
                        "issue_type": "burst anomaly",
                        "comment": "algorithm note",
                        "scope": {"kind": "individual", "individual": "beta"},
                    },
                    {
                        "annotation_id": "step_review:individual_review:1",
                        "step_id": "step_review",
                        "annotation_kind": "individual_review",
                        "source_artifact": "movement.csv",
                        "reviewed": True,
                        "review_decision": "ok",
                        "needs_check": False,
                        "comment": "Track looks plausible",
                        "user": "reviewer",
                        "created_at": "2026-07-30T12:00:00+00:00",
                        "scope": {"kind": "individual", "individual": "alpha"},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    output_path = tmp_path / "reviewed.csv"

    summary = export_reviewed_csv(
        source_path,
        output_path,
        source_artifact="movement.csv",
        sidecar_path=sidecar_path,
        annotation_step_ids={"analysis_1": "step_historical"},
    )

    with output_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    assert not any(name.startswith("vc_") for name in fieldnames)
    assert "manually_marked_outliers" not in fieldnames
    assert "algorithm_marked_outliers" not in fieldnames
    assert rows[0]["visible"] == "true"
    assert rows[0]["manually-marked-outlier"] == "true"
    assert rows[0]["algorithm-marked-outlier"] == "false"
    assert rows[0]["outlier_issue_type"] == "drift"
    assert rows[0]["outlier_comments"] == "manual note"
    assert rows[0]["individual-reviewed"] == "true"
    assert rows[0]["individual-review-ok"] == "true"
    assert rows[1]["visible"] == "false"
    assert rows[1]["manually-marked-outlier"] == "true"
    assert rows[1]["algorithm-marked-outlier"] == "true"
    assert rows[1]["individual-reviewed"] == "false"
    assert rows[1]["individual-review-ok"] == "false"
    assert rows[1]["outlier_status"] == "confirmed"
    assert rows[1]["outlier_issue_type"] == "burst anomaly"
    assert rows[1]["outlier_comments"] == (
        "existing source note; "
        "Already flagged in source: manually-marked-outlier=true"
    )
    assert rows[1]["outlier_flag_step_ids"] == "step_historical"
    assert rows[2]["visible"] == "false"
    assert rows[2]["algorithm-marked-outlier"] == "true"
    assert rows[2]["outlier_status"] == ""
    assert rows[2]["outlier_comments"] == "Already flagged in source: algorithm-marked-outlier=true"
    assert summary["flagged_row_count"] == 3


def test_review_individual_persists_one_ordered_lightweight_step_per_decision(tmp_path):
    clean_csv = """eventid,individual,timestamp,longitude,latitude
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1
fix_b_1,beta,2024-01-01T00:30:00Z,-71.0,41.0
"""
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=clean_csv)
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"
    source_before = (study_dir / "movement.csv").read_text(encoding="utf-8")

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/review-individual",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
            "decision": {
                "individual": "alpha",
                "review_decision": "ok",
                "needs_check": True,
                "comment": "Track looks plausible",
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    reviewed_dataset_id = payload["dataset"]["dataset_id"]
    assert payload["step"]["parameters"]["action"] == "review_individual"
    assert payload["step"]["output_artifacts"] == ["movement_review_annotations.json"]
    assert payload["step"]["summary"]["reviewed_individual_count"] == 1
    assert payload["step"]["summary"]["reviewed_ok_count"] == 1
    assert payload["step"]["summary"]["needs_check_count"] == 1
    assert (study_dir / "movement.csv").read_text(encoding="utf-8") == source_before

    _, source_path = get_dataset_artifact(study_dir, reviewed_dataset_id, "movement.csv")
    assert source_path == study_dir / "movement.csv"
    _, sidecar_path = get_dataset_artifact(
        study_dir,
        reviewed_dataset_id,
        "movement_review_annotations.json",
    )
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    assert sidecar["schema_version"] == 6
    assert [item["scope"]["individual"] for item in sidecar["annotations"]] == ["alpha"]
    assert sidecar["annotations"][0]["needs_check"] is True
    assert all(item["annotation_kind"] == "individual_review" for item in sidecar["annotations"])

    second_response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/review-individual",
        json={
            "dataset_id": reviewed_dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
            "decision": {
                "individual": "beta",
                "review_decision": "remove",
                "needs_check": False,
                "comment": "Issues were marked separately",
            },
        },
    )
    assert second_response.status_code == 200
    second_payload = second_response.json()
    assert second_payload["dataset"]["parent_dataset_id"] == reviewed_dataset_id
    assert second_payload["step"]["summary"]["reviewed_fix_keep_count"] == 0
    assert second_payload["step"]["summary"]["reviewed_remove_count"] == 1
    reviewed_dataset_id = second_payload["dataset"]["dataset_id"]
    _, sidecar_path = get_dataset_artifact(
        study_dir,
        reviewed_dataset_id,
        "movement_review_annotations.json",
    )
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    assert [item["scope"]["individual"] for item in sidecar["annotations"]] == ["alpha", "beta"]
    assert len({item["step_id"] for item in sidecar["annotations"]}) == 2

    root_overview = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/overview",
        params={"logical_name": "movement.csv"},
    )
    reviewed_overview = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{reviewed_dataset_id}/overview",
        params={"logical_name": "movement.csv"},
    )
    assert root_overview.status_code == 200
    assert reviewed_overview.status_code == 200
    assert root_overview.json()["stats"]["alpha"].get("reviewed") is not True
    reviewed_stats = reviewed_overview.json()["stats"]
    assert reviewed_stats["alpha"]["reviewed"] is True
    assert reviewed_stats["alpha"]["review_decision"] == "ok"
    assert reviewed_stats["alpha"]["needs_check"] is True
    assert reviewed_stats["beta"]["reviewed"] is True
    assert reviewed_stats["beta"]["review_decision"] == "remove"
    assert reviewed_stats["beta"]["needs_check"] is False
    assert reviewed_stats["beta"]["review_comment"] == "Issues were marked separately"

    export_response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/export-reviewed-csv",
        json={
            "dataset_id": reviewed_dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
        },
    )
    assert export_response.status_code == 200
    export_payload = export_response.json()
    analysis_id = export_payload["analysis"]["analysis_id"]
    output_name = export_payload["analysis"]["parameters"]["output_artifact"]
    download = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{analysis_id}/artifact/{output_name}"
    )
    rows = list(csv.DictReader(io.StringIO(download.text)))
    assert [row["individual-reviewed"] for row in rows] == ["true", "true", "true"]
    assert [row["individual-review-ok"] for row in rows] == ["true", "true", "false"]
    assert [row["individual-review-decision"] for row in rows] == ["ok", "ok", "remove"]
    assert [row["individual-needs-check"] for row in rows] == ["true", "true", "false"]
    assert all(row["visible"] == "true" for row in rows)
    assert all(row["manually-marked-outlier"] == "false" for row in rows)
    assert all(row["algorithm-marked-outlier"] == "false" for row in rows)


def test_review_individual_rejects_old_batch_payload(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/review-individual",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
            "decisions": [{"individual": "alpha", "review_decision": "ok"}],
        },
    )

    assert response.status_code == 400
    assert "Review one individual" in response.json()["error"]

    old_decision = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/review-individual",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
            "decision": {
                "individual": "alpha",
                "review_decision": "not_ok",
                "needs_check": False,
            },
        },
    )
    assert old_decision.status_code == 400
    assert "ok, fix_keep, or remove" in old_decision.json()["error"]


def test_reviewed_csv_artifact_name_is_based_on_source_name():
    assert (
        _reviewed_csv_artifact_name("Kays_c8aac319_raw_merged.csv")
        == "Kays_c8aac319_raw_merged_reviewed.csv"
    )


def test_export_drops_deprecated_flag_names_without_importing_them(tmp_path):
    source_path = tmp_path / "raw.csv"
    source_path.write_text(
        "eventid,individual,timestamp,longitude,latitude,manually_marked_outliers,algorithm_marked_outliers\n"
        "fix_1,alpha,2024-01-01T00:00:00Z,-70,40,false,true\n",
        encoding="utf-8",
    )
    first_output = tmp_path / "first.csv"
    second_output = tmp_path / "second.csv"

    export_reviewed_csv(source_path, first_output, source_artifact="raw.csv")
    export_reviewed_csv(first_output, second_output, source_artifact="raw.csv")

    with second_output.open(newline="", encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    assert "manually_marked_outliers" not in row
    assert "algorithm_marked_outliers" not in row
    assert row["manually-marked-outlier"] == "false"
    assert row["algorithm-marked-outlier"] == "false"
    assert row["outlier_comments"] == ""


def test_movement_export_reviewed_csv_route_creates_downloadable_analysis(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/export-reviewed-csv",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["analysis"]["parameters"]["action"] == "export_reviewed_csv"
    assert payload["summary"]["exported_row_count"] == 5
    analysis_id = payload["analysis"]["analysis_id"]
    download = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{analysis_id}/artifact/movement_reviewed.csv"
    )
    assert download.status_code == 200
    rows = list(csv.DictReader(io.StringIO(download.text)))
    assert len(rows) == 5
    assert rows[0]["visible"] == "true"
    assert rows[0]["manually-marked-outlier"] == "false"
    assert rows[0]["algorithm-marked-outlier"] == "true"
    assert not any(name.startswith("vc_") for name in rows[0])


def test_annotate_scope_persists_sidecar_without_rewriting_source_csv(tmp_path):
    clean_csv = """eventid,individual,timestamp,longitude,latitude,set
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
fix_b_1,beta,2024-01-01T00:30:00Z,-71.0,41.0,test
"""
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=clean_csv)
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"
    source_before = (study_dir / "movement.csv").read_text(encoding="utf-8")

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "burst", "burst_id": "alpha:train:burst_000000"},
            "status": "suspected",
            "origin": "algorithm",
            "issue_type": "burst anomaly",
            "comment": "Ranked as an unusual burst",
            "owner_question": "Please verify",
            "source_analysis_id": "analysis_saved",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": 3600,
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    next_dataset_id = payload["dataset"]["dataset_id"]
    assert payload["step"]["output_artifacts"] == ["movement_review_annotations.json"]
    assert payload["step"]["summary"]["scope_kind"] == "burst"
    assert payload["step"]["summary"]["resolved_fix_count"] == 2
    assert "fix_keys" not in payload["step"]["parameters"]["scope"]
    assert (study_dir / "movement.csv").read_text(encoding="utf-8") == source_before
    _, sidecar_path = get_dataset_artifact(study_dir, next_dataset_id, "movement_review_annotations.json")
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    annotation = sidecar["annotations"][0]
    assert annotation["annotation_id"] == payload["step"]["step_id"]
    assert annotation["step_id"] == payload["step"]["step_id"]
    assert annotation["origin"] == "algorithm"
    assert annotation["source_analysis_id"] == "analysis_saved"
    assert sidecar["schema_version"] == 5
    assert annotation["scope"]["row_ranges"] == [[1, 2]]
    assert "fix_keys" not in annotation["scope"]

    fixes_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{next_dataset_id}/fixes",
        params={"logical_name": "movement.csv", "burst_gap_mode": "manual", "burst_gap_seconds": "3600"},
    )
    assert fixes_response.status_code == 200
    fixes = fixes_response.json()["fixes"]
    reviewed = [fix for fix in fixes if fix.get("review", {}).get("status") == "suspected"]
    assert [fix["fix_key"] for fix in reviewed] == ["id:fix_a_1#row:1", "id:fix_a_2#row:2"]
    assert reviewed[0]["review"]["issues"][0]["origin"] == "algorithm"
    assert reviewed[0]["review"]["issues"][0]["issue_note"] == "Ranked as an unusual burst"
    assert reviewed[0]["review"]["issues"][0]["scope_kind"] == "burst"
    assert reviewed[0]["review"]["issues"][0]["scope_burst_id"] == "alpha:train:burst_000000"

    suspicious_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{next_dataset_id}/fixes",
        params={
            "logical_name": "movement.csv",
            "review_status": "suspected",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "3600",
        },
    )
    assert suspicious_response.status_code == 200
    suspicious_payload = suspicious_response.json()
    assert suspicious_payload["matching_fix_count"] == 2
    assert [
        fix["fix_key"]
        for fix in suspicious_payload["fixes"]
    ] == ["id:fix_a_1#row:1", "id:fix_a_2#row:2"]

    root_suspicious_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params={
            "logical_name": "movement.csv",
            "review_status": "suspected",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "3600",
        },
    )
    assert root_suspicious_response.status_code == 200
    assert root_suspicious_response.json()["matching_fix_count"] == 0
    assert root_suspicious_response.json()["fixes"] == []

    root_fixes_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params={
            "logical_name": "movement.csv",
            "individual": "alpha",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "3600",
        },
    )
    assert root_fixes_response.status_code == 200
    assert [fix["fix_key"] for fix in root_fixes_response.json()["fixes"]] == [
        "id:fix_a_1#row:1",
        "id:fix_a_2#row:2",
    ]
    assert all(
        fix.get("review", {}).get("status", "") != "suspected"
        for fix in root_fixes_response.json()["fixes"]
    )

    export_response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/export-reviewed-csv",
        json={
            "dataset_id": next_dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
        },
    )
    assert export_response.status_code == 200
    export_analysis_id = export_response.json()["analysis"]["analysis_id"]
    download = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{export_analysis_id}/artifact/movement_reviewed.csv"
    )
    exported = list(csv.DictReader(io.StringIO(download.text)))
    assert [row["algorithm-marked-outlier"] for row in exported] == ["true", "true", "false"]
    assert [row["outlier_issue_type"] for row in exported] == ["burst anomaly", "burst anomaly", ""]
    assert [row["outlier_flag_step_ids"] for row in exported] == [
        payload["step"]["step_id"],
        payload["step"]["step_id"],
        "",
    ]
    assert all("Ranked as an unusual burst" not in row["outlier_comments"] for row in exported)
    assert all("vc_outlier_status" not in row for row in exported)


def test_annotate_scope_flags_multiple_bursts_in_one_step(tmp_path):
    clean_csv = """eventid,individual,timestamp,longitude,latitude,set
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
fix_a_3,alpha,2024-01-01T02:00:00Z,-70.2,40.2,train
"""
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=clean_csv)
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {
                "kind": "bursts",
                "burst_ids": [
                    "alpha:train:burst_000000",
                    "alpha:train:burst_000002",
                ],
            },
            "status": "suspected",
            "origin": "manual",
            "issue_type": "burst review",
            "comment": "These two bursts need review",
            "owner_question": "Are these bursts valid?",
            "workflow_context": {
                "entry_point": "individual_review_queue",
                "active_individual": "alpha",
                "scope_kind": "bursts",
                "selection_methods": ["queue_burst_list", "map_burst_click"],
            },
            "burst_gap_mode": "manual",
            "burst_gap_seconds": 1800,
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    summary = payload["step"]["summary"]
    assert payload["step"]["title"] == "Mark 2 bursts as suspected in movement.csv"
    assert summary["scope_kind"] == "bursts"
    assert summary["annotation_count"] == 2
    assert summary["resolved_fix_count"] == 2
    assert len(summary["annotation_ids"]) == 2
    assert len(set(summary["annotation_ids"])) == 2
    assert payload["step"]["parameters"]["scope"]["burst_ids"] == [
        "alpha:train:burst_000000",
        "alpha:train:burst_000002",
    ]

    _, sidecar_path = get_dataset_artifact(
        study_dir,
        payload["dataset"]["dataset_id"],
        "movement_review_annotations.json",
    )
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    annotations = sidecar["annotations"]
    assert [item["scope"]["kind"] for item in annotations] == ["burst", "burst"]
    assert [item["scope"]["burst_id"] for item in annotations] == [
        "alpha:train:burst_000000",
        "alpha:train:burst_000002",
    ]
    assert [item["scope"]["row_ranges"] for item in annotations] == [
        [[1, 1]],
        [[3, 3]],
    ]
    assert all(
        item["workflow_context"] == {
            "entry_point": "individual_review_queue",
            "active_individual": "alpha",
            "scope_kind": "bursts",
            "selection_methods": ["queue_burst_list", "map_burst_click"],
        }
        for item in annotations
    )
    assert {item["step_id"] for item in annotations} == {payload["step"]["step_id"]}

    confirmed = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/confirm-issues",
        json={
            "dataset_id": payload["dataset"]["dataset_id"],
            "logical_name": "movement.csv",
            "confirmations": [
                {
                    "parent_annotation_id": annotations[0]["annotation_id"],
                    "fix_keys": ["id:fix_a_1#row:1"],
                }
            ],
            "user": "reviewer",
        },
    )
    assert confirmed.status_code == 200
    reviewed = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/"
        f"dataset/{confirmed.json()['dataset']['dataset_id']}/fixes",
        params={
            "logical_name": "movement.csv",
            "individual": "alpha",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": "1800",
        },
    ).json()["fixes"]
    assert reviewed[0]["review"]["status"] == "confirmed"
    assert reviewed[1].get("review", {}).get("status", "") == ""
    assert reviewed[2]["review"]["status"] == "suspected"


def test_annotate_scope_rejects_direct_confirmation(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "fix", "fix_keys": ["id:fix_a_1#row:1"]},
            "status": "confirmed",
            "origin": "manual",
            "issue_type": "drift",
            "comment": "Direct confirmation should not be allowed",
            "owner_question": "Please verify",
            "user": "reviewer",
        },
    )

    assert response.status_code == 400
    assert "confirm-issues" in response.json()["error"]


def test_annotate_scope_rejects_unknown_issue_selection_method(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "fix", "fix_keys": ["id:fix_a_1#row:1"]},
            "status": "suspected",
            "origin": "manual",
            "issue_type": "manual review",
            "comment": "Check this fix",
            "owner_question": "Is this fix valid?",
            "workflow_context": {
                "entry_point": "movement_map",
                "scope_kind": "fix",
                "selection_methods": ["untrusted_method"],
            },
            "user": "reviewer",
        },
    )

    assert response.status_code == 400
    assert response.json()["error"] == "Invalid issue selection method"


def test_confirm_issues_persists_sidecar_without_copying_csv(tmp_path):
    clean_csv = """eventid,individual,timestamp,longitude,latitude,set
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
fix_b_1,beta,2024-01-01T00:30:00Z,-71.0,41.0,test
"""
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=clean_csv)
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"
    source_before = (study_dir / "movement.csv").read_text(encoding="utf-8")

    suspected_response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "fix", "fix_keys": ["id:fix_a_2#row:2"]},
            "status": "suspected",
            "origin": "threshold",
            "issue_type": "speed threshold",
            "comment": "Above selected speed threshold",
            "owner_question": "Is this movement plausible?",
            "user": "reviewer",
        },
    )
    assert suspected_response.status_code == 200
    suspected_payload = suspected_response.json()
    suspected_dataset_id = suspected_payload["dataset"]["dataset_id"]
    suspected_annotation_id = suspected_payload["step"]["step_id"]

    confirm_response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/confirm-issues",
        json={
            "dataset_id": suspected_dataset_id,
            "logical_name": "movement.csv",
            "confirmations": [
                {
                    "parent_annotation_id": suspected_annotation_id,
                    "fix_keys": ["id:fix_a_2#row:2"],
                }
            ],
            "note": "Confirmed during track review",
            "user": "confirmer",
        },
    )

    assert confirm_response.status_code == 200
    confirmed_payload = confirm_response.json()
    confirmed_dataset_id = confirmed_payload["dataset"]["dataset_id"]
    assert confirmed_payload["step"]["output_artifacts"] == ["movement_review_annotations.json"]
    assert confirmed_payload["step"]["summary"]["confirmed_fix_count"] == 1
    assert confirmed_payload["step"]["summary"]["algorithm_marked_fix_count"] == 1
    assert confirmed_payload["step"]["summary"]["materialized_csv"] is False
    assert confirmed_payload["step"]["parameters"]["confirmations"][0]["row_ranges"] == [[2, 2]]
    assert "fix_keys" not in confirmed_payload["step"]["parameters"]["confirmations"][0]
    assert (study_dir / "movement.csv").read_text(encoding="utf-8") == source_before

    _, suspected_csv_path = get_dataset_artifact(
        study_dir,
        suspected_dataset_id,
        "movement.csv",
    )
    _, confirmed_csv_path = get_dataset_artifact(
        study_dir,
        confirmed_dataset_id,
        "movement.csv",
    )
    assert confirmed_csv_path == suspected_csv_path
    assert confirmed_csv_path.read_text(encoding="utf-8") == clean_csv
    rows = list(csv.DictReader(confirmed_csv_path.open(encoding="utf-8")))
    assert "visible" not in rows[0]
    assert "algorithm-marked-outlier" not in rows[0]

    _, sidecar_path = get_dataset_artifact(
        study_dir,
        confirmed_dataset_id,
        "movement_review_annotations.json",
    )
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    confirmation = sidecar["annotations"][-1]
    assert sidecar["schema_version"] == 5
    assert confirmation["annotation_kind"] == "confirmation"
    assert confirmation["parent_annotation_id"] == suspected_annotation_id
    assert confirmation["origin"] == "threshold"
    assert confirmation["scope"]["row_ranges"] == [[2, 2]]
    assert "fix_keys" not in confirmation["scope"]

    confirmed_fixes_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{confirmed_dataset_id}/fixes",
        params={"logical_name": "movement.csv", "review_status": "confirmed"},
    )
    assert confirmed_fixes_response.status_code == 200
    confirmed_fixes = confirmed_fixes_response.json()["fixes"]
    assert [fix["fix_key"] for fix in confirmed_fixes] == ["id:fix_a_2#row:2"]
    assert confirmed_fixes[0]["review"]["issues"][-1]["parent_annotation_id"] == suspected_annotation_id
    assert confirmed_fixes[0]["analytically_excluded"] is True

    export_response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/export-reviewed-csv",
        json={
            "dataset_id": confirmed_dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
        },
    )
    assert export_response.status_code == 200
    export_analysis_id = export_response.json()["analysis"]["analysis_id"]
    download = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{export_analysis_id}/artifact/movement_reviewed.csv"
    )
    exported = list(csv.DictReader(io.StringIO(download.text)))
    assert exported[1]["visible"] == "false"
    assert exported[1]["manually-marked-outlier"] == "false"
    assert exported[1]["algorithm-marked-outlier"] == "true"
    assert exported[1]["outlier_status"] == "confirmed"
    assert exported[1]["outlier_issue_type"] == "speed threshold"
    assert suspected_annotation_id in exported[1]["outlier_flag_step_ids"]
    assert confirmed_payload["step"]["step_id"] in exported[1]["outlier_flag_step_ids"]
    assert not any(name.startswith("vc_") for name in exported[1])


def test_dismiss_issues_partially_resolves_parent_and_preserves_audit_history(tmp_path):
    clean_csv = """eventid,individual,timestamp,longitude,latitude,set
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
fix_b_1,beta,2024-01-01T00:30:00Z,-71.0,41.0,test
"""
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=clean_csv)
    base_url = "/api/apps/movement/family/movement_clean/study/test_study"
    study_dir = tmp_path / "data" / "movement_clean" / "test_study"

    suspected = client.post(
        f"{base_url}/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {
                "kind": "fix",
                "fix_keys": ["id:fix_a_1#row:1", "id:fix_a_2#row:2"],
            },
            "status": "suspected",
            "origin": "algorithm",
            "issue_type": "filter run",
            "comment": "Matched the test filter",
            "source_analysis_id": "analysis_filter_1",
            "user": "reviewer",
        },
    )
    assert suspected.status_code == 200
    suspected_payload = suspected.json()
    parent_id = suspected_payload["step"]["step_id"]

    dismissed = client.post(
        f"{base_url}/actions/dismiss-issues",
        json={
            "dataset_id": suspected_payload["dataset"]["dataset_id"],
            "expected_current_dataset_id": suspected_payload["dataset"]["dataset_id"],
            "logical_name": "movement.csv",
            "dismissals": [{
                "parent_annotation_id": parent_id,
                "fix_keys": ["id:fix_a_1#row:1"],
            }],
            "note": "Plausible after checking the track",
            "user": "reviewer",
        },
    )
    assert dismissed.status_code == 200
    dismissed_payload = dismissed.json()
    assert dismissed_payload["step"]["parameters"]["action"] == "dismiss_issues"
    assert dismissed_payload["step"]["summary"]["dismissed_fix_count"] == 1
    assert dismissed_payload["step"]["parameters"]["dismissals"][0]["row_ranges"] == [[1, 1]]
    assert (study_dir / "movement.csv").read_text(encoding="utf-8") == clean_csv

    _, sidecar_path = get_dataset_artifact(
        study_dir,
        dismissed_payload["dataset"]["dataset_id"],
        "movement_review_annotations.json",
    )
    annotations = json.loads(sidecar_path.read_text(encoding="utf-8"))["annotations"]
    assert len(annotations) == 2
    assert annotations[0]["annotation_id"] == parent_id
    assert annotations[1]["annotation_kind"] == "dismissal"
    assert annotations[1]["parent_annotation_id"] == parent_id
    assert annotations[1]["comment"] == "Plausible after checking the track"

    fixes = client.get(
        f"{base_url}/dataset/{dismissed_payload['dataset']['dataset_id']}/fixes",
        params={"logical_name": "movement.csv", "individual": "alpha"},
    )
    assert fixes.status_code == 200
    by_key = {item["fix_key"]: item for item in fixes.json()["fixes"]}
    assert fixes.json()["stats"]["alpha"]["unresolved_suspected_count"] == 1
    assert fixes.json()["stats"]["alpha"]["unresolved_issue_origins"] == ["algorithm"]
    first_review = by_key["id:fix_a_1#row:1"]["review"]
    second_review = by_key["id:fix_a_2#row:2"]["review"]
    assert first_review["status"] == ""
    assert first_review["effective_issues"][0]["status"] == "dismissed"
    assert second_review["status"] == "suspected"
    assert second_review["effective_issues"][0]["status"] == "suspected"

    exported_response = client.post(
        f"{base_url}/actions/export-reviewed-csv",
        json={
            "dataset_id": dismissed_payload["dataset"]["dataset_id"],
            "logical_name": "movement.csv",
            "user": "reviewer",
        },
    )
    assert exported_response.status_code == 200
    export_analysis_id = exported_response.json()["analysis"]["analysis_id"]
    download = client.get(
        f"{base_url}/analysis/{export_analysis_id}/artifact/movement_reviewed.csv"
    )
    exported_rows = list(csv.DictReader(io.StringIO(download.text)))
    assert exported_rows[0]["outlier_status"] == ""
    assert exported_rows[0]["algorithm-marked-outlier"] == "false"
    assert exported_rows[1]["outlier_status"] == "suspected"
    assert exported_rows[1]["algorithm-marked-outlier"] == "true"

    repeated = client.post(
        f"{base_url}/actions/dismiss-issues",
        json={
            "dataset_id": dismissed_payload["dataset"]["dataset_id"],
            "logical_name": "movement.csv",
            "dismissals": [{
                "parent_annotation_id": parent_id,
                "fix_keys": ["id:fix_a_1#row:1"],
            }],
            "user": "reviewer",
        },
    )
    assert repeated.status_code == 400
    assert "already resolved" in repeated.json()["error"]


def test_roi_preview_flag_and_unflag_are_exact_and_individual_scoped(tmp_path):
    clean_csv = """eventid,individual,timestamp,longitude,latitude,set,outlier_status,outlier_issue_type,outlier_comments
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.00,40.00,train,suspected,drift,Existing suspicion
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.05,40.05,test,,,
fix_a_3,alpha,2024-01-01T02:00:00Z,-72.00,42.00,train,,,
fix_b_1,beta,2024-01-01T00:30:00Z,-70.02,40.02,test,,,
"""
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=clean_csv)
    base_url = "/api/apps/movement/family/movement_clean/study/test_study"
    polygon = [[-70.2, 39.9], [-69.9, 39.9], [-69.9, 40.2], [-70.2, 40.2]]

    assert point_in_polygon(-70.0, 40.0, polygon) is True
    assert point_in_polygon(-72.0, 42.0, polygon) is False
    preview = client.post(
        f"{base_url}/actions/preview-roi",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "roi", "polygon": polygon, "individuals": ["alpha"]},
        },
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["match_count"] == 2
    assert preview.json()["unreviewed_count"] == 1
    assert preview.json()["suspected_count"] == 1
    assert preview.json()["dismissible_fix_count"] == 1

    flagged = client.post(
        f"{base_url}/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "roi", "polygon": polygon, "individuals": ["alpha"]},
            "status": "suspected",
            "origin": "manual",
            "issue_type": "location review",
            "comment": "Inside the ROI",
            "workflow_context": {
                "entry_point": "individual_review_queue",
                "scope_kind": "roi",
                "active_individual": "alpha",
                "selection_methods": ["map_polygon"],
            },
            "user": "reviewer",
        },
    )
    assert flagged.status_code == 200, flagged.text
    assert flagged.json()["step"]["summary"]["resolved_fix_count"] == 1
    flagged_dataset_id = flagged.json()["dataset"]["dataset_id"]
    persisted_scope = flagged.json()["step"]["parameters"]["scope"]
    assert persisted_scope["kind"] == "roi"
    assert persisted_scope["polygon"] == polygon
    assert persisted_scope["individuals"] == ["alpha"]

    flagged_preview = client.post(
        f"{base_url}/actions/preview-roi",
        json={
            "dataset_id": flagged_dataset_id,
            "logical_name": "movement.csv",
            "scope": {"kind": "roi", "polygon": polygon, "individuals": ["alpha"]},
        },
    )
    assert flagged_preview.status_code == 200, flagged_preview.text
    flagged_preview_payload = flagged_preview.json()
    assert flagged_preview_payload["suspected_count"] == 2
    assert flagged_preview_payload["dismissible_fix_count"] == 2

    dismissed = client.post(
        f"{base_url}/actions/dismiss-issues",
        json={
            "dataset_id": flagged_dataset_id,
            "expected_current_dataset_id": flagged_dataset_id,
            "logical_name": "movement.csv",
            "dismissals": flagged_preview_payload["dismissals"],
            "note": "Region checked",
            "user": "reviewer",
        },
    )
    assert dismissed.status_code == 200, dismissed.text
    assert dismissed.json()["step"]["summary"]["dismissed_fix_count"] == 2


def test_movement_fixes_route_rejects_invalid_repeated_individual(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params=[("logical_name", "movement.csv"), ("individuals", "bad\x01value")],
    )

    assert response.status_code == 404
    assert response.json()["error"] == "Invalid individual"


def test_movement_report_generator_uses_compilable_template_file():
    template_text = REPORT_ANALYSIS_TEMPLATE_PATH.read_text(encoding="utf-8").strip() + "\n"

    assert GENERATE_REPORT_SCRIPT.endswith(template_text)
    assert "_VIBECLEANING_BUNDLED_SOURCES" in GENERATE_REPORT_SCRIPT
    assert "repo_root" not in GENERATE_REPORT_SCRIPT
    compile(GENERATE_REPORT_SCRIPT, str(REPORT_ANALYSIS_TEMPLATE_PATH), "exec")


def test_build_issue_sections_keeps_all_issue_types_when_snapshots_are_sampled():
    matched_records = [
        {
            "fix_key": "row:1",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 1,
            "time_text": "2024-01-01T00:00:00Z",
            "lon": -70.0,
            "lat": 40.0,
            "step_length_m": 100.0,
            "speed_mps": 2.0,
            "time_delta_s": 50.0,
            "review": {
                "status": "suspected",
                "issue_id": "issue_1",
                "issue_type": "spike",
                "issue_note": "Spike issue",
                "owner_question": "Is this a spike?",
            },
            "raw": {"hdop": "1.2"},
        },
        {
            "fix_key": "row:2",
            "individual": "beta",
            "set_name": "train",
            "time_ms": 2,
            "time_text": "2024-01-01T01:00:00Z",
            "lon": -71.0,
            "lat": 41.0,
            "step_length_m": 150.0,
            "speed_mps": 3.0,
            "time_delta_s": 50.0,
            "review": {
                "status": "suspected",
                "issue_id": "issue_2",
                "issue_type": "drift",
                "issue_note": "Drift issue",
                "owner_question": "Is this drift?",
            },
            "raw": {"hdop": "3.8"},
        },
    ]
    snapshot_windows = [
        {
            "snapshot_key": "snapshot_01",
            "caption": "spike | alpha",
            "individual": "alpha",
            "set_name": "train",
            "issue_type": "spike",
            "issue_types": ["spike"],
            "anchor_row_ranges": [[1, 1]],
            "report_row_ranges": [[1, 1]],
            "start_fix_key": "row:1",
            "end_fix_key": "row:1",
            "start_time_ms": 1,
            "end_time_ms": 1,
            "start_time_text": "2024-01-01T00:00:00Z",
            "end_time_text": "2024-01-01T00:00:00Z",
            "window_fix_count": 1,
        }
    ]

    sections = build_issue_sections(
        matched_records,
        snapshot_windows,
        fieldnames=["hdop"],
        columns={},
    )

    assert [section["issue_type"] for section in sections] == ["drift", "spike"]
    assert sections[0]["examples"]
    assert sections[1]["examples"][0]["snapshot_key"] == "snapshot_01"


def test_build_issue_sections_adds_issue_field_summary_per_individual():
    matched_records = [
        {
            "fix_key": "row:1",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 1,
            "time_text": "2024-01-01T00:00:00Z",
            "lon": -70.0,
            "lat": 40.0,
            "step_length_m": 100.0,
            "speed_mps": 2.0,
            "time_delta_s": 50.0,
            "review": {
                "status": "suspected",
                "issue_id": "issue_1",
                "issue_type": "speed",
                "issue_field": "speed_mps",
                "issue_note": "Speed issue",
                "owner_question": "Is this speed plausible?",
            },
            "raw": {"hdop": "1.2"},
        },
        {
            "fix_key": "fix_b",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 2,
            "time_text": "2024-01-01T01:00:00Z",
            "lon": -70.1,
            "lat": 40.1,
            "step_length_m": 150.0,
            "speed_mps": 8.0,
            "time_delta_s": 50.0,
            "review": {
                "status": "suspected",
                "issue_id": "issue_1",
                "issue_type": "speed",
                "issue_field": "speed_mps",
                "issue_note": "Speed issue",
                "owner_question": "Is this speed plausible?",
            },
            "raw": {"hdop": "1.8"},
        },
    ]

    sections = build_issue_sections(
        matched_records,
        snapshot_windows=[],
        fieldnames=["hdop"],
        columns={},
    )

    assert sections[0]["issue_field"] == "speed_mps"
    assert sections[0]["individual_rows"][0]["issue_field_summary"] == "median 5.000; range 2.000 to 8.000"


def test_build_issue_sections_keeps_window_examples_with_their_captured_issue_type():
    matched_records = [
        {
            "fix_key": "row:1",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 1,
            "time_text": "2024-01-01T00:00:00Z",
            "lon": -70.0,
            "lat": 40.0,
            "step_length_m": 100.0,
            "speed_mps": 2.0,
            "time_delta_s": 50.0,
            "review": {
                "status": "suspected",
                "issue_id": "issue_1",
                "issue_type": "gps",
                "issues": [
                    {"status": "suspected", "issue_id": "issue_1", "issue_type": "gps"},
                    {"status": "suspected", "issue_id": "issue_2", "issue_type": "speed"},
                ],
                "issue_note": "Issue note",
                "owner_question": "Owner question",
            },
            "raw": {"hdop": "9.9"},
        }
    ]
    snapshot_windows = [
        {
            "snapshot_key": "snapshot_gps",
            "caption": "gps | alpha",
            "individual": "alpha",
            "set_name": "train",
            "issue_type": "gps",
            "issue_types": ["gps"],
            "anchor_row_ranges": [[1, 1]],
            "report_row_ranges": [[1, 1]],
            "start_fix_key": "row:1",
            "end_fix_key": "row:1",
            "start_time_ms": 1,
            "end_time_ms": 1,
            "start_time_text": "2024-01-01T00:00:00Z",
            "end_time_text": "2024-01-01T00:00:00Z",
            "window_fix_count": 1,
        }
    ]

    sections = build_issue_sections(
        matched_records,
        snapshot_windows,
        fieldnames=["hdop"],
        columns={},
    )

    examples_by_issue = {section["issue_type"]: section["examples"] for section in sections}
    assert examples_by_issue["gps"][0]["snapshot_key"] == "snapshot_gps"
    assert examples_by_issue["speed"][0]["snapshot_key"] == ""


def test_html_report_generates_svg_fallback_when_auto_snapshot_is_missing():
    sections = [
        {
            "issue_type": "speed",
            "records": [
                {
                    "fix_key": "fix_a",
                    "individual": "alpha",
                    "time_ms": 1,
                    "time_text": "2024-01-01T00:00:00Z",
                    "lon": -70.0,
                    "lat": 40.0,
                    "step_length_m": 100.0,
                    "speed_mps": 2.0,
                    "review": {"status": "suspected"},
                },
                {
                    "fix_key": "fix_b",
                    "individual": "alpha",
                    "time_ms": 2,
                    "time_text": "2024-01-01T01:00:00Z",
                    "lon": -70.2,
                    "lat": 40.2,
                    "step_length_m": 150.0,
                    "speed_mps": 3.0,
                    "review": {"status": "suspected"},
                },
            ],
            "issue_ids": ["issue_1"],
            "issue_field": "speed_mps",
            "issue_threshold": "> 2.5",
            "issue_note": "Speed issue",
            "owner_question": "Is this speed plausible?",
            "status_counts": {"suspected": 2},
            "individual_rows": [
                {
                    "individual": "alpha",
                    "fix_count": 2,
                    "issue_ids": ["issue_1"],
                    "first_time_text": "2024-01-01T00:00:00Z",
                    "last_time_text": "2024-01-01T01:00:00Z",
                    "issue_field_summary": "median 2.500; range 2.000 to 3.000",
                    "max_step": 150.0,
                    "max_speed": 3.0,
                }
            ],
            "examples": [
                {
                    "snapshot_key": "snapshot_01",
                    "caption": "speed | alpha",
                    "individual": "alpha",
                    "set_name": "train",
                    "start_time_ms": 1,
                    "end_time_ms": 2,
                    "start_time_text": "2024-01-01T00:00:00Z",
                    "end_time_text": "2024-01-01T01:00:00Z",
                    "window_fix_count": 2,
                    "suspicious_fix_count": 2,
                    "issue_ids": ["issue_1"],
                    "status_counts": {"suspected": 2},
                    "max_step": 150.0,
                    "max_speed": 3.0,
                    "quality_lines": [],
                    "map_points": [
                        {"lon": -70.0, "lat": 40.0, "fix_key": "fix_a", "time_ms": 1},
                        {"lon": -70.2, "lat": 40.2, "fix_key": "fix_b", "time_ms": 2},
                    ],
                }
            ],
            "first_time_text": "2024-01-01T00:00:00Z",
            "last_time_text": "2024-01-01T01:00:00Z",
            "quality_fields": [],
        }
    ]

    html = build_html_report(
        "movement.csv",
        "tester",
        "auto",
        sections,
        {},
        2,
    )

    assert "data:image/svg+xml;base64," in html
    assert "No auto-rendered map snapshot included for this example." not in html


def test_format_individual_profile_helpers():
    assert format_temporal_resolution(3600) == "60 minutes"
    assert format_monitoring_span(1704067200000, 1704153600000) == "2024-01-01 to 2024-01-02"


def test_build_individual_profile_sections_extracts_metadata_and_bursts(tmp_path):
    csv_path = write_profile_csv(tmp_path / "movement.csv")

    fieldnames, columns, _, valid_records = load_rows_with_context(csv_path)
    sections = build_individual_profile_sections(valid_records, fieldnames, columns, ["alpha"], "movement.csv")

    assert len(sections) == 1
    section = sections[0]
    assert section["study_name"] == "Study A"
    assert section["study_id"] == "study_001"
    assert section["animal_id"] == "alpha"
    assert section["species"] == "Cervus elaphus"
    assert section["median_temporal_resolution_text"] == "60 minutes"
    assert section["median_speed_mps"] is not None
    assert section["median_speed_text"].endswith("m/s")
    assert section["median_speed_excluding_suspected_mps"] is not None
    assert section["median_speed_excluding_suspected_text"].endswith("m/s")
    assert section["monitoring_text"] == "2024-01-01 to 2024-01-01"
    assert section["source"] == "movebank.mar2025"
    assert section["burst_count"] == 2
    assert section["reviewed_fix_count"] == 1
    assert section["map_data_url"].startswith("data:image/svg+xml;base64,")
    encoded = section["map_data_url"].split(",", 1)[1]
    svg = base64.b64decode(encoded).decode("utf-8")
    assert "Longitude" in svg
    assert "stroke-dasharray" in svg
    assert "°W" in svg or "°E" in svg


def test_report_features_recompute_across_confirmed_fix(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        "eventid,individual,timestamp,longitude,latitude,outlier_status,visible\n"
        "fix_a,alpha,2024-01-01T00:00:00Z,-70.0,40.0,,true\n"
        "fix_b,alpha,2024-01-01T01:00:00Z,-70.1,40.1,confirmed,false\n"
        "fix_c,alpha,2024-01-01T02:00:00Z,-70.2,40.2,,true\n",
        encoding="utf-8",
    )

    _, _, _, records = load_rows_with_context(csv_path)
    recompute_analytical_movement_context(records)

    assert records[1]["analytically_excluded"] is True
    assert records[1]["speed_mps"] is None
    assert records[2]["analytically_excluded"] is False
    assert records[0]["time_delta_s"] == 7200.0
    assert records[0]["step_length_m"] is not None
    assert records[2]["time_delta_s"] is None


def test_report_loader_retains_only_selected_individuals_with_source_row_keys(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        "eventid,individual,timestamp,longitude,latitude\n"
        "fix_a,alpha,2024-01-01T00:00:00Z,-70.0,40.0\n"
        "fix_b,beta,2024-01-01T00:30:00Z,-71.0,41.0\n"
        "fix_c,alpha,2024-01-01T01:00:00Z,-70.1,40.1\n",
        encoding="utf-8",
    )

    _, _, rows, records = load_rows_with_context(
        csv_path,
        selected_individuals={"alpha"},
    )

    assert len(rows) == 2
    assert [record["individual"] for record in records] == ["alpha", "alpha"]
    assert [record["fix_key"] for record in records] == [
        "id:fix_a#row:1",
        "id:fix_c#row:3",
    ]
    assert records[0]["time_delta_s"] == 3600.0
    assert records[0]["step_length_m"] is not None
    assert records[1]["time_delta_s"] is None


def test_build_individual_profile_html_report_omits_optional_fields_when_missing():
    sections = [
        {
            "individual": "alpha",
            "study_name": "Study A",
            "study_id": "",
            "animal_id": "alpha",
            "species": "Cervus elaphus",
            "median_temporal_resolution_s": 3600.0,
            "median_temporal_resolution_text": "60 minutes",
            "median_speed_mps": 4.2,
            "median_speed_text": "4.20 m/s",
            "median_speed_excluding_suspected_mps": 3.8,
            "median_speed_excluding_suspected_text": "3.80 m/s",
            "monitoring_start_ms": 1,
            "monitoring_end_ms": 2,
            "monitoring_text": "2024-01-01 to 2024-01-01",
            "source": "movement.csv",
            "burst_count": None,
            "row_count": 2,
            "reviewed_fix_count": 0,
            "review_status_counts": {},
            "issue_breakdown": [],
            "issue_summary_lines": [],
            "map_data_url": "data:image/svg+xml;base64,abc",
        }
    ]

    html = build_individual_profile_html_report("movement.csv", "tester", sections)

    assert "Study ID" not in html
    assert "No. of bursts" not in html
    assert "<strong>Median speed:</strong> 4.20 m/s" in html
    assert "<strong>Source file:</strong> movement.csv" in html
    assert "<strong>Total fixes:</strong> 2" in html
    assert "<strong>Median speed excluding suspected fixes:</strong> 3.80 m/s" not in html
    assert "Issue Summary" not in html


def test_build_individual_profile_html_report_prefers_snapshot_artifact_when_available():
    sections = [
        {
            "individual": "alpha",
            "snapshot_key": "individual_profile::alpha",
            "study_name": "Study A",
            "study_id": "study_001",
            "animal_id": "alpha",
            "species": "Cervus elaphus",
            "median_temporal_resolution_s": 3600.0,
            "median_temporal_resolution_text": "60 minutes",
            "median_speed_mps": 4.2,
            "median_speed_text": "4.20 m/s",
            "median_speed_excluding_suspected_mps": 3.8,
            "median_speed_excluding_suspected_text": "3.80 m/s",
            "monitoring_start_ms": 1,
            "monitoring_end_ms": 2,
            "monitoring_text": "2024-01-01 to 2024-01-01",
            "source": "movement.csv",
            "burst_count": None,
            "row_count": 2,
            "reviewed_fix_count": 0,
            "review_status_counts": {},
            "issue_breakdown": [],
            "issue_summary_lines": [],
            "map_data_url": "data:image/svg+xml;base64,fallback",
        }
    ]

    html = build_individual_profile_html_report(
        "movement.csv",
        "tester",
        sections,
        {"individual_profile::alpha": {"artifact_name": "movement_snapshot_01.png"}},
    )

    assert 'src="movement_snapshot_01.png"' in html

def test_build_individual_profile_html_report_collapses_reviewed_fix_summary():
    sections = [
        {
            "individual": "alpha",
            "snapshot_key": "individual_profile::alpha",
            "study_name": "Study A",
            "study_id": "study_001",
            "animal_id": "alpha",
            "species": "Cervus elaphus",
            "median_temporal_resolution_s": 3600.0,
            "median_temporal_resolution_text": "60 minutes",
            "median_speed_mps": 4.2,
            "median_speed_text": "4.20 m/s",
            "median_speed_excluding_suspected_mps": 3.8,
            "median_speed_excluding_suspected_text": "3.80 m/s",
            "monitoring_start_ms": 1,
            "monitoring_end_ms": 2,
            "monitoring_text": "2024-01-01 to 2024-01-01",
            "source": "movement.csv",
            "burst_count": None,
            "row_count": 3,
            "reviewed_fix_count": 2,
            "review_status_counts": {"suspected": 1, "confirmed": 1},
            "issue_breakdown": [],
            "issue_summary_lines": [],
            "map_data_url": "data:image/svg+xml;base64,abc",
        }
    ]

    html = build_individual_profile_html_report("movement.csv", "tester", sections)

    assert "<strong>Flagged fixes:</strong> 2" in html
    assert "<strong>Median speed excluding confirmed outliers:</strong> 3.80 m/s" in html
    assert "Reviewed fixes" not in html
    assert "Status counts" not in html


def test_movement_generate_report_route_keeps_issue_first_behavior_and_embeds_snapshots(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)
    png_bytes = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
        "AAIAAAoAAv/lPAAAAABJRU5ErkJggg=="
    )

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/generate-report",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "report_type": "issue_first",
            "fix_keys": ["id:fix_a_1#row:1"],
            "issue_ids": ["issue_1"],
            "report_fixes": [
                {
                    "fix_key": "id:fix_a_1#row:1",
                    "individual": "alpha",
                    "set_name": "train",
                    "time_ms": 1704067200000,
                    "time_text": "2024-01-01T00:00:00Z",
                    "lon": -70.0,
                    "lat": 40.0,
                    "step_length_m": None,
                    "speed_mps": None,
                    "time_delta_s": None,
                    "attributes": {},
                    "review": {
                        "status": "suspected",
                        "issue_id": "issue_1",
                        "issue_type": "drift",
                        "issue_field": "speed_mps",
                        "issue_threshold": "",
                        "issues": [],
                        "issue_note": "first alpha issue",
                        "owner_question": "question 1",
                        "review_user": "reviewer",
                        "reviewed_at": "2024-01-02T00:00:00Z",
                    },
                }
            ],
            "snapshot_windows": [
                {
                    "snapshot_key": "snapshot_01",
                    "caption": "alpha drift",
                    "individual": "alpha",
                    "set_name": "train",
                    "issue_type": "drift",
                    "issue_types": ["drift"],
                    "anchor_fix_keys": ["id:fix_a_1#row:1"],
                    "report_fix_keys": ["id:fix_a_1#row:1"],
                    "start_fix_key": "id:fix_a_1#row:1",
                    "end_fix_key": "id:fix_a_1#row:1",
                    "start_time_ms": 1704067200000,
                    "end_time_ms": 1704067200000,
                    "start_time_text": "2024-01-01T00:00:00Z",
                    "end_time_text": "2024-01-01T00:00:00Z",
                    "window_fix_count": 1,
                }
            ],
            "screenshot_mode": "auto",
            "snapshots": [
                {
                    "snapshot_key": "snapshot_01",
                    "caption": "alpha drift",
                    "data_url": "data:image/png;base64,"
                    + base64.b64encode(png_bytes).decode("ascii"),
                }
            ],
            "user": "tester",
        },
    )

    assert response.status_code == 200
    analysis_id = response.json()["analysis"]["analysis_id"]
    html_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{analysis_id}/artifact/movement_outlier_report.html"
    )
    appendix_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{analysis_id}/artifact/movement_outlier_fixes.csv"
    )
    assert html_response.status_code == 200
    assert appendix_response.status_code == 200
    assert "Movement Outlier Review Report" in html_response.text
    assert (
        "data:image/png;base64," + base64.b64encode(png_bytes).decode("ascii")
        in html_response.text
    )
    assert 'src="movement_snapshot_01.png"' not in html_response.text
    parameters = response.json()["analysis"]["parameters"]
    assert parameters["fix_row_ranges"] == [[1, 1]]
    assert "fix_keys" not in parameters
    assert "report_fixes" not in parameters


def test_movement_generate_report_route_supports_single_individual_profile(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=PROFILE_CSV_CONTENT)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/generate-report",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "report_type": "individual_profile",
            "individuals": ["gamma"],
            "user": "tester",
        },
    )

    assert response.status_code == 200
    analysis_id = response.json()["analysis"]["analysis_id"]
    html_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{analysis_id}/artifact/movement_individual_reports.html"
    )
    markdown_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{analysis_id}/artifact/movement_individual_reports.md"
    )
    assert html_response.status_code == 200
    assert markdown_response.status_code == 200
    assert "Movement Individual Profile Report" in html_response.text
    assert "Individual: gamma" in html_response.text
    assert "data:image/svg+xml;base64," in html_response.text
    assert "Study ID" in html_response.text
    assert "Issue Summary" not in html_response.text


def test_movement_report_stores_snapshot_as_checksummed_analysis_input(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=PROFILE_CSV_CONTENT)
    png_bytes = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
        "AAIAAAoAAv/lPAAAAABJRU5ErkJggg=="
    )

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/generate-report",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "report_type": "individual_profile",
            "individuals": ["gamma"],
            "snapshots": [
                {
                    "snapshot_key": "individual_profile::gamma",
                    "caption": "gamma whole track",
                    "data_url": "data:image/png;base64,"
                    + base64.b64encode(png_bytes).decode("ascii"),
                }
            ],
            "user": "tester",
        },
    )

    assert response.status_code == 200
    analysis = response.json()["analysis"]
    assert analysis["parameters"]["snapshots"] == [
        {
            "artifact_name": "movement_snapshot_01.png",
            "attachment_name": "movement_snapshot_01.png",
            "caption": "gamma whole track",
            "snapshot_key": "individual_profile::gamma",
        }
    ]
    assert "data_url" not in json.dumps(analysis["parameters"])
    assert len(analysis["input_attachments"]) == 1
    attachment = analysis["input_attachments"][0]
    assert attachment["logical_name"] == "movement_snapshot_01.png"
    assert attachment["size"] == len(png_bytes)
    assert len(attachment["sha256"]) == 64

    study_dir = tmp_path / "data" / "movement_clean" / "test_study"
    assert (study_dir / attachment["path"]).read_bytes() == png_bytes
    spec = json.loads((study_dir / analysis["spec_path"]).read_text(encoding="utf-8"))
    assert spec["input_attachments"][0]["sha256"] == attachment["sha256"]
    assert "data_url" not in json.dumps(spec)

    snapshot_response = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/"
        f"analysis/{analysis['analysis_id']}/artifact/movement_snapshot_01.png"
    )
    html_response = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/"
        f"analysis/{analysis['analysis_id']}/artifact/movement_individual_reports.html"
    )
    assert snapshot_response.status_code == 200
    assert snapshot_response.content == png_bytes
    assert html_response.status_code == 200
    assert (
        "data:image/png;base64," + base64.b64encode(png_bytes).decode("ascii")
        in html_response.text
    )
    assert 'src="movement_snapshot_01.png"' not in html_response.text


def test_movement_generate_report_route_supports_combined_multi_individual_profile(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=PROFILE_CSV_CONTENT)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/generate-report",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "report_type": "individual_profile",
            "individuals": ["beta", "alpha"],
            "output_mode": "combined",
            "user": "tester",
        },
    )

    assert response.status_code == 200
    analysis_id = response.json()["analysis"]["analysis_id"]
    html_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{analysis_id}/artifact/movement_individual_reports.html"
    )
    assert html_response.status_code == 200
    assert "Individual: alpha" in html_response.text
    assert "Individual: beta" in html_response.text
    assert html_response.text.count("Issue Summary") == 2


def test_movement_generate_report_route_supports_separate_multi_individual_profile(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=PROFILE_CSV_CONTENT)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/generate-report",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "report_type": "individual_profile",
            "individuals": ["alpha", "beta"],
            "output_mode": "separate",
            "user": "tester",
        },
    )

    assert response.status_code == 200
    analysis = response.json()["analysis"]
    analysis_id = analysis["analysis_id"]
    realized = {item["logical_name"] for item in analysis["realized_output_artifacts"]}
    assert "movement_individual_report_index.html" in realized
    alpha_html = next(name for name in realized if name.endswith("_alpha.html"))
    index_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{analysis_id}/artifact/movement_individual_report_index.html"
    )
    alpha_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/analysis/{analysis_id}/artifact/{alpha_html}"
    )
    assert index_response.status_code == 200
    assert alpha_response.status_code == 200
    assert alpha_html in index_response.text
    assert "Individual: alpha" in alpha_response.text


def test_movement_generate_report_route_validates_individual_profile_inputs(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path, csv_content=PROFILE_CSV_CONTENT)

    missing_individuals = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/generate-report",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "report_type": "individual_profile",
            "individuals": [],
            "user": "tester",
        },
    )
    invalid_type = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/generate-report",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "report_type": "bad_mode",
            "individuals": ["alpha"],
            "user": "tester",
        },
    )
    invalid_output_mode = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/generate-report",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "report_type": "individual_profile",
            "individuals": ["alpha"],
            "output_mode": "bad_mode",
            "user": "tester",
        },
    )

    assert missing_individuals.status_code == 400
    assert missing_individuals.json()["error"] == "Select at least one individual"
    assert invalid_type.status_code == 400
    assert invalid_type.json()["error"] == "Invalid report type"
    assert invalid_output_mode.status_code == 400
    assert invalid_output_mode.json()["error"] == "Invalid output mode"
