from pathlib import Path
import re
import sys
import base64
import csv
import io
import json
from math import isclose

from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MOVEMENT_APP_JS = REPO_ROOT / "examples" / "movement" / "static" / "app.js"
MOVEMENT_INDEX = REPO_ROOT / "examples" / "movement" / "static" / "index.html"
OSM_LAYER_JS = REPO_ROOT / "examples" / "movement" / "static" / "osm_layer.js"

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
    resolve_filter_row_ranges,
)
from examples.movement.bursts import build_auto_bursts
from examples.movement.movement_features import STEP_FEATURE_FIELDS, compute_track_movement
import examples.movement.summary as movement_summary
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
    assert features["early"] == {"step_length_m": None, "speed_mps": None, "time_delta_s": None}
    assert features["late"]["time_delta_s"] == 3600.0
    assert isclose(features["late"]["speed_mps"], features["late"]["step_length_m"] / 3600.0, rel_tol=1e-12)
    assert not any("anomaly" in key.lower() for row in features.values() for key in row)
    assert stats["alpha"]["seen_step"] == 1


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
    (project_dir / ".vibecleaning").mkdir(parents=True)
    csv_path = write_movement_csv(project_dir / "movement.csv")

    build_movement_overview(csv_path)
    build_movement_fixes(csv_path, individual="alpha")

    assert not (project_dir / ".vibecleaning" / "cache" / "movement_summary").exists()


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
    assert "time_delta_s" not in fixes[0].get("attributes", {})
    assert fixes[1]["attributes"]["time_delta_s"] == 3600.0
    assert fixes[2]["attributes"]["time_delta_s"] == 3600.0
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
    after_exclusion = by_key["id:fix_c#row:3"]

    assert excluded["analytically_excluded"] is True
    assert "step_length_m" not in excluded.get("attributes", {})
    assert after_exclusion["attributes"]["time_delta_s"] == 7200.0
    assert isclose(
        after_exclusion["attributes"]["speed_mps"],
        after_exclusion["attributes"]["step_length_m"] / 7200.0,
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
    assert by_key["id:fix_c#row:3"]["attributes"]["time_delta_s"] == 3600.0
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
    assert by_key["id:fix_c#row:3"]["attributes"]["time_delta_s"] == 3600.0
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
    assert fixes_by_key["id:fix_1#row:2"]["attributes"]["time_delta_s"] == 3600.0
    assert "time_delta_s" not in fixes_by_key["id:fix_2#row:3"].get("attributes", {})
    assert fixes_by_key["id:fix_3#row:4"]["attributes"]["time_delta_s"] == 3600.0
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


def test_movement_frontend_includes_osm_streets_basemap_config():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert '"OSM Streets": {' in source
    assert "style: OSM_STREETS_STYLE" in source
    assert "https://tile.openstreetmap.org/{z}/{x}/{y}.png" in source
    assert 'data-role="map-attribution"' in source
    assert '<option value="OSM Streets">OSM Streets</option>' in source


def test_movement_frontend_includes_individual_search_and_resizable_side_pane():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'data-role="individual-search"' in source
    assert 'data-role="side-resize"' in source
    assert "sidePaneWidthPx" in source


def test_individual_fix_splitter_mirrors_working_side_pane_drag():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "const MIN_CHECKED_FIXES_HEIGHT_PX = 60" in source
    assert 'addEventListener("pointerdown", event => this.beginIndividualPaneResize(event))' in source
    assert "this.individualPaneResize.pointerId = event.pointerId" in source
    assert "this.refs.individualResize?.setPointerCapture?.(event.pointerId)" in source
    assert 'window.addEventListener("pointermove", this.handleIndividualPanePointerMove)' in source
    assert "event.clientY - listRect.top" in source
    assert "this.refs.individualResize?.releasePointerCapture?.(event.pointerId)" in source
    assert "--movement-checked-fixes-height" in source
    assert "bounds.available - nextHeight" in source
    assert "align-content: start;" in source
    assert "--movement-queue-list-height" in source
    assert "sheetRect.bottom - event.clientY" in source


def test_hidden_output_links_do_not_leave_an_empty_root_grid_row():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert ".movement-toolbar {\n          grid-row: 1;" in source
    assert ".movement-status {\n          grid-row: 2;" in source
    assert ".movement-output-links {\n          grid-row: 3;" in source
    assert ".movement-main {\n          grid-row: 4;" in source


def test_movement_frontend_exposes_history_lock_and_resume_controls():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    index = MOVEMENT_INDEX.read_text(encoding="utf-8")

    assert 'data-role="edit-lock-profile"' in index
    assert 'data-role="resume-history"' in index
    assert '<span class="movement-edit-lock-badge">Read-only</span>' in index
    assert "document.querySelector('[data-role=\"edit-lock-profile\"]')" in source
    assert 'data-role="resume-modal"' in source
    assert ".movement-edit-lock-message {" in source
    assert "/edit-profile?" in source
    assert "/resume`" in source
    assert "expected_current_dataset_id: this.expectedCurrentDatasetId()" in source
    assert "this.canPersistEdits()" in source
    assert "Generated outputs will be deleted." in source
    assert "Analyses, reports, exports, filtering, and visualization remain available." in source


def test_movement_frontend_includes_auto_burst_controls():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'data-role="show-bursts"' in source
    assert 'data-role="burst-gap-mode"' in source
    assert 'data-role="burst-gap-seconds"' in source
    assert 'data-role="burst-gap-quantile"' in source
    assert 'data-role="burst-gap-seconds-control"' in source
    assert 'data-role="burst-gap-quantile-control"' in source
    assert "<option value=\"quantile\">Gap quantile</option>" in source
    assert "<option value=\"manual\">Fixed time gap</option>" in source
    assert "Gap quantile (0–1)" in source
    assert "Time gap (seconds)" in source
    assert "Fallback (s)" not in source
    assert 'this.refs.burstGapQuantileControl.hidden = mode !== "quantile";' in source
    assert 'this.refs.burstGapSecondsControl.hidden = mode !== "manual";' in source
    assert 'data-role="burst-count"' in source
    assert "burst_gap_mode: this.getBurstGapMode()" in source
    assert "burst_gap_quantile: String(this.getBurstGapQuantile())" in source
    assert "formatBurstGapMetadata" in source
    assert '<option value="auto_bursts">Automatic bursts</option>' in source
    assert "movement-burst-casing" in source
    assert "movement-bursts" in source
    assert "movement-auto-burst-points" not in source
    assert "movement-auto-burst-endpoints" not in source
    assert "buildAutoBurstEndpointMarkers" not in source
    assert "burstPathColor" in source
    assert "renderBurstCountIndicator" in source
    assert "getVisibleAutoBursts({ requireOverlay: false })" in source


def test_movement_frontend_colors_inbound_steps_above_burst_casing():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    renderer = source[
        source.index("  renderLayers({ temporalOnly = false } = {}) {"):
        source.index("  isSourceOnlyFlaggedBurst(")
    ]

    assert "buildMovementTrackStepSegments(data.eligibleFixesByTrack)" in source
    assert "destinationFix" in source
    assert "this.colorForFix(item.destinationFix)" in renderer
    assert "getVisibleTrackSteps(visibleIndividuals, visibleSetNames)" in renderer
    assert "suppressedBaseTrackKeys" not in source
    assert renderer.index('id: "movement-bursts"') < renderer.index('id: "movement-paths"')
    assert "interpolateSeriesPosition" not in source
    assert 'id: "movement-cursor"' not in source


def test_movement_frontend_distinguishes_source_flags_from_review_status():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "sourceFlags:" in source
    assert "function isSourceOnlyFlaggedFix(fix)" in source
    assert "sourceFlagged: isSourceOnlyFlaggedFix(previous) || isSourceOnlyFlaggedFix(destinationFix)" in source
    assert "item.sourceFlagged ? 52 : 185" in source
    assert 'id: "movement-source-flagged-points"' not in source
    assert 'id: "movement-suspected-outline"' in source
    assert 'const showSuspectedOutlines = this.data.suspiciousState === "loaded";' in source
    assert "for (const fix of this.data.suspiciousFixes || [])" in source
    assert 'fix.review?.status !== "suspected"' in source
    assert "they remain analytically included until confirmed in Vibecleaning" in source
    assert '"source_flags",\n      "Source flags"' in source


def test_movement_frontend_excludes_confirmed_audit_fixes_from_color_scale():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    refresh = source[
        source.index("function refreshMovementFixCollections(data)")
        :source.index("function buildMovementColorFields(fields)")
    ]

    assert "computeMovementColorStyles(" in refresh
    assert "!fix.analyticallyExcluded" in refresh
    assert 'fix.review?.status !== "confirmed"' in refresh


def test_movement_frontend_uses_on_demand_individual_loading_for_truncated_overviews():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "function collectSummaryIndividuals(summary)" in source
    assert "summary.series_by_individual" in source
    assert "overviewTruncated" in source
    assert "Select individuals to load fixes on demand." in source
    assert "initialMovementVisibleIndividuals(data)" in source
    assert "initialMovementVisibleIndividuals(this.data)" in source
    assert "if (data.overviewTruncated) {" in source
    assert "getActiveThresholdMatchKeys()" in source
    assert "temporalOnly ? this.lastThresholdMatchKeys : this.getActiveThresholdMatchKeys()" in source


def test_movement_frontend_map_click_does_not_force_table_reveal():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "handleMapClick(event)" in source
    assert "this.applyTableSelectionInteraction(fixKey" in source
    assert "payloadIndividuals.length ? payloadIndividuals : [...selectedIndividuals]" in source
    assert "this.revealFixInTable" not in source
    assert "scrollTableRowIntoView" not in source
    assert 'this.refs?.sideSheetTabs?.dataset.activeSheet === "table"' in source


def test_movement_frontend_has_shared_endpoint_range_selection():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'data-role="range-select-mode"' not in source
    assert 'data-role="table-range-select-mode"' not in source
    assert 'this.map.on("dblclick", event => this.handleMapDoubleClick(event));' in source
    assert "applyMapRangeEndpoint(fixKey)" in source
    assert "this.map.doubleClickZoom?.disable?.();" in source
    assert "eligibleTrackPositionByFixKey" in source
    assert "track.slice(startIndex, endIndex + 1)" in source
    assert "contiguousRange: true" in source
    assert 'event.key === "Escape"' in source
    assert 'id: "movement-table-selection-endpoints"' not in source
    assert "range: event.shiftKey" in source


def test_movement_frontend_uses_one_scope_aware_flag_action():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert source.count('data-role="mark-suspected"') == 2  # markup and ref
    assert 'data-role="segment-suspected"' not in source
    assert "openActiveFlagModal()" in source
    assert 'kind: "segment"' in source
    assert 'kind: "filter"' in source
    assert 'kind: "fixes"' in source
    assert "Flag selected segment" in source
    assert "Flag threshold matches" in source
    assert "Flag checked fixes" in source
    assert 'this.flagTargetKind = "filter";' in source
    check_start = source.index("  checkAboveThresholdSelection() {")
    check_end = source.index("\n  getFixesForIndividualsFrom(", check_start)
    assert 'this.flagTargetKind = "fixes";' in source[check_start:check_end]
    assert "The main flag action applies the selected-level filter directly." in source
    assert "Add matches to checked fixes" in source
    assert 'selectionMethod: "map_double_click"' in source
    assert 'selectionMethod: "table_shift_click"' in source
    assert "selection_method: context.selectionMethod" in source


def test_movement_frontend_precomputes_inbound_flagged_steps():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "function buildFlaggedStepOverlays(data)" in source
    assert "data.flaggedStepOverlays = buildFlaggedStepOverlays(data);" in source
    assert "position.index <= 0" in source
    assert "path: [previous.position, fix.position]" in source
    assert 'id: "movement-flagged-fix-steps"' in source
    assert "data: visibleFlaggedSteps" in source
    assert 'item.status === "confirmed"' in source
    assert 'effectiveIssues.some(issue => issue.scopeKind !== "segment")' in source


def test_movement_frontend_uses_cached_binary_searched_temporal_focus():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    renderer = source[
        source.index("  renderLayers({ temporalOnly = false } = {}) {"):
        source.index("  isSourceOnlyFlaggedBurst(")
    ]

    assert "scheduleTemporalFocusRender()" in source
    assert "window.requestAnimationFrame" in source
    assert "function nearestTrackFixIndex(fixes, currentTimeMs)" in source
    assert "while (low < high)" in source
    assert "visiblePointCache" in source
    assert "getVisibleMovementPoints(visibleIndividuals, visibleSetNames)" in renderer
    assert "showPoints && this.temporalSliderEngaged" in renderer
    assert "buildTemporalFocalData(visibleIndividuals, visibleSetNames)" in renderer
    assert 'id: "movement-temporal-focal-steps"' not in renderer
    assert 'id: "movement-temporal-focal-halos"' in renderer
    assert 'id: "movement-temporal-focal-points"' in renderer
    assert 'this.refs.slider.addEventListener("pointerdown"' in source
    assert 'this.refs.slider.addEventListener("pointerup"' in source
    assert "this.colorForFix(item.fix)" in renderer
    assert "Math.max(0, focusIndex - 1)" in source
    assert "Math.min(fixes.length - 1, focusIndex + 1)" in source
    assert "for (const fix of this.data.fixes)" not in renderer
    assert "temporalOnly ? this.lastThresholdMatchKeys" in renderer


def test_movement_frontend_loads_ephemeral_osm_helpers_only_in_dev_mode():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    helper = OSM_LAYER_JS.read_text(encoding="utf-8")

    assert 'MOVEMENT_APP_MODE === "movement"' in source
    assert '? await import("/static/osm_layer.js")' in source
    assert 'from "/static/osm_layer.js"' not in source
    assert "this.osmContext = null" in source
    assert 'this.osmContextStatus = "idle"' in source
    assert "queryOsmContext(query" in source
    assert 'this.setStatus("Loading OSM context...")' in source
    assert "OSM context failed:" in source
    assert "formatOsmContextStatus(payload)" in source
    assert "getOsmContextMetadata()" in source
    assert "clearOsmContext({ render = true, announce = false }" in source
    assert "this.clearOsmContext({ render: false })" in source
    assert "layers.push(...this.getOsmDeckLayers())" in source
    assert "new deckInstance.GeoJsonLayer" in helper
    assert 'fetch("/api/osm/features"' in helper
    assert "pickable: false" in helper
    assert "scopeFromPoint" in helper
    assert "scopeFromMapBounds" in helper
    assert "scopeFromSegmentBounds" in helper


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
    assert ranges == [[1, 1], [3, 3]]


def test_threshold_issue_ui_sends_a_full_dataset_filter_scope():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    assert 'kind: "filter"' in source
    assert '"all matching fixes in the full dataset"' in source


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
            f"{base_url}/actions/review-individuals",
            json={
                "dataset_id": root_id,
                "expected_current_dataset_id": confirmed_dataset_id,
                "logical_name": "movement.csv",
                "decisions": [{"individual": "alpha", "review_ok": True}],
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
    assert rewound_profile["editable"] is False
    assert rewound_profile["blockers"][0]["code"] == "forward_history_pending"

    blocked_rewound_edit = client.post(
        f"{base_url}/actions/review-individuals",
        json={
            "dataset_id": suspected_dataset_id,
            "expected_current_dataset_id": suspected_dataset_id,
            "logical_name": "movement.csv",
            "decisions": [{"individual": "alpha", "review_ok": True}],
            "user": "reviewer",
        },
    )
    assert blocked_rewound_edit.status_code == 423
    assert (
        blocked_rewound_edit.json()["edit_profile"]["blockers"][0]["code"]
        == "forward_history_pending"
    )

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
    assert root_profile["resume"]["discard_dataset_count"] == 2
    assert root_profile["resume"]["discard_step_count"] == 2

    resumed = client.post(
        f"{base_url}/resume",
        json={
            "dataset_id": root_id,
            "expected_current_dataset_id": root_id,
            "resume_token": root_profile["resume"]["token"],
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
                "target_artifact": "movement.csv",
                "burst_gap_mode": "manual",
                "burst_gap_seconds": 60,
                "burst_gap_quantile": 0.75,
                "feature_set": feature_set,
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


def test_movement_frontend_restores_saved_burst_analyses():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "restoreSavedAnalyses" in source
    assert "loadSavedAnomalyRanking" in source
    assert 'params.set("burst_gap_effective_seconds"' in source
    assert "latest_compatible_by_action" in source
    assert "burst_anomaly_ranking.json" in source
    assert 'status: "checking"' in source
    assert 'status: "available"' in source
    assert 'status: "restoring"' in source
    assert "const loadedRanking = this.hasCompatibleIndividualQueueRanking()" in source
    assert "rankingAnalysisId === loadedRankingAnalysisId" in source
    assert "createdAt: String(ranking.created_at || loadedRanking.createdAt || \"\")" in source
    assert "this.anomalyRanking = loadedRanking || {" in source
    assert "Checking for a compatible saved burst ranking" in source
    assert "Loading saved burst ranking" in source
    assert 'data-action="load-saved-ranking"' in source
    assert "burst_feature_space.json" in source
    assert 'result.loadedFromHistory ? "Restored" : "Created"' in source
    assert "exportReviewedCsv" in source
    assert "Download ${escapeHtml(outputName)}" in source
    assert "/actions/annotate-scope" in source
    assert "openIndividualReviewModal" in source
    assert "openBurstReviewModal" in source
    assert 'candidateGenerated ? "algorithm" : "manual"' in source
    assert "--movement-individual-list-height" in source
    assert 'data-role="individual-resize"' in source
    assert ".movement-side-sheet.ranking" in source
    assert 'data-role="confirm-modal"' in source
    assert "/actions/confirm-issues" in source
    assert 'id: "movement-confirmed-exclusions"' in source
    assert 'id: "movement-suspected-outline"' in source
    assert "[92, 101, 110, 24]" in source
    assert "getFillColor: item => this.queueMapColor(" in source
    assert "|| !visibleIndividuals.has(fix.individual)" in source
    assert "getUnresolvedSuspectedIssueGroups" in source


def test_movement_frontend_loads_suspicious_fixes_as_passive_overlay_with_focus_action():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'data-role="select-suspicious">Review suspicious fixes</button>' in source
    assert "async loadSuspiciousFixes({ focus = true } = {})" in source
    assert "loadSuspiciousFixes({ focus: false })" in source
    assert 'this.cancelRequest("detail");' in source
    assert "if (focus)" in source
    assert "Review suspicious fixes" in source
    assert 'reviewStatus: "suspected"' in source
    assert "this.data.suspiciousFixes = suspiciousFixes" in source
    assert "this.data.selectedIndividuals = new Set(suspiciousFixes.map" in source
    assert "this.zoomToPath(suspiciousFixes.map" in source
    assert "...(data.suspiciousFixes || [])" in source


def test_movement_frontend_limits_suspicious_halos_to_visible_individuals():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    collection_start = source.index("    if (showSuspectedOutlines) {")
    collection_end = source.index("\n    if (showPoints) {", collection_start)
    collection = source[collection_start:collection_end]

    assert "for (const fix of this.data.suspiciousFixes || [])" in collection
    assert "|| !visibleIndividuals.has(fix.individual)" in collection
    assert "|| !visibleSetNames.has(fix.setName)" in collection


def test_movement_report_can_render_one_snapshot_per_flagged_burst():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'data-role="report-snapshot-unit"' in source
    assert '<option value="burst">One per flagged burst</option>' in source
    assert '<option value="context">Merge nearby flagged fixes</option>' in source
    assert 'this.refs.reportSnapshotUnit.value = "burst"' in source
    assert "buildBurstReportSnapshotWindows(reportFixes)" in source
    assert 'issue.scopeKind !== "burst"' in source
    assert "issue.scopeBurstId || \"\"" in source
    assert "Math.min(...indices) - 8" in source
    assert "Math.max(...indices) + 8" in source
    assert 'snapshotKind: "burst"' in source
    assert "burst_id: window.burstId || \"\"" in source


def test_movement_frontend_preserves_view_context_across_dataset_nodes():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "captureDatasetViewContext()" in source
    assert "initializeDatasetView(viewContext = null)" in source
    assert "restoreDatasetMapView(viewContext = null)" in source
    assert "selectedIndividuals: this.getSelectedIndividuals()" in source
    assert "selectedFixKeys: new Set(this.data.selectedFixKeys)" in source
    assert "currentTimeMs: this.currentTimeMs" in source
    assert "mapView," in source
    assert "async loadDataset(viewContext = this.captureDatasetViewContext())" in source
    assert "const preservedFixKeys = this.initializeDatasetView(viewContext)" in source
    assert "this.restoreDatasetMapView(viewContext)" in source
    assert "await this.loadStudy({ preferredDatasetId: datasetId, viewContext })" in source


def test_movement_frontend_preserves_queue_context_across_annotation_steps():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "captureAnnotationReloadContext()" in source
    assert "restoreAnnotationReloadContext(viewContext = null)" in source
    assert "anomalyRanking: this.hasCompatibleIndividualQueueRanking()" in source
    assert "orderMode: queue.orderMode" in source
    assert "activeIndividual: queue.activeIndividual" in source
    assert "mapScope: queue.mapScope" in source
    assert "appliedRankingAnalysisId: queue.appliedRankingAnalysisId" in source
    assert "this.restoreAnnotationReloadContext(viewContext)" in source
    assert "{ preserveAnnotationContext: true }" in source


def test_movement_frontend_exposes_lightweight_individual_review_queue():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "const INDIVIDUAL_QUEUE_PAGE_SIZE = 25" in source
    assert "const INDIVIDUAL_QUEUE_GROUP_SIZE = 5" in source
    assert 'mode: "browse",' in source
    assert 'mode: this.uiState.individualViewMode === "queue" ? "queue" : "browse"' not in source
    assert 'data-role="individual-view-browse">Browse all</button>' in source
    assert 'data-role="individual-view-queue">Review queue</button>' in source
    assert source.index('data-role="individual-view-browse"') < source.index('data-role="side-sheet-tabs"')
    assert 'data-queue-scope="solo">Only current</button>' in source
    assert 'data-queue-scope="group">Group view</button>' in source
    assert 'data-queue-scope="all"' not in source
    assert '.movement-side-tabs.hidden,' in source
    assert 'data-role="individual-search-control"' in source
    assert "this.refs.individualSearchControl.hidden = this.individualReviewQueue.mode === \"queue\"" in source
    assert "const individuals = this.data?.individuals || [];" in source
    assert ".movement-side-sheet.individuals.queue-mode .movement-individual-resize" in source
    assert '.movement-side-sheet.individuals.queue-mode [data-role="individuals"]' in source
    assert '.movement-side-sheet.individuals.queue-mode [data-role="selected-fixes"]' in source
    assert "display: none;" in source
    assert 'data-queue-comment data-individual=' in source
    assert 'data-queue-table data-individual=' in source
    assert 'data-role="individual-queue-comment"' not in source
    assert "getIndividualQueueMapIndividuals()" in source
    assert "return position.group;" in source
    assert "/actions/review-individuals" in source
    assert "flushIndividualReviewDecisions()" in source
    assert 'id: "movement-active-individual-outline"' not in source
    assert "return 0.25;" in source
    assert "this.queueMapColor(" in source
    assert 'data-role="issue-scope"' in source
    assert 'data-role="issue-burst-list"' in source
    assert "setupIndividualQueueIssueScope(individual)" in source
    assert "this.openIndividualReviewModal(addIssueButton.dataset.individual" in source
    assert 'data-add-individual-issue data-individual=' in source
    assert "await this.stageIndividualReviewDecision(queueReviewIndividual, false)" in source
    assert "prior_decisions_by_individual" in source
    assert "movement-prior-decision-badge" in source
    assert "leftReviewGroup" not in source
    assert 'data-review-decision="second_opinion"' in source


def test_movement_frontend_has_lazy_editor_review_dashboard():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    index = MOVEMENT_INDEX.read_text(encoding="utf-8")

    assert 'data-role="admin-dashboard" hidden>Review dashboard</button>' in index
    assert 'data-role="admin-dashboard-modal"' in source
    assert '"/api/apps/movement/admin/review-summary"' in source
    assert 'include_individuals: "true"' in source
    assert "openAdminDashboard()" in source
    assert "handleAdminDashboardClick(event)" in source
    assert "setInterval" not in source[source.index("  async openAdminDashboard() {"):source.index("  async assignCurrentReview() {")]


def test_movement_individual_queue_explains_burst_ranking_availability():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "No compatible burst ranking is available for this dataset version and burst definition." in source
    assert "Compatible saved ranking available" in source
    assert 'data-queue-action="load-ranking"' in source
    assert "hasAvailableIndividualQueueRanking()" in source
    assert "Burst ranking in progress. Dataset order will remain in use." in source
    assert "Apply completed ranking" in source
    assert 'data-queue-action="run-ranking"' in source
    assert "noteCompletedIndividualQueueRanking()" in source
    assert "queue.pendingRankingAnalysisId = this.anomalyRanking.analysisId" in source


def test_movement_issue_dialog_has_compact_burst_preview():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'data-role="issue-burst-preview"' in source
    assert 'data-role="issue-burst-preview-list"' in source
    assert "buildIssueBurstPreviewModel(burst)" in source
    assert "renderIssueBurstPreviews(selectedBursts)" in source
    assert "initialBurstId: burstId" in source
    assert "this.hideIssueBurstPreview()" in source
    assert "item.individual === selected.individual" in source
    assert "item.setName === selected.setName" in source
    assert "samplePreviewPath(previous.path, 80)" in source
    assert "samplePreviewPath(next.path, 80)" in source
    assert "this.colorForFix(fix)" in source
    assert "movementPathDistanceMeters(positions)" in source
    assert "movementMedianStepMeters(positions)" in source
    assert "`median step ${formatCompactDistance(model.medianStepMeters)}`" in source
    assert "gap before ${formatCompactDuration(model.gapBeforeSeconds)}" in source
    assert "gap after ${formatCompactDuration(model.gapAfterSeconds)}" in source
    assert "Stationary or overlapping fixes" in source
    assert "Dashed gray: adjacent bursts" in source
    assert "previewScaleBarSvg(geometry)" in source
    assert "metersPerPixel: (111195.0802335 * spanX) / frameWidth" in source
    assert "formatPreviewScaleDistance(distanceMeters)" in source
    assert "previewContextCueSvg" not in source
    assert "Burst metadata is available, but its path is not loaded" in source
    assert '<option value="burst">By Burst</option>' in source
    assert 'data-issue-burst-id=' in source
    assert "selectedIssueBursts()" in source
    assert "setIssueBurstIncluded(burstId, included)" in source
    assert "getIssueBurstAnomalyScore(selected.burstId)" in source
    assert "scoreLabel," not in source
    assert "Highest anomaly score first" in source
    assert "loadIssueBurstScores()" in source
    assert "burst_anomaly_ranking.json" in source
    assert "background: #e9eef5;" in source
    assert 'kind: "bursts"' in source
    assert "burst_ids: context.burstIds" in source


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
                        "review_ok": True,
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
                "review_decision": "not_ok",
                "needs_check": False,
                "comment": "Issues were marked separately",
            },
        },
    )
    assert second_response.status_code == 200
    second_payload = second_response.json()
    assert second_payload["dataset"]["parent_dataset_id"] == reviewed_dataset_id
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
    assert reviewed_stats["alpha"]["review_ok"] is True
    assert reviewed_stats["beta"]["reviewed"] is True
    assert reviewed_stats["beta"]["review_ok"] is False
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
    assert records[2]["time_delta_s"] == 7200.0
    assert records[2]["step_length_m"] is not None


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
    assert records[1]["time_delta_s"] == 3600.0
    assert records[1]["step_length_m"] is not None


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
    assert "<strong>Source csv:</strong> movement.csv" in html
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


def test_movement_frontend_colors_map_bursts_by_individual():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    # Burst identity on the map is individual palette plus casing boundaries,
    # not a per-burst rainbow keyed on burstIdx.
    assert "function burstPathColor(individualPalette, burst, alpha = 200)" in source
    assert "color: burstPathColor(data.individualPalette, burst, 185)," in source
    assert "function autoBurstColor(" not in source
    assert "burstIdx * 47" not in source


def test_movement_frontend_draws_bursts_with_a_shared_casing_layer():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'id: "movement-burst-casing"' in source
    assert 'id: "movement-bursts"' in source

    # Both layers must receive the same identity-stable array and comparator so
    # deck.gl skips re-tesselation on unrelated renders.
    casing_start = source.index('id: "movement-burst-casing"')
    fill_start = source.index('id: "movement-bursts"')
    casing_block = source[casing_start:fill_start]
    fill_block = source[fill_start:fill_start + 800]
    for block in (casing_block, fill_block):
        assert "data: visibleAutoBurstPaths," in block
        assert "dataComparator: sameArrayItems," in block

    # No point-count threshold may gate the casing.
    assert "BURST_CASING_MAX_POINTS" not in source


def test_movement_frontend_expresses_burst_focus_as_a_style_predicate():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    # The parallel focused-burst layer stack is gone.
    assert "movement-focused-ranking-burst-path-outline" not in source
    assert "movement-focused-ranking-burst-path" not in source
    assert "movement-focused-ranking-burst-points" not in source
    assert "movement-focused-ranking-burst-markers" not in source

    # Focus is an accessor branch refreshed by updateTriggers instead.
    assert "isFocusedBurstItem(item, focusedBurstId)" in source
    assert "burstCasingColor(item, focusedBurstId)" in source
    assert "burstFillColor(item, focusedBurstId)" in source
    assert "burstEndpointColor(item, focusedBurstId)" not in source
    # The casing layer must refresh on focus change; it also carries the queue
    # dimming key, so assert the trigger contains focusedBurstId rather than
    # matching an exact literal.
    casing = source[source.index('id: "movement-burst-casing"'):]
    casing = casing[:casing.index("}),")]
    assert "updateTriggers" in casing
    assert "focusedBurstId" in casing[casing.index("updateTriggers"):]

    # Fix-level emphasis is a stroked ring that never recolors fixes.
    assert 'id: "movement-burst-focus-ring"' in source
    ring_start = source.index('id: "movement-burst-focus-ring"')
    ring_block = source[ring_start:ring_start + 400]
    assert "filled: false," in ring_block
    assert "getFillColor" not in ring_block


def test_movement_frontend_keeps_source_flag_distinction_under_focus():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    # The focus branch must not short-circuit the source-flag branch; flagged
    # context bursts stay dimmer than clean context bursts.
    assert "this.mutedRankingContextColor(item.color, item?.sourceFlagged ? 22 : 36)" in source
    assert "item?.sourceFlagged ? 40 : 70" in source


def test_movement_frontend_never_suppresses_tracks_for_burst_overlays():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    # Bursts are an optional casing below the variable-colored track. They
    # must never erase the base track, including when a burst has one fix.
    assert (
        "const drawableAutoBursts = visibleAutoBursts.filter(burst => burst.path.length >= 2);"
        in source
    )
    assert "suppressedBaseTrackKeys" not in source


def test_movement_frontend_burst_picking_does_not_depend_on_feature_space():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    # Bursts are pickable regardless of whether a feature-space analysis ran.
    assert "pickable: Boolean(this.burstFeatureSpace?.points?.length)" not in source
    assert "focusMapBurst(burstId)" in source
    click_start = source.index("handleMapClick(event) {")
    click_block = source[click_start:click_start + 1600]
    assert "focusMapBurst" not in click_block
    assert "setBurstVisible" not in click_block

    # Feature-space selection keeps its own independent guards and still runs
    # first in the click handler.
    assert 'this.refs?.sideSheetTabs?.dataset.activeSheet !== "feature_space"' in source
    click_start = source.index("handleMapClick(event) {")
    click_block = source[click_start:click_start + 1600]
    assert click_block.index("getMapPickedFeatureSpaceBurst") < click_block.index("pickObject")
    assert click_block.index("getMapPickedFeatureSpaceBurst") < click_block.index("focusMapBurst")


def test_movement_frontend_burst_counter_counts_only_drawn_bursts():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    counter_start = source.index("renderBurstCountIndicator(message = \"\") {")
    counter_block = source[counter_start:counter_start + 900]
    assert "this.getVisibleAutoBursts({ requireOverlay: true }).length" in counter_block


def test_movement_frontend_refreshes_queue_dimming_when_active_individual_changes():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    renderer = source[
        source.index("  renderLayers({ temporalOnly = false } = {}) {"):
        source.index("  isSourceOnlyFlaggedBurst(")
    ]

    # Queue dimming depends on state outside the data array, so every layer
    # whose data is identity-stable must declare it as an update trigger.
    # Without this, advancing the review queue leaves stale colors on screen.
    assert 'this.individualReviewQueue.mode === "queue"' in renderer
    assert "queue:${this.queueActiveIndividual()}" in renderer

    # The map must not read the raw active-individual field, which can be empty
    # when a render lands before the queue list has re-resolved it. An empty
    # value draws the whole review batch at full opacity.
    resolver = source[source.index("  queueActiveIndividual() {"):]
    resolver = resolver[:resolver.index("\n  queueMapOpacity(")]
    assert "this.getIndividualQueuePosition();" in resolver
    opacity = source[source.index("  queueMapOpacity(individual) {"):]
    opacity = opacity[:opacity.index("\n  }")]
    assert "this.queueActiveIndividual()" in opacity
    assert "this.individualReviewQueue.activeIndividual" not in opacity

    layer_ids = re.findall(r'id: "([^"]+)"', renderer)
    dimming_calls = (
        "queueMapColor",
        "burstFillColor",
        "burstCasingColor",
    )
    frozen = []
    for match in re.finditer(r'id: "([^"]+)"', renderer):
        block = renderer[match.start():match.start() + 1600]
        end = block.find("}),")
        block = block[:end] if end > 0 else block
        dims = any(call in block for call in dimming_calls)
        identity_stable = "dataComparator" in block
        if dims and identity_stable and "queueDimKey" not in block:
            frozen.append(match.group(1))

    assert not frozen, f"layers dim but never refresh their dimming: {frozen}"
    # Guard against the audit silently passing on an empty layer list.
    assert "movement-paths" in layer_ids
    assert "movement-bursts" in layer_ids
