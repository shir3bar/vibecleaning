from pathlib import Path
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from examples.movement.burst_features import build_burst_feature_rows
from examples.movement.movement_features import haversine_meters


def _fix(
    fix_key: str,
    time_ms: int,
    lon: float,
    *,
    attributes: dict | None = None,
    individual: str = "alpha",
) -> dict:
    fix = {
        "fix_key": fix_key,
        "individual": individual,
        "time_ms": time_ms,
        "lon": lon,
        "lat": 0.0,
    }
    if attributes is not None:
        fix["attributes"] = attributes
    return fix


def _burst(
    burst_id: str,
    fix_keys: list[str],
    start_time_ms: int,
    end_time_ms: int,
    *,
    burst_idx: int = 0,
    individual: str = "alpha",
    set_name: str = "train",
) -> dict:
    return {
        "burst_id": burst_id,
        "burst_idx": burst_idx,
        "individual": individual,
        "set_name": set_name,
        "start_time_ms": start_time_ms,
        "end_time_ms": end_time_ms,
        "fix_keys": fix_keys,
    }


def test_build_burst_feature_rows_are_ordered_and_summarize_movement():
    step_m = haversine_meters(0.0, 0.0, 0.001, 0.0)
    fixes = [
        _fix("fix_0", 0, 0.0),
        _fix(
            "fix_1",
            10_000,
            0.001,
            attributes={"step_length_m": step_m, "speed_mps": step_m / 10.0, "time_delta_s": 10.0},
        ),
        _fix(
            "fix_2",
            20_000,
            0.002,
            attributes={"step_length_m": step_m, "speed_mps": step_m / 10.0, "time_delta_s": 10.0},
        ),
        _fix("fix_late", 30_000, 0.003),
    ]
    bursts = [
        _burst("alpha:train:burst_000001", ["fix_late"], 30_000, 30_000, burst_idx=1),
        _burst("alpha:train:burst_000000", ["fix_0", "fix_1", "fix_2"], 0, 20_000),
    ]

    rows = build_burst_feature_rows(fixes, bursts)

    assert [row["burst_id"] for row in rows] == [
        "alpha:train:burst_000000",
        "alpha:train:burst_000001",
    ]
    row = rows[0]
    assert row["individual"] == "alpha"
    assert row["set_name"] == "train"
    assert row["start_time_ms"] == 0
    assert row["end_time_ms"] == 20_000
    assert row["n_fixes"] == 3
    assert row["fix_keys"] == ["fix_0", "fix_1", "fix_2"]
    assert row["duration_s"] == 20.0
    assert row["path_length_m"] == pytest.approx(2.0 * step_m)
    assert row["mean_step_length_m"] == pytest.approx(step_m)
    assert row["sd_step_length_m"] == pytest.approx(0.0, abs=1e-9)
    assert row["net_displacement_m"] == pytest.approx(2.0 * step_m)
    assert row["straightness"] == pytest.approx(1.0)
    assert row["mean_speed_mps"] == pytest.approx(step_m / 10.0)
    assert row["median_speed_mps"] == pytest.approx(step_m / 10.0)
    assert row["max_speed_mps"] == pytest.approx(step_m / 10.0)
    assert row["sd_speed_mps"] == pytest.approx(0.0, abs=1e-9)
    assert row["max_time_gap_s"] == 10.0


def test_singleton_burst_has_no_transition_summaries():
    fixes = [
        _fix(
            "fix_only",
            20_000,
            0.002,
            attributes={"step_length_m": 999.0, "speed_mps": 99.0, "time_delta_s": 10.0},
        )
    ]

    row = build_burst_feature_rows(
        fixes,
        [_burst("alpha:train:burst_000001", ["fix_only"], 20_000, 20_000, burst_idx=1)],
    )[0]

    assert row["n_fixes"] == 1
    assert row["duration_s"] == 0.0
    assert row["path_length_m"] == 0.0
    assert row["net_displacement_m"] == 0.0
    assert row["mean_step_length_m"] is None
    assert row["sd_step_length_m"] is None
    assert row["mean_speed_mps"] is None
    assert row["median_speed_mps"] is None
    assert row["max_speed_mps"] is None
    assert row["sd_speed_mps"] is None
    assert row["max_time_gap_s"] is None
    assert row["straightness"] is None


def test_transition_on_first_fix_of_later_burst_is_excluded():
    fixes = [
        _fix("fix_0", 0, 0.0),
        _fix(
            "fix_1",
            10_000,
            0.001,
            attributes={"step_length_m": 10.0, "speed_mps": 1.0, "time_delta_s": 10.0},
        ),
        _fix(
            "fix_2",
            100_000,
            0.010,
            attributes={"step_length_m": 900.0, "speed_mps": 10.0, "time_delta_s": 90.0},
        ),
        _fix(
            "fix_3",
            105_000,
            0.011,
            attributes={"step_length_m": 7.0, "speed_mps": 1.4, "time_delta_s": 5.0},
        ),
    ]
    bursts = [
        _burst("alpha:train:burst_000000", ["fix_0", "fix_1"], 0, 10_000),
        _burst("alpha:train:burst_000001", ["fix_2", "fix_3"], 100_000, 105_000, burst_idx=1),
    ]

    later_row = build_burst_feature_rows(fixes, bursts)[1]

    assert later_row["path_length_m"] == 7.0
    assert later_row["mean_step_length_m"] == 7.0
    assert later_row["max_speed_mps"] == 1.4
    assert later_row["max_time_gap_s"] == 5.0


def test_raw_numeric_source_fields_are_summarized_without_anomaly_fields():
    fixes = [
        _fix(
            "fix_0",
            0,
            0.0,
            attributes={
                "gps:hdop": "1",
                "height-above-msl": 100,
                "gps:quality": "bad",
                "gps:valid": True,
            },
        ),
        _fix(
            "fix_1",
            10_000,
            0.001,
            attributes={
                "step_length_m": 5.0,
                "speed_mps": 0.5,
                "time_delta_s": 10.0,
                "gps:hdop": 3,
                "height-above-msl": "104",
                "gps:quality": "poor",
                "gps:valid": False,
            },
        ),
    ]

    row = build_burst_feature_rows(
        fixes,
        [_burst("alpha:train:burst_000000", ["fix_0", "fix_1"], 0, 10_000)],
    )[0]

    assert row["gps:hdop__mean"] == 2.0
    assert row["gps:hdop__median"] == 2.0
    assert row["gps:hdop__max"] == 3.0
    assert row["gps:hdop__sd"] == 1.0
    assert row["height-above-msl__mean"] == 102.0
    assert row["height-above-msl__median"] == 102.0
    assert row["height-above-msl__max"] == 104.0
    assert row["height-above-msl__sd"] == 2.0
    assert not any(key.startswith("gps:quality__") for key in row)
    assert not any(key.startswith("gps:valid__") for key in row)
    assert not any(key.startswith("osm:") for key in row)
    assert not any("anomaly" in key.lower() for key in row)


def test_precomputed_osm_distance_fields_are_summarized_within_each_burst_only():
    fixes = [
        _fix(
            "fix_0",
            0,
            0.0,
            attributes={
                "osm:nearest_road_distance_m": 10.0,
                "osm:nearest_railway_distance_m": 90.0,
                "osm:nearest_road_class": "track",
                "osm:road_match_status": "matched",
            },
        ),
        _fix(
            "fix_1",
            10_000,
            0.001,
            attributes={
                "step_length_m": 5.0,
                "speed_mps": 0.5,
                "time_delta_s": 10.0,
                "osm:nearest_road_distance_m": 30.0,
                "osm:nearest_railway_distance_m": 110.0,
                "osm:nearest_road_class": "service",
                "osm:road_match_status": "matched",
            },
        ),
        _fix(
            "fix_2",
            100_000,
            0.01,
            attributes={
                "osm:nearest_road_distance_m": 999.0,
                "osm:nearest_railway_distance_m": 888.0,
                "osm:nearest_road_class": "motorway",
                "osm:road_match_status": "matched",
            },
        ),
    ]
    rows = build_burst_feature_rows(
        fixes,
        [
            _burst("alpha:train:burst_000000", ["fix_0", "fix_1"], 0, 10_000),
            _burst("alpha:train:burst_000001", ["fix_2"], 100_000, 100_000, burst_idx=1),
        ],
    )

    first = rows[0]
    assert first["osm:nearest_road_distance_m__min"] == 10.0
    assert first["osm:nearest_road_distance_m__mean"] == 20.0
    assert first["osm:nearest_road_distance_m__median"] == 20.0
    assert first["osm:nearest_road_distance_m__max"] == 30.0
    assert first["osm:nearest_road_distance_m__sd"] == 10.0
    assert first["osm:nearest_railway_distance_m__min"] == 90.0
    assert first["osm:nearest_railway_distance_m__mean"] == 100.0
    assert first["osm:nearest_railway_distance_m__max"] == 110.0
    assert not any("class__" in field or "match_status__" in field for field in first)
    second = rows[1]
    assert second["osm:nearest_road_distance_m__mean"] == 999.0
    assert second["osm:nearest_railway_distance_m__mean"] == 888.0


def test_absent_categorical_or_nonnumeric_osm_fields_do_not_create_summaries():
    row = build_burst_feature_rows(
        [
            _fix(
                "fix_0",
                0,
                0.0,
                attributes={
                    "osm:nearest_road_class": "track",
                    "osm:road_match_status": "matched",
                    "osm:nearest_road_distance_m": "unknown",
                },
            ),
            _fix(
                "fix_1",
                10_000,
                0.001,
                attributes={
                    "step_length_m": 1.0,
                    "speed_mps": 0.1,
                    "time_delta_s": 10.0,
                    "osm:nearest_road_distance_m": "",
                },
            ),
        ],
        [_burst("alpha:train:burst_000000", ["fix_0", "fix_1"], 0, 10_000)],
    )[0]

    assert not any(field.startswith("osm:") for field in row)


def test_burst_features_do_not_fetch_osm():
    source = (REPO_ROOT / "examples" / "movement" / "burst_features.py").read_text(encoding="utf-8")

    assert "fetch_osm_features" not in source
    assert "app.osm" not in source
