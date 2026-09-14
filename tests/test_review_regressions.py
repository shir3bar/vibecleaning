"""Regression coverage for the September movement review audit."""
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from examples.movement import rds_index
from examples.movement.anomaly_ranking import _empirical_percentile, score_bursts
from examples.movement.burst_features import build_burst_feature_rows
from examples.movement.movement_features import burst_movement_summary


def test_burst_membership_not_cached_step_alignment():
    fixes = [dict(fix_key=str(i), individual="bird", time_ms=t, lon=x, lat=0,
                  attributes={"step_length_m": 999999, "speed_mps": 999})
             for i, (t, x) in enumerate([(0, 0), (10000, .001), (7200000, .1)])]
    burst = dict(burst_id="a", fix_keys=["0", "1"])
    result = build_burst_feature_rows(fixes, [burst])[0]
    expected = burst_movement_summary([0, 10000], [0, .001], [0, 0])
    for key, value in expected.items():
        assert result[key] == value
    fixes[0]["review"] = {"status": "suspected"}
    assert build_burst_feature_rows(fixes, [burst])[0] == result
    fixes[0]["analytically_excluded"] = True
    assert build_burst_feature_rows(fixes, [burst])[0]["n_fixes"] == 1


@pytest.mark.parametrize("value, expected", [("FALSE", False), ("TRUE", True), ("0", False), ("1", True), (False, False), (np.bool_(True), True)])
def test_boolean_values(value, expected):
    assert rds_index._parse_rds_boolean(value) is expected


def test_boolean_invalid_and_missing():
    for value in ("nope", 2, ""):
        with pytest.raises(ValueError):
            rds_index._parse_rds_boolean(value)
    assert rds_index._parse_rds_boolean(pd.NA, allow_missing=True) is None
    with pytest.raises(ValueError):
        rds_index._parse_rds_boolean(pd.NA)


def test_geometry_crs_without_amt_attribute():
    frame = pd.DataFrame()
    frame.attrs["geometry_crs"] = {"input": np.array(["EPSG:4326"])}
    rds_index._validate_crs(frame, filename="native-sf.rds")
    frame.attrs["crs_"] = {"input": "EPSG:3857"}
    with pytest.raises(ValueError, match="consistent EPSG"):
        rds_index._validate_crs(frame, filename="conflicting.rds")


def test_cache_keeps_other_content_versions_and_reuses_annotations(tmp_path, monkeypatch):
    bundle = rds_index.RdsBundle(study_dir=tmp_path, dataset_id="v1", artifacts=(), paths=(), signature="same-source")
    path = rds_index.rds_index_path(bundle)
    path.parent.mkdir(parents=True)
    old = path.parent / "other-source.sqlite"
    old.touch()
    monkeypatch.setattr(rds_index, "load_rds_bundle", lambda *a: bundle)
    monkeypatch.setattr(rds_index, "_index_matches", lambda *a: True)
    monkeypatch.setattr(rds_index, "build_rds_index", lambda *a: pytest.fail("Annotation must not rebuild index"))
    assert rds_index.ensure_rds_index(tmp_path, "v1")[1] == rds_index.ensure_rds_index(tmp_path, "v2")[1]
    assert old.exists()


def test_percentiles_match_original_tied_rank_definition():
    values = [-2, 1, 1, 1, 5, 9]
    for value in values:
        indexes = [i for i, x in enumerate(values) if x == value]
        assert _empirical_percentile(value, values) == sum(indexes) / len(indexes) / (len(values) - 1) * 100
    assert _empirical_percentile(7, values) is None
    assert _empirical_percentile(1, [1]) == 50


def test_if_uses_single_scoring_traversal(monkeypatch):
    from sklearn.ensemble import IsolationForest
    def fail(*args, **kwargs):
        pytest.fail("Do not repeat score_samples via decision_function")
    monkeypatch.setattr(IsolationForest, "decision_function", fail)
    result = score_bursts([dict(burst_id=str(i), individual="bird", mean_speed_mps=i) for i in range(10)], {"n_estimators": 10})
    for row in result["scored_bursts"]:
        assert row["decision_function"] == -row["anomaly_score"] - result["score_offset"]


def test_reviewed_rds_can_be_reimported_and_updated(tmp_path):
    from examples.movement.rds_export import write_reviewed_rds_python, _compare_original_columns
    sources = sorted((Path(__file__).resolve().parents[1] / "data/movement_rds").glob("*.rds"), key=lambda p: p.stat().st_size)
    if not sources:
        pytest.skip("No sample RDS")
    source = sources[0]
    n = len(rds_index.read_movement_rds(source))
    columns = {key: [None] * n for key in rds_index.RDS_REVIEW_COLUMNS}
    columns["outlier_status"][0] = "suspected"
    first, second = tmp_path / "first.rds", tmp_path / "second.rds"
    write_reviewed_rds_python(source, first, columns)
    _compare_original_columns(source, first, columns)
    columns["outlier_status"][0] = "confirmed"
    write_reviewed_rds_python(first, second, columns)
    _compare_original_columns(first, second, columns)
    assert rds_index.read_movement_rds(second)["outlier_status"].iloc[0] == "confirmed"
    columns["outlier_status"][0] = "suspected"
    with pytest.raises(ValueError, match="incorrect generated"):
        _compare_original_columns(first, second, columns)


def test_polygon_holes_and_multipolygons():
    from examples.movement.osm_context import distance_to_feature_m
    polygon = {"type": "Polygon", "coordinates": [
        [[0, 0], [.01, 0], [.01, .01], [0, .01], [0, 0]],
        [[.004, .004], [.006, .004], [.006, .006], [.004, .006], [.004, .004]],
    ]}
    assert distance_to_feature_m(.002, .002, polygon) == 0
    assert distance_to_feature_m(.005, .005, polygon) > 100
    assert distance_to_feature_m(.004, .005, polygon) == pytest.approx(0, abs=1e-7)
    multi = {"type": "MultiPolygon", "coordinates": [polygon["coordinates"]]}
    assert distance_to_feature_m(.005, .005, multi) == distance_to_feature_m(.005, .005, polygon)


def test_history_rejects_old_feature_recipe_not_suspected_annotations():
    from examples.movement.analysis_history import _analysis_parameters_match, BURST_FEATURE_SIGNATURE
    args = dict(burst_gap_mode="manual", burst_gap_seconds=60, burst_gap_quantile=.75,
                feature_set="movement_only", ranking_method="isolation_forest")
    parameters = dict(args)
    compatible, reasons = _analysis_parameters_match("run_burst_anomaly_ranking", parameters, **args)
    assert not compatible and "burst feature implementation differs" in reasons
    parameters["burst_feature_signature"] = BURST_FEATURE_SIGNATURE
    assert _analysis_parameters_match("run_burst_anomaly_ranking", parameters, **args) == (True, [])


def test_csv_and_rds_use_identical_within_burst_features(tmp_path):
    sources = sorted((Path(__file__).resolve().parents[1] / "data/movement_rds").glob("*.rds"), key=lambda p: p.stat().st_size)
    if not sources:
        pytest.skip("No sample RDS")
    source = sources[0]
    bundle = rds_index.RdsBundle(study_dir=tmp_path, dataset_id="test",
        artifacts=({"logical_name": source.name},), paths=(source,), signature="test")
    path = tmp_path / "index.sqlite"
    rds_index.build_rds_index(bundle, path)
    rds_rows = rds_index.rds_burst_feature_rows(path)
    fixes = rds_index.build_rds_fixes(path)["fixes"]
    for row in rds_rows:
        members = [f for f in fixes if row["start_time_ms"] <= f["time_ms"] <= row["end_time_ms"]]
        burst = {**row, "fix_keys": [f["fix_key"] for f in members]}
        csv_row = build_burst_feature_rows(members, [burst])[0]
        for key, value in row.items():
            assert csv_row[key] == value
