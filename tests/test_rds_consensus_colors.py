"""Small synthetic display tests: no detector runs or research-data writes."""
import json
import sqlite3
from contextlib import closing
import struct

import numpy as np
import pandas as pd
import pytest

from examples.movement import rds_index as index


def test_consensus_fields_have_unique_neutral_labels():
    fields = index.RDS_OPTIONAL_COLOR_FIELDS
    assert len(fields) == len({f["key"] for f in fields})
    by_key = {f["key"]: f for f in fields}
    for suffix in ("class_aware", "weighted_evidence"):
        assert by_key[f"is_outlier_{suffix}"]["label"] == f"is_outlier ({suffix.replace('_', ' ')})"
        for method in ("bridge", "prob", "speed", "detour"):
            assert by_key[f"loglr_{method}_{suffix}"]["kind"] == "numeric"
            assert by_key[f"flagged_by_{method}_{suffix}"]["kind"] == "boolean"
    assert not any("post-hoc" in f["label"] for f in fields)


def test_consensus_values_survive_index_and_missing_scores_are_hidden(tmp_path, monkeypatch):
    source = tmp_path / "1_2.rds"
    source.write_bytes(b"synthetic fixture; reader is mocked")
    frame = pd.DataFrame({
        "x_": [1., 1.1, 1.2], "y_": [2., 2.1, 2.2],
        "t_": [0., 3600., 7200.], "timestamp": [0., 3600., 7200.],
        "geometry": [[1., 2.], [1.1, 2.1], [1.2, 2.2]],
        "study_id": [1]*3, "individual_id": [2]*3,
        "individual_local_identifier": ["example"]*3, "burst_": [1]*3,
        "is_outlier": [False, True, False],
        "is_outlier_class_aware": [False, False, True],
        "is_outlier_weighted_evidence": [True, True, False],
        "combined_evidence_weighted_evidence": [0.5, 2., -1.],
        "combined_evidence_class_aware": [np.nan]*3,
    })
    for method in ("bridge", "prob", "speed", "detour"):
        frame[f"loglr_{method}_weighted_evidence"] = [0.5, -2., np.nan]
        frame[f"loglr_{method}_class_aware"] = np.nan
        frame[f"flagged_by_{method}_class_aware"] = [False, False, True]
    frame.attrs.update({"class": ["sf", "move2"], "sf_column": "geometry",
                        "time_column": "t_", "track_id_column": "individual_local_identifier",
                        "geometry_crs": {"input": "EPSG:4326"}})
    monkeypatch.setattr(index, "read_movement_rds", lambda path: frame.copy())
    bundle = index.RdsBundle(tmp_path, "test", ({"logical_name": source.name},), (source,), "test-signature")
    output = tmp_path / "index.sqlite"
    index.build_rds_index(bundle, output)
    payload = index.build_rds_binary_columns(output, bundle_signature=bundle.signature)
    assert payload[:4] == b"VCM1"
    header_length = struct.unpack("<I", payload[4:8])[0]
    header = json.loads(payload[8:8 + header_length])
    columns = header["color_columns"]
    assert header["row_count"] == 3
    assert columns["combined_evidence_weighted_evidence"]["kind"] == "numeric"
    assert columns["is_outlier_class_aware"]["kind"] == "boolean"
    assert columns["is_outlier_weighted_evidence"]["kind"] == "boolean"
    assert "combined_evidence_class_aware" not in columns
    for method in ("bridge", "prob", "speed", "detour"):
        assert columns[f"loglr_{method}_weighted_evidence"]["kind"] == "numeric"
    assert "combined_evidence_weighted_evidence" in header["color_stats"]
    with closing(sqlite3.connect(output)) as connection, connection:
        fields = {f["key"] for f in index._available_rds_color_fields(connection)}
        assert "combined_evidence_weighted_evidence" in fields
        assert "combined_evidence_class_aware" not in fields
        for method in ("bridge", "prob", "speed", "detour"):
            assert f"loglr_{method}_weighted_evidence" in fields
            assert f"loglr_{method}_class_aware" not in fields
        assert connection.execute('SELECT combined_evidence_weighted_evidence FROM fixes ORDER BY ordinal').fetchall() == [(0.5,), (2.,), (-1.,)]
        assert connection.execute('SELECT is_outlier_class_aware, is_outlier_weighted_evidence FROM fixes ORDER BY ordinal').fetchall() == [(0, 1), (0, 1), (1, 0)]
        connection.execute("UPDATE meta SET value='5' WHERE key='schema_version'")
    assert not index._index_matches(output, bundle.signature)


@pytest.fixture
def detector_filter_index(tmp_path, monkeypatch):
    paths = [tmp_path / f"1_{individual_id}.rds" for individual_id in (2, 3)]
    for path in paths:
        path.write_bytes(b"synthetic fixture; reader is mocked")

    def read_fixture(path):
        individual_id = int(path.stem.split("_")[1])
        frame = pd.DataFrame({
            "x_": [1. + i * .01 for i in range(8)], "y_": [2.] * 8,
            "t_": [i * 3600. for i in range(8)],
            "timestamp": [i * 3600. for i in range(8)],
            "geometry": [[1. + i * .01, 2.] for i in range(8)],
            "study_id": [1] * 8, "individual_id": [individual_id] * 8,
            "individual_local_identifier": [f"animal-{individual_id}"] * 8,
            "burst_": [1] * 8, "is_outlier": [False] * 8,
            "error_class": ["detour", "block", "speed_cap", None, "", "Missing", "quoted'category", "detour"],
            "error_class_class_aware": ["block", "detour", None, None, None, None, None, None],
            "flagged_by_detour": [True, False, None, False, False, False, False, True],
            "combined_evidence": [2., -1., None, 0., 1., None, -3., 4.],
        })
        frame.attrs.update({"class": ["sf", "move2"], "sf_column": "geometry",
                            "time_column": "t_", "track_id_column": "individual_local_identifier",
                            "geometry_crs": {"input": "EPSG:4326"}})
        return frame

    monkeypatch.setattr(index, "read_movement_rds", read_fixture)
    bundle = index.RdsBundle(tmp_path, "test", tuple({"logical_name": p.name} for p in paths),
                             tuple(paths), "detector-filters")
    output = tmp_path / "index.sqlite"
    index.build_rds_index(bundle, output)
    return output


@pytest.mark.parametrize("spec,expected", [
    ({"field_key": "error_class", "field_kind": "categorical", "selected_levels": ["detour"]}, [[1, 1], [8, 8]]),
    ({"field_key": "error_class", "field_kind": "categorical", "selected_levels": ["detour", "block"]}, [[1, 2], [8, 8]]),
    ({"field_key": "error_class", "field_kind": "categorical", "selected_levels": ["Missing"]}, [[4, 6]]),
    ({"field_key": "error_class", "field_kind": "categorical", "selected_levels": ["quoted'category"]}, [[7, 7]]),
    ({"field_key": "error_class", "field_kind": "categorical", "selected_levels": ["absent"]}, []),
    ({"field_key": "error_class_class_aware", "field_kind": "categorical", "selected_levels": ["detour"]}, [[2, 2]]),
    ({"field_key": "flagged_by_detour", "field_kind": "boolean", "selected_levels": ["True"]}, [[1, 1], [8, 8]]),
    ({"field_key": "flagged_by_detour", "field_kind": "boolean", "selected_levels": ["False"]}, [[2, 2], [4, 7]]),
    ({"field_key": "flagged_by_detour", "field_kind": "boolean", "selected_levels": ["Missing"]}, [[3, 3]]),
    ({"field_key": "combined_evidence", "field_kind": "numeric", "operator": "gt", "threshold_value": 1}, [[1, 1], [8, 8]]),
    ({"field_key": "combined_evidence", "field_kind": "numeric", "operator": "lt", "threshold_value": 0}, [[2, 2], [7, 7]]),
])
def test_detector_filters_resolve_exact_source_rows(detector_filter_index, spec, expected):
    resolved, count = index.resolve_rds_review_scope(detector_filter_index, {
        "kind": "filter", "filter": {**spec, "individuals": ["animal-2"]},
    })
    assert count == sum(end - start + 1 for start, end in expected)
    assert resolved["source_rows"] == ([{"logical_name": "1_2.rds", "row_ranges": expected}] if expected else [])


def test_category_filter_excludes_confirmed_fixes_across_individuals(detector_filter_index):
    scope = {"kind": "filter", "filter": {
        "field_key": "error_class", "field_kind": "categorical", "selected_levels": ["detour"],
    }}
    assert index.resolve_rds_review_scope(detector_filter_index, scope)[1] == 4
    confirmed = {"annotation_id": "confirmed", "status": "confirmed", "scope": {
        "kind": "fix", "source_rows": [{"logical_name": "1_2.rds", "row_ranges": [[1, 1]]}],
    }}
    resolved, count = index.resolve_rds_review_scope(detector_filter_index, scope, annotations=[confirmed])
    assert count == 3
    assert resolved["source_rows"] == [
        {"logical_name": "1_2.rds", "row_ranges": [[8, 8]]},
        {"logical_name": "1_3.rds", "row_ranges": [[1, 1], [8, 8]]},
    ]


@pytest.mark.parametrize("field,kind", [("not_a_field", "categorical"), ("error_class", "numeric")])
def test_detector_filter_rejects_unknown_fields_and_wrong_kinds(detector_filter_index, field, kind):
    with pytest.raises(ValueError, match="RDS filter field"):
        index.resolve_rds_review_scope(detector_filter_index, {"kind": "filter", "filter": {
            "field_key": field, "field_kind": kind, "selected_levels": ["detour"], "threshold_value": 0,
        }})
