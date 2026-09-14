"""Small synthetic display tests: no detector runs or research-data writes."""
import json
import sqlite3
import struct

import numpy as np
import pandas as pd

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
    with sqlite3.connect(output) as connection:
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
