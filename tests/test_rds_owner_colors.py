"""Owner markings must remain separate from detector and review decisions."""
import json
import struct

import numpy as np
import pandas as pd
import pytest

from examples.movement import rds_index as index


@pytest.fixture
def owner_index(tmp_path, monkeypatch):
    def build(*, kami_present=True, empty_algorithm=False):
        source = tmp_path / "1_2.rds"
        source.write_bytes(b"fixture decoded by the mocked RDS reader")
        frame = pd.DataFrame({
            "x_": [1., 1.1, 1.2, 1.3], "y_": [2., 2.1, 2.2, 2.3],
            "t_": [0., 3600., 7200., 10800.], "timestamp": [0., 3600., 7200., 10800.],
            "geometry": [[1., 2.], [1.1, 2.1], [1.2, 2.2], [1.3, 2.3]],
            "study_id": [1]*4, "individual_id": [2]*4,
            "individual_local_identifier": ["example"]*4, "burst_": [1]*4,
            "algorithm-marked-outlier": ["True", "False", "", None],
            "manually-marked-outlier": [False, None, True, False],
        })
        if empty_algorithm:
            frame["algorithm-marked-outlier"] = pd.NA
        if kami_present:
            frame["is_outlier"] = [False, True, False, False]
        frame.attrs.update({"class": ["sf", "move2"], "sf_column": "geometry",
                            "time_column": "t_", "track_id_column": "individual_local_identifier",
                            "geometry_crs": {"input": "EPSG:4326"}})
        monkeypatch.setattr(index, "read_movement_rds", lambda path: frame.copy())
        bundle = index.RdsBundle(tmp_path, "test", ({"logical_name": source.name},), (source,), "owner-colors")
        output = tmp_path / "index.sqlite"
        index.build_rds_index(bundle, output)
        return output
    return build


def binary_columns(path):
    payload = index.build_rds_binary_columns(path, bundle_signature="owner-colors")
    length = struct.unpack("<I", payload[4:8])[0]
    header = json.loads(payload[8:8 + length])
    start = (8 + length + 7) // 8 * 8
    arrays = {name: np.frombuffer(payload, dtype=item["dtype"], count=item["length"],
                                 offset=start + item["offset"]).tolist()
              for name, item in header["arrays"].items()}
    arrays.update({key: arrays[column["array"]] for key, column in header["color_columns"].items()})
    return header, arrays


def test_owner_columns_reach_overview_map_and_exact_filter_without_changing_kami(owner_index):
    path = owner_index()
    fields = {f["key"]: f for f in index.build_rds_overview(path)["color_fields"]}
    header, arrays = binary_columns(path)
    for name in ("algorithm-marked-outlier", "manually-marked-outlier", "is_outlier"):
        assert fields[name]["label"] == name
        assert fields[name]["kind"] == header["color_columns"][name]["kind"] == "boolean"
    assert arrays["algorithm-marked-outlier"] == [1, 0, 255, 255]
    assert arrays["manually-marked-outlier"] == [0, 255, 1, 0]
    assert arrays["is_outlier"] == [0, 1, 0, 0]
    assert arrays["review_status"] == [0, 0, 0, 0]
    fixes = index.build_rds_fixes(path)["fixes"]
    assert not any(f["analytically_excluded"] for f in fixes)
    assert fixes[0]["attributes"]["algorithm-marked-outlier"] is True
    assert fixes[2]["attributes"]["manually-marked-outlier"] is True
    for field, levels, expected in (
        ("algorithm-marked-outlier", ["True"], [[1, 1]]),
        ("manually-marked-outlier", ["True"], [[3, 3]]),
        ("is_outlier", ["True"], [[2, 2]]),
        ("algorithm-marked-outlier", ["Missing"], [[3, 4]]),
        ("manually-marked-outlier", ["False"], [[1, 1], [4, 4]]),
    ):
        scope, count = index.resolve_rds_review_scope(path, {"kind": "filter", "filter": {
            "field_key": field, "field_kind": "boolean", "selected_levels": levels,
            "individuals": ["example"],
        }})
        assert scope["source_rows"] == [{"logical_name": "1_2.rds", "row_ranges": expected}]
        assert count == sum(end - start + 1 for start, end in expected)


def test_owner_columns_work_without_inventing_kami_predictions(owner_index):
    path = owner_index(kami_present=False, empty_algorithm=True)
    fields = {f["key"] for f in index.build_rds_overview(path)["color_fields"]}
    assert {"algorithm-marked-outlier", "manually-marked-outlier"} <= fields
    assert "is_outlier" not in fields
    header, arrays = binary_columns(path)
    assert "is_outlier" not in header["color_columns"]
    assert arrays["algorithm-marked-outlier"] == [255]*4
    assert arrays["is_outlier"] == [255]*4
    assert index.source_outlier_ranking(path)["scored_bursts"] == []
