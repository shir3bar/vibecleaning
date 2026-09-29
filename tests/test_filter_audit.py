"""A filter with no matches still records a reproducible protocol step."""
import hashlib
import json

import pytest

from app.reviews import active_review, load_review_state
from app.state import get_dataset_artifact
from test_movement_fixes import create_movement_test_client
from test_rds_movement import _client


@pytest.mark.parametrize("source_format", ["csv", "rds"])
def test_zero_filters_persist_settings_without_changing_fixes(tmp_path, source_format):
    if source_format == "rds":
        client, study = _client(tmp_path)
        base = "/api/apps/movement/family/movement_rds/study/268904527"
    else:
        client, _ = create_movement_test_client(tmp_path, csv_content=(
            "eventid,individual,timestamp,longitude,latitude,is_outlier\n"
            "a1,alpha,2024-01-01T00:00:00Z,1,40,False\n"
            "a2,alpha,2024-01-01T01:00:00Z,1.01,40,False\n"
            "a3,alpha,2024-01-01T02:00:00Z,1,40,False\n"
        ))
        study = tmp_path / "data/movement_clean/test_study"
        base = "/api/apps/movement/family/movement_clean/study/test_study"
    raw_hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in study.iterdir() if p.is_file()}
    loaded = client.get(base + "/load").json()
    dataset, logical = loaded["dataset_id"], loaded["logical_name"]
    overview = client.get(base + f"/dataset/{dataset}/overview", params={"logical_name": logical}).json()
    individuals = overview["individuals"]
    filters = [
        {"field_key": "speed_mps", "field_kind": "numeric", "operator": "gt", "threshold_value": 1e20},
        {"field_key": "is_outlier", "field_kind": "boolean", "selected_levels": ["Missing"]},
        {"kind": "gps_spike", "step_length_threshold_m": 1e20, "minimum_abs_turn_angle_deg": 120},
        {"kind": "stationarity", "radius_m": 50, "minimum_duration_s": 1e20,
         "maximum_gap_s": 72*3600, "position": "anywhere"},
    ]
    assert active_review(load_review_state(study)) is None
    for index, definition in enumerate(filters):
        spec = {**definition, "individuals": individuals, "set_names": []}
        body = {"dataset_id": dataset, "logical_name": logical, "filter": spec}
        preview = client.post(base + "/actions/preview-filter", json=body)
        assert preview.status_code == 200, preview.text
        assert preview.json()["match_count"] == 0
        if index == 0:
            assert active_review(load_review_state(study)) is None  # Preview is read-only.
        profile = client.get(base + "/edit-profile", params={"dataset_id": dataset}).json()
        saved = client.post(base + "/actions/annotate-scope", json={
            **body, "scope": {"kind": "filter", "filter": spec},
            "expected_current_dataset_id": dataset, "expected_review_revision": profile.get("review_revision", 0),
            "status": "suspected", "issue_type": "Protocol filter", "comment": "Checked; no matches.", "user": "Reviewer",
        })
        assert saved.status_code == 200, saved.text
        payload = saved.json()
        assert payload["step"]["summary"]["resolved_fix_count"] == 0
        new_dataset = payload["dataset"]["dataset_id"]
        assert new_dataset != dataset
        _, sidecar = get_dataset_artifact(study, new_dataset, "movement_review_annotations.json")
        annotations = json.loads(sidecar.read_text(encoding="utf-8"))["annotations"]
        assert len(annotations) == index + 1
        annotation = annotations[-1]
        assert annotation["resolved_fix_count"] == 0
        assert annotation["source_dataset_id"] == dataset
        for key, value in spec.items():
            assert annotation["scope"]["filter"][key] == value
        assert not annotation["scope"].get("source_rows")
        assert not annotation["scope"].get("row_ranges")
        dataset = new_dataset
    if source_format == "rds":
        state = load_review_state(study)
        assert len(state["reviews"]) == 1
        assert active_review(state)["reviewer_user_id"] == profile["actor"]["user_id"]
    after = client.get(base + f"/dataset/{dataset}/overview", params={"logical_name": logical}).json()
    graph = client.get(base + "/graph").json()
    assert all(step["label_parameters"]["resolved_fix_count"] == 0 for step in graph["steps"])
    for individual, stats in overview["stats"].items():
        for key, value in stats.items():
            assert after["stats"][individual][key] == value
        assert not after["stats"][individual].get("reviewed")
    assert {name: hashlib.sha256((study / name).read_bytes()).hexdigest() for name in raw_hashes} == raw_hashes
    empty_manual = client.post(base + "/actions/annotate-scope", json={
        "dataset_id": dataset, "logical_name": logical, "expected_current_dataset_id": dataset,
        "expected_review_revision": load_review_state(study)["revision"],
        "scope": {"kind": "fix", "fix_keys": []}, "status": "suspected",
        "issue_type": "Manual flag", "comment": "Nothing selected", "user": "Reviewer",
    })
    assert empty_manual.status_code == 400
    assert client.get(base + "/load").json()["dataset_id"] == dataset
