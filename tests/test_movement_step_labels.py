from copy import deepcopy
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from examples.movement import routes


def test_frontend_step_labels_and_dropdown(node_runtime):
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node_runtime, str(root / "tests/frontend/step_labels.cjs"),
         str(root / "examples/movement/static/app.js")],
        capture_output=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_graph_labels_include_saved_threshold_without_large_scopes(monkeypatch):
    graph = {"steps": [{"step_id": "step_1", "title": "Flag filter"}]}
    history = {"steps": [{"step_id": "step_1", "parameters": {
        "action": "annotate_scope", "status": "suspected", "issue_type": "High speed",
        "issue_field": "speed_mps", "issue_threshold": "> 5",
        "scope": {"kind": "filter", "source_rows": [{"row_ranges": [[1, 10000]]}],
                  "filter": {"field_key": "speed_mps", "operator": "gt",
                             "threshold_value": 5, "individuals": ["animal-A"]}},
        "records": [{"large": "not needed for display"}],
    }}]}
    original = deepcopy(history)
    monkeypatch.setattr(routes, "graph_payload", lambda _: deepcopy(graph))
    monkeypatch.setattr(routes, "list_history", lambda _: history)
    loaded = routes._movement_graph_payload(Path("unused"), history)
    refreshed = routes._movement_graph_payload(Path("unused"))
    assert loaded == refreshed
    labels = loaded["steps"][0]["label_parameters"]
    assert labels["filter"] == {"field_key": "speed_mps", "operator": "gt", "threshold_value": 5}
    assert labels["action"] == "annotate_scope"
    assert labels["issue_type"] == "High speed"
    assert "scope" not in labels and "records" not in labels
    assert history == original


def test_graph_labels_allow_legacy_steps_without_parameters(monkeypatch):
    monkeypatch.setattr(routes, "graph_payload", lambda _: {"steps": [{"step_id": "old", "title": "Old title"}]})
    result = routes._movement_graph_payload(Path("unused"), {"steps": []})
    assert result["steps"][0] == {
        "step_id": "old", "title": "Old title", "label_parameters": {"filter": {}},
    }
