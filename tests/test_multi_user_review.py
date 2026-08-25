import json
from pathlib import Path
import sys
from unittest.mock import ANY

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.auth import (
    Actor,
    AuthenticationError,
    AuthManager,
    add_authentication,
    apply_actor,
    build_user_record,
    hash_password,
    verify_password,
)
from app import auth_cli
from app.execution import create_step, undo_to_parent
from app.reviews import (
    ReviewConflictError,
    ReviewLockedError,
    assign_review,
    authorize_persistent_change,
    finish_editor_control,
    review_coverage,
    review_profile,
    start_editor_control,
)
from app.state import ensure_project_state
from app.state import get_dataset_artifact, load_dataset, project_paths
from app.web import create_app
from examples.movement.routes import register_movement_routes


STEP_SCRIPT = """
import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
source = Path(spec["input_artifacts"][0]["path"])
output = Path(spec["output_artifacts"][0]["path"])
output.parent.mkdir(parents=True, exist_ok=True)
output.write_bytes(source.read_bytes())
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text(json.dumps({"updated": True}))
"""


def _actors():
    editor = Actor("user_editor", "editor", "Eli Editor", "editor")
    reviewer = Actor("user_reviewer", "reviewer", "Rae Reviewer", "reviewer")
    return editor, reviewer


def _project(tmp_path: Path):
    project = tmp_path / "study"
    project.mkdir()
    (project / "movement.csv").write_text("individual,value\nalpha,1\nbeta,2\n")
    state = ensure_project_state(project)
    return project, state["current_dataset_id"]


def _decision(
    review_id: str,
    dataset_id: str,
    individual: str,
    decision: str,
    *,
    needs_check: bool = False,
):
    return {
        "annotation_kind": "individual_review",
        "reviewed": True,
        "review_id": review_id,
        "source_dataset_id": dataset_id,
        "review_decision": decision,
        "needs_check": needs_check,
        "scope": {"kind": "individual", "individual": individual},
    }


def test_cookie_authentication_and_server_owned_attribution():
    manager = AuthManager(
        [
            build_user_record(
                username="reviewer",
                display_name="Rae Reviewer",
                role="reviewer",
                password="correct-horse-battery",
                user_id="user_reviewer",
            )
        ]
    )
    app = FastAPI()
    add_authentication(app, manager)
    client = TestClient(app)

    assert client.get("/api/auth/me").status_code == 401
    rejected = client.post(
        "/api/auth/login",
        headers={"Origin": "https://hostile.example"},
        json={"username": "reviewer", "password": "correct-horse-battery"},
    )
    assert rejected.status_code == 403
    logged_in = client.post(
        "/api/auth/login",
        json={"username": "reviewer", "password": "correct-horse-battery"},
    )
    assert logged_in.status_code == 200
    assert "HttpOnly" in logged_in.headers["set-cookie"]
    assert "SameSite=strict" in logged_in.headers["set-cookie"]
    assert client.get("/api/auth/me").json()["actor"]["user_id"] == "user_reviewer"

    actor = manager.actor_by_id("user_reviewer")
    attributed = apply_actor(
        {"user": "forged", "actor": {"user_id": "forged"}, "parameters": {}},
        actor,
        review_id="review_current",
    )
    assert attributed["user"] == "Rae Reviewer"
    assert attributed["actor"]["user_id"] == "user_reviewer"
    assert attributed["parameters"]["workflow"]["review_id"] == "review_current"


def test_passwords_have_no_minimum_length_but_cannot_be_empty():
    password_hash = hash_password("x")
    assert verify_password("x", password_hash) is True
    assert verify_password("", password_hash) is False
    with pytest.raises(AuthenticationError, match="Password is required"):
        hash_password("")


def test_operator_cli_bootstraps_and_manages_accounts(tmp_path, monkeypatch, capsys):
    data_root = tmp_path / "data"
    passwords = iter(["a", "b", "c"])
    monkeypatch.setattr(auth_cli, "_password", lambda: next(passwords))
    root_args = ["--data-root", str(data_root)]
    assert auth_cli.main(root_args + ["bootstrap", "admin", "--display-name", "Admin User"]) == 0
    assert auth_cli.main(root_args + ["add", "rae", "--display-name", "Rae Reviewer", "--role", "reviewer"]) == 0
    assert auth_cli.main(root_args + ["disable", "rae"]) == 0
    assert auth_cli.main(root_args + ["enable", "rae"]) == 0
    assert auth_cli.main(root_args + ["reset-password", "rae"]) == 0
    assert auth_cli.main(root_args + ["list"]) == 0
    listing = capsys.readouterr().out
    assert "admin\tAdmin User\teditor\tenabled" in listing
    assert "rae\tRae Reviewer\treviewer\tenabled" in listing


def test_review_coverage_follows_dataset_update_and_undo(tmp_path):
    project, baseline = _project(tmp_path)
    editor, reviewer = _actors()
    assignment = assign_review(
        project,
        editor=editor,
        reviewer=reviewer,
        expected_current_dataset_id=baseline,
        expected_review_revision=0,
        individuals=["alpha", "beta"],
    )
    review = assignment["review"]
    review_id = review["review_id"]
    original = [
        _decision(review_id, baseline, "alpha", "ok"),
        _decision(review_id, baseline, "beta", "ok", needs_check=True),
    ]
    assert review_coverage(project, review, original) == {
        "required_count": 2,
        "reviewed_count": 2,
        "remaining_count": 0,
        "remaining_individuals": [],
        "needs_check_count": 1,
        "needs_check_individuals": ["beta"],
        "complete_allowed": True,
    }

    controlled = start_editor_control(
        project,
        editor=editor,
        expected_current_dataset_id=baseline,
        expected_review_revision=1,
        reason="Apply a new source delivery",
    )
    revision = controlled["state"]["revision"]
    updated = create_step(
        project,
        {
            "user": editor.display_name,
            "actor": editor.as_dict(),
            "title": "Update movement records",
            "kind": "python",
            "script": STEP_SCRIPT,
            "parent_dataset_id": baseline,
            "input_artifacts": ["movement.csv"],
            "output_artifacts": ["movement.csv"],
            "parameters": {
                "workflow": {
                    "review_id": review_id,
                    "review_effect": "changes_individual_scope",
                    "review_impact": {
                        "scope": "individuals",
                        "added_individuals": ["gamma"],
                        "changed_individuals": ["alpha"],
                        "removed_individuals": ["beta"],
                    },
                }
            },
            "set_as_head": True,
        },
    )
    updated_id = updated["dataset"]["dataset_id"]
    coverage = review_coverage(project, review, original)
    assert coverage["remaining_individuals"] == ["alpha", "gamma"]
    assert coverage["needs_check_count"] == 0

    current = original + [
        _decision(review_id, updated_id, "alpha", "fix_keep"),
        _decision(review_id, updated_id, "gamma", "remove", needs_check=True),
    ]
    assert review_coverage(project, review, current)["complete_allowed"] is True
    profile = review_profile(project, reviewer, current)
    assert profile["capabilities"]["can_review"] is False
    released = finish_editor_control(
        project,
        editor=editor,
        expected_current_dataset_id=updated_id,
        expected_review_revision=revision,
    )
    assert review_profile(project, reviewer, current)["capabilities"]["can_review"] is True
    undo_to_parent(project)
    restored = review_coverage(project, review, original)
    assert restored["required_count"] == 2
    assert restored["needs_check_individuals"] == ["beta"]
    with pytest.raises(ReviewConflictError):
        start_editor_control(
            project,
            editor=editor,
            expected_current_dataset_id=baseline,
            expected_review_revision=revision,
            reason="Stale browser",
        )


def test_editor_can_be_assigned_as_review_owner_without_intervention_control(tmp_path):
    project, baseline = _project(tmp_path)
    editor, _ = _actors()
    assignment = assign_review(
        project,
        editor=editor,
        reviewer=editor,
        expected_current_dataset_id=baseline,
        expected_review_revision=0,
        individuals=["alpha", "beta"],
    )
    review = assignment["review"]
    assert review["reviewer_user_id"] == editor.user_id
    assert review["reviewer"]["role"] == "editor"

    profile = review_profile(project, editor, [])
    assert profile["capabilities"]["can_review"] is True
    assert profile["capabilities"]["can_intervene"] is False
    assert profile["capabilities"]["can_update_dataset"] is False
    assert authorize_persistent_change(
        project,
        editor,
        expected_review_revision=assignment["state"]["revision"],
        review_effect="annotation_only",
    )["review_id"] == review["review_id"]
    with pytest.raises(ReviewLockedError, match="Take editor control"):
        authorize_persistent_change(
            project,
            editor,
            expected_review_revision=assignment["state"]["revision"],
            review_effect="changes_individual_scope",
        )


def test_assigned_editor_can_submit_review_decisions_without_taking_control(tmp_path):
    data_root = tmp_path / "data"
    study = data_root / "movement_raw" / "editor_assignment"
    study.mkdir(parents=True)
    (study / "movement.csv").write_text(
        "eventid,individual,timestamp,longitude,latitude\n"
        "a1,alpha,2024-01-01T00:00:00Z,-70,40\n"
    )
    manager = AuthManager(
        [
            build_user_record(
                username="admin",
                display_name="Admin Editor",
                role="editor",
                password="admin-password-long",
                user_id="user_admin",
            ),
            build_user_record(
                username="assigned-editor",
                display_name="Assigned Editor",
                role="editor",
                password="assigned-editor-password-long",
                user_id="user_assigned_editor",
            ),
        ]
    )
    static_root = Path(__file__).resolve().parents[1] / "examples" / "movement" / "static"
    app = create_app(data_root=data_root, static_root=static_root, auth_manager=manager)
    register_movement_routes(app, data_root=data_root, allowed_families={"movement_raw"})
    admin_client = TestClient(app)
    assignee_client = TestClient(app)
    assert admin_client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "admin-password-long"},
    ).status_code == 200
    assert assignee_client.post(
        "/api/auth/login",
        json={
            "username": "assigned-editor",
            "password": "assigned-editor-password-long",
        },
    ).status_code == 200

    loaded = admin_client.get(
        "/api/apps/movement/family/movement_raw/study/editor_assignment/load"
    ).json()
    assigned = admin_client.post(
        "/api/apps/movement/family/movement_raw/study/editor_assignment/review/assign",
        json={
            "reviewer_user_id": "user_assigned_editor",
            "logical_name": "movement.csv",
            "expected_current_dataset_id": loaded["dataset_id"],
            "expected_review_revision": loaded["edit_profile"]["review_revision"],
        },
    )
    assert assigned.status_code == 200, assigned.text
    assignee_load = assignee_client.get(
        "/api/apps/movement/family/movement_raw/study/editor_assignment/load"
    ).json()
    assert assignee_load["edit_profile"]["editable"] is True
    assert assignee_load["edit_profile"]["capabilities"]["can_review"] is True
    assert assignee_load["edit_profile"]["editor_control"] is None

    decision = assignee_client.post(
        "/api/apps/movement/family/movement_raw/study/editor_assignment/actions/review-individual",
        json={
            "dataset_id": assignee_load["dataset_id"],
            "logical_name": "movement.csv",
            "expected_current_dataset_id": assignee_load["dataset_id"],
            "expected_review_revision": assignee_load["edit_profile"]["review_revision"],
            "decision": {
                "individual": "alpha",
                "review_decision": "ok",
                "needs_check": False,
                "comment": "",
            },
        },
    )
    assert decision.status_code == 200, decision.text


def test_movement_routes_hide_unassigned_studies_and_persist_session_actor(tmp_path):
    data_root = tmp_path / "data"
    study = data_root / "movement_raw" / "study_one"
    study.mkdir(parents=True)
    (study / "movement.csv").write_text(
        "eventid,individual,timestamp,longitude,latitude\n"
        "a1,alpha,2024-01-01T00:00:00Z,-70,40\n"
        "b1,beta,2024-01-01T00:00:00Z,-71,41\n"
    )
    editor_record = build_user_record(
        username="editor",
        display_name="Eli Editor",
        role="editor",
        password="editor-password-long",
        user_id="user_editor",
    )
    reviewer_record = build_user_record(
        username="reviewer",
        display_name="Rae Reviewer",
        role="reviewer",
        password="reviewer-password-long",
        user_id="user_reviewer",
    )
    assignee_editor_record = build_user_record(
        username="editor-reviewer",
        display_name="Edda Editor Reviewer",
        role="editor",
        password="editor-reviewer-password-long",
        user_id="user_editor_reviewer",
    )
    manager = AuthManager([editor_record, reviewer_record, assignee_editor_record])
    static_root = Path(__file__).resolve().parents[1] / "examples" / "movement" / "static"
    app = create_app(data_root=data_root, static_root=static_root, auth_manager=manager)
    register_movement_routes(app, data_root=data_root, allowed_families={"movement_raw"})
    editor_client = TestClient(app)
    reviewer_client = TestClient(app)
    assert editor_client.post(
        "/api/auth/login",
        json={"username": "editor", "password": "editor-password-long"},
    ).status_code == 200
    assert reviewer_client.post(
        "/api/auth/login",
        json={"username": "reviewer", "password": "reviewer-password-long"},
    ).status_code == 200

    hidden = reviewer_client.get("/api/apps/movement/family/movement_raw/studies")
    assert hidden.status_code == 200
    assert hidden.json()["studies"] == []
    assert reviewer_client.get(
        "/api/apps/movement/family/movement_raw/study/study_one/load"
    ).status_code == 404
    assert reviewer_client.get("/api/projects").status_code == 403
    baseline_id = ensure_project_state(study)["current_dataset_id"]
    analyses_before = len(list(project_paths(study)["analyses"].iterdir()))
    blocked_filter = reviewer_client.post(
        "/api/apps/movement/family/movement_raw/study/study_one/actions/run-candidate-query",
        json={
            "dataset_id": baseline_id,
            "logical_name": "movement.csv",
            "query_definition": {
                "app": "movement",
                "name": "Fast fixes",
                "candidate_kind": "fix",
                "evaluator": {"type": "fix_numeric_comparison"},
                "definition": {"field": "speed_mps", "op": ">", "value": 0},
                "parameters": {},
                "required_fields": ["speed_mps"],
            },
        },
    )
    assert blocked_filter.status_code == 409
    assert len(list(project_paths(study)["analyses"].iterdir())) == analyses_before
    loaded = editor_client.get(
        "/api/apps/movement/family/movement_raw/study/study_one/load"
    ).json()
    assignable_users = editor_client.get("/api/apps/movement/reviewers")
    assert assignable_users.status_code == 200
    assert {
        (item["user_id"], item["role"])
        for item in assignable_users.json()["reviewers"]
    } == {
        ("user_editor", "editor"),
        ("user_editor_reviewer", "editor"),
        ("user_reviewer", "reviewer"),
    }
    profile = loaded["edit_profile"]
    assigned = editor_client.post(
        "/api/apps/movement/family/movement_raw/study/study_one/review/assign",
        json={
            "reviewer_user_id": "user_reviewer",
            "logical_name": "movement.csv",
            "expected_current_dataset_id": loaded["dataset_id"],
            "expected_review_revision": profile["review_revision"],
        },
    )
    assert assigned.status_code == 200

    control = editor_client.post(
        "/api/apps/movement/family/movement_raw/study/study_one/editor-control/start",
        json={
            "reason": "Check an uncertain location",
            "expected_current_dataset_id": loaded["dataset_id"],
            "expected_review_revision": assigned.json()["state"]["revision"],
        },
    )
    assert control.status_code == 200
    locked_profile = reviewer_client.get(
        "/api/apps/movement/family/movement_raw/study/study_one/edit-profile",
        params={"dataset_id": loaded["dataset_id"]},
    ).json()
    assert locked_profile["editable"] is False
    assert locked_profile["editor_control"]["owner_user_id"] == "user_editor"
    released = editor_client.post(
        "/api/apps/movement/family/movement_raw/study/study_one/editor-control/finish",
        json={
            "expected_current_dataset_id": loaded["dataset_id"],
            "expected_review_revision": control.json()["state"]["revision"],
        },
    )
    assert released.status_code == 200

    studies = reviewer_client.get("/api/apps/movement/family/movement_raw/studies").json()
    assert [item["name"] for item in studies["studies"]] == ["study_one"]
    reviewer_load = reviewer_client.get(
        "/api/apps/movement/family/movement_raw/study/study_one/load"
    ).json()
    assert reviewer_load["edit_profile"]["editable"] is True
    first_result = reviewer_client.post(
        "/api/apps/movement/family/movement_raw/study/study_one/actions/review-individual",
        json={
            "dataset_id": reviewer_load["dataset_id"],
            "logical_name": "movement.csv",
            "expected_current_dataset_id": reviewer_load["dataset_id"],
            "expected_review_revision": reviewer_load["edit_profile"]["review_revision"],
            "user": "Forged Browser Name",
            "actor": {"user_id": "forged"},
            "decision": {
                "individual": "alpha",
                "review_decision": "ok",
                "needs_check": False,
                "comment": "",
            },
        },
    )
    assert first_result.status_code == 200, first_result.text
    first_output_id = first_result.json()["dataset"]["dataset_id"]
    active_dashboard = editor_client.get("/api/apps/movement/admin/review-summary")
    assert active_dashboard.status_code == 200
    active_row = active_dashboard.json()["studies"][0]
    assert active_row["review"]["status"] == "active"
    assert active_row["current_dataset_id"] == first_output_id
    assert active_row["counts"] == {
        "required": 2,
        "reviewed": 1,
        "undecided": 1,
        "ok": 1,
        "fix_keep": 0,
        "remove": 0,
        "needs_check": 0,
    }
    active_dashboard_detail = editor_client.get(
        "/api/apps/movement/admin/review-summary",
        params={
            "family": "movement_raw",
            "study": "study_one",
            "include_individuals": "true",
        },
    )
    assert active_dashboard_detail.status_code == 200
    assert active_dashboard_detail.json()["studies"][0]["individuals"] == [
        {
            "individual": "alpha",
            "review_decision": "ok",
            "needs_check": False,
            "reviewed_at": ANY,
        },
        {
            "individual": "beta",
            "review_decision": "",
            "needs_check": False,
            "reviewed_at": "",
        },
    ]
    result = reviewer_client.post(
        "/api/apps/movement/family/movement_raw/study/study_one/actions/review-individual",
        json={
            "dataset_id": first_output_id,
            "logical_name": "movement.csv",
            "expected_current_dataset_id": first_output_id,
            "expected_review_revision": reviewer_load["edit_profile"]["review_revision"],
            "decision": {
                "individual": "beta",
                "review_decision": "fix_keep",
                "needs_check": True,
                "comment": "check",
            },
        },
    )
    assert result.status_code == 200, result.text
    output_id = result.json()["dataset"]["dataset_id"]
    dataset = load_dataset(study, output_id)
    assert dataset["actor"]["user_id"] == "user_reviewer"
    _, annotations_path = get_dataset_artifact(
        study, output_id, "movement_review_annotations.json"
    )
    annotations = json.loads(annotations_path.read_text())["annotations"]
    assert {item["review_decision"] for item in annotations} == {"ok", "fix_keep"}
    assert [item["scope"]["individual"] for item in annotations if item["needs_check"]] == ["beta"]
    assert all(item["user"] == "Rae Reviewer" for item in annotations)
    assert all(item["actor"]["user_id"] == "user_reviewer" for item in annotations)
    assert all(item["review_id"] == assigned.json()["review"]["review_id"] for item in annotations)

    current = reviewer_client.get(
        "/api/apps/movement/family/movement_raw/study/study_one/edit-profile",
        params={"dataset_id": output_id},
    ).json()
    assert current["coverage"]["complete_allowed"] is True
    assert current["coverage"]["needs_check_individuals"] == ["beta"]

    completed = reviewer_client.post(
        "/api/apps/movement/family/movement_raw/study/study_one/review/complete",
        json={
            "expected_current_dataset_id": output_id,
            "expected_review_revision": current["review_revision"],
        },
    )
    assert completed.status_code == 200
    completed_dashboard = editor_client.get("/api/apps/movement/admin/review-summary")
    assert completed_dashboard.status_code == 200
    completed_row = completed_dashboard.json()["studies"][0]
    assert completed_row["review"]["status"] == "completed"
    assert completed_row["counts"]["ok"] == 1
    assert completed_row["counts"]["fix_keep"] == 1
    assert completed_row["counts"]["remove"] == 0
    assert completed_row["counts"]["needs_check"] == 1
    assert completed_row["counts"]["undecided"] == 0
    reassigned = editor_client.post(
        "/api/apps/movement/family/movement_raw/study/study_one/review/assign",
        json={
            "reviewer_user_id": "user_reviewer",
            "logical_name": "movement.csv",
            "expected_current_dataset_id": output_id,
            "expected_review_revision": completed.json()["state"]["revision"],
        },
    )
    assert reassigned.status_code == 200
    overview = reviewer_client.get(
        "/api/apps/movement/family/movement_raw/study/study_one/"
        f"dataset/{output_id}/overview",
        params={"logical_name": "movement.csv"},
    )
    assert overview.status_code == 200
    assert all(not item.get("reviewed", False) for item in overview.json()["stats"].values())
    fresh_profile = reviewer_client.get(
        "/api/apps/movement/family/movement_raw/study/study_one/edit-profile",
        params={"dataset_id": output_id},
    ).json()
    assert fresh_profile["coverage"]["reviewed_count"] == 0
    assert fresh_profile["coverage"]["prior_needs_check_individuals"] == ["beta"]
    assert fresh_profile["coverage"]["prior_decisions_by_individual"]["alpha"]["review_decision"] == "ok"
    assert fresh_profile["coverage"]["prior_decisions_by_individual"]["beta"]["review_decision"] == "fix_keep"
    assert fresh_profile["coverage"]["prior_decisions_by_individual"]["beta"]["needs_check"] is True

    forbidden_dashboard = reviewer_client.get("/api/apps/movement/admin/review-summary")
    assert forbidden_dashboard.status_code == 403

    dashboard = editor_client.get("/api/apps/movement/admin/review-summary")
    assert dashboard.status_code == 200
    dashboard_row = dashboard.json()["studies"][0]
    assert dashboard_row["family"] == "movement_raw"
    assert dashboard_row["study"] == "study_one"
    assert dashboard_row["review"]["status"] == "active"
    assert dashboard_row["counts"] == {
        "required": 2,
        "reviewed": 0,
        "undecided": 2,
        "ok": 0,
        "fix_keep": 0,
        "remove": 0,
        "needs_check": 0,
    }

    dashboard_detail = editor_client.get(
        "/api/apps/movement/admin/review-summary",
        params={
            "family": "movement_raw",
            "study": "study_one",
            "include_individuals": "true",
        },
    )
    assert dashboard_detail.status_code == 200
    assert dashboard_detail.json()["studies"][0]["individuals"] == [
        {"individual": "alpha", "review_decision": "", "needs_check": False, "reviewed_at": ""},
        {"individual": "beta", "review_decision": "", "needs_check": False, "reviewed_at": ""},
    ]
