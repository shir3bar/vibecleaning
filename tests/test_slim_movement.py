from pathlib import Path
import sys

from fastapi.testclient import TestClient


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MOVEMENT_STATIC_ROOT = REPO_ROOT / "examples" / "movement" / "static"
SLIM_INDEX = MOVEMENT_STATIC_ROOT / "index.html"

from examples.slim_movement.app import create_slim_movement_app
from app.auth import AuthManager


MOVEMENT_CSV = """eventid,individual,timestamp,longitude,latitude
fix_1,alpha,2024-01-01T00:00:00Z,-70,40
fix_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1
"""


def create_slim_test_client(
    tmp_path: Path,
    *,
    credentials: tuple[str, str] | None = None,
    authenticated: bool = True,
    shared_locking: str | None = None,
) -> TestClient:
    data_root = tmp_path / "data"
    raw_study = data_root / "movement_raw" / "raw_study"
    raw_study.mkdir(parents=True)
    (raw_study / "zebra_raw.csv").write_text(MOVEMENT_CSV, encoding="utf-8")
    (raw_study / "a_osm_context.csv").write_text(MOVEMENT_CSV, encoding="utf-8")
    (raw_study / "movement_review_annotations.json").write_text('{"annotations": []}\n', encoding="utf-8")
    clean_study = data_root / "movement_clean" / "clean_study"
    clean_study.mkdir(parents=True)
    (clean_study / "clean.csv").write_text(MOVEMENT_CSV, encoding="utf-8")

    username, password = credentials or ("test-reviewer", "test-password-long")
    app = create_slim_movement_app(
        data_root=data_root,
        static_root=MOVEMENT_STATIC_ROOT,
        index_path=SLIM_INDEX,
        auth_manager=AuthManager.for_testing(
            username=username,
            password=password,
            role="editor",
        ),
        shared_locking=shared_locking,
    )
    client = TestClient(app)
    if authenticated:
        response = client.post(
            "/api/auth/login",
            json={"username": username, "password": password},
        )
        assert response.status_code == 200
    return client


def test_slim_runtime_exposes_cooperative_mode_warning(tmp_path):
    client = create_slim_test_client(
        tmp_path,
        authenticated=False,
        shared_locking="disabled",
    )

    response = client.get("/api/runtime")

    assert response.status_code == 200
    assert response.json()["cooperative_mode"] is True
    assert "one active writer" in response.json()["warning"]


def test_slim_movement_serves_shared_viewer_with_slim_profile(tmp_path):
    client = create_slim_test_client(tmp_path)

    response = client.get("/", headers={"Authorization": ""})
    app_js = client.get("/static/app.js", headers={"Authorization": ""})
    login_js = client.get("/static/login.js", headers={"Authorization": ""})

    assert response.status_code == 200
    assert '<meta name="vibecleaning-movement-mode" content="slim_movement">' in response.text
    assert 'data-role="admin-dashboard" hidden>Review dashboard</button>' in response.text
    assert 'id="login-form"' in response.text
    assert 'id="app-shell" hidden' in response.text
    assert 'class="topbar-actions"' in response.text
    assert 'data-role="edit-lock-profile"' in response.text
    assert response.text.index('data-role="edit-lock-profile"') < response.text.index(
        'id="logout-button"'
    )
    assert app_js.status_code == 200
    assert "MOVEMENT_APP_CONFIG" in app_js.text
    assert login_js.status_code == 200
    assert 'fetch("/api/auth/login"' in login_js.text

    dashboard = client.get("/api/apps/movement/admin/review-summary")
    assert dashboard.status_code == 200
    assert [item["family"] for item in dashboard.json()["studies"]] == ["movement_raw"]


def test_slim_auth_keeps_login_assets_public_and_protects_data_routes(tmp_path):
    password = "a-long-random-password"
    client = create_slim_test_client(
        tmp_path,
        credentials=("reviewer", password),
        authenticated=False,
    )

    for path in ("/", "/static/app.js", "/static/login.js", "/api/runtime"):
        response = client.get(path)
        assert response.status_code == 200
        assert "www-authenticate" not in response.headers
        assert "set-cookie" not in response.headers

    runtime = client.get("/api/runtime")
    assert runtime.json()["shared_locking"] == "required"

    for path in (
        "/api/auth/me",
        "/api/apps/movement/families",
        "/api/apps/movement/family/movement_raw/study/raw_study/load",
        "/api/projects",
    ):
        response = client.get(path)
        assert response.status_code == 401
        assert response.json() == {"error": "Authentication required"}
        assert "www-authenticate" not in response.headers
        assert "set-cookie" not in response.headers
        assert response.headers["cache-control"] == "no-store"

    for auth in (
        ("reviewer", "wrong-password"),
        ("wrong-user", password),
    ):
        response = client.post(
            "/api/auth/login",
            json={"username": auth[0], "password": auth[1]},
        )
        assert response.status_code == 401
        assert "www-authenticate" not in response.headers

    authenticated = client.post(
        "/api/auth/login",
        json={"username": "reviewer", "password": password},
    )
    assert authenticated.status_code == 200
    assert authenticated.json() == {
        "authenticated": True,
        "actor": authenticated.json()["actor"],
    }
    assert authenticated.headers["cache-control"] == "no-store"
    assert (
        client.get(
            "/api/apps/movement/families",
        ).status_code
        == 200
    )


def test_slim_movement_catalog_exposes_only_movement_raw(tmp_path):
    client = create_slim_test_client(tmp_path)

    families = client.get("/api/apps/movement/families")
    raw_studies = client.get("/api/apps/movement/family/movement_raw/studies")
    clean_studies = client.get("/api/apps/movement/family/movement_clean/studies")

    assert families.status_code == 200
    assert [item["name"] for item in families.json()["families"]] == ["movement_raw"]
    assert [item["name"] for item in raw_studies.json()["studies"]] == ["raw_study"]
    assert clean_studies.status_code == 404


def test_slim_movement_load_selects_raw_csv_and_keeps_review_routes(tmp_path):
    client = create_slim_test_client(tmp_path)

    loaded = client.get("/api/apps/movement/family/movement_raw/study/raw_study/load")

    assert loaded.status_code == 200
    assert loaded.json()["logical_name"] == "zebra_raw.csv"
    paths = {route.path for route in client.app.routes}
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/annotate-scope" in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/confirm-issues" in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/dismiss-issues" in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/review-individual" in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/export-reviewed-csv" in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/generate-report" in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/run-burst-anomaly-ranking" in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/run-candidate-query" not in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/run-burst-feature-space" not in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/enrich-osm-context" not in paths


def test_slim_movement_applies_shared_history_edit_locks(tmp_path):
    client = create_slim_test_client(tmp_path)
    base_url = "/api/apps/movement/family/movement_raw/study/raw_study"
    loaded = client.get(f"{base_url}/load").json()
    root_id = loaded["dataset_id"]
    initial_profile = client.get(
        f"{base_url}/edit-profile",
        params={"dataset_id": root_id},
    )
    assert initial_profile.status_code == 200
    assert initial_profile.json()["editable"] is True

    annotation = client.post(
        f"{base_url}/actions/annotate-scope",
        json={
            "dataset_id": root_id,
            "expected_current_dataset_id": root_id,
            "expected_review_revision": initial_profile.json()["review_revision"],
            "logical_name": "zebra_raw.csv",
            "scope": {"kind": "fix", "fix_keys": ["id:fix_1#row:1"]},
            "status": "suspected",
            "origin": "manual",
            "issue_type": "location review",
            "comment": "Check this location",
            "owner_question": "Is this fix valid?",
            "user": "reviewer",
        },
    )
    assert annotation.status_code == 200
    current_id = annotation.json()["dataset"]["dataset_id"]

    blocked = client.post(
        f"{base_url}/actions/review-individual",
        json={
            "dataset_id": root_id,
            "expected_current_dataset_id": current_id,
            "expected_review_revision": initial_profile.json()["review_revision"],
            "logical_name": "zebra_raw.csv",
            "decision": {
                "individual": "alpha",
                "review_decision": "ok",
                "needs_check": False,
            },
            "user": "reviewer",
        },
    )
    assert blocked.status_code == 423
    assert blocked.json()["edit_profile"]["blockers"][0]["code"] == "historical_version"


def test_slim_movement_uses_compact_overviews(tmp_path):
    client = create_slim_test_client(tmp_path)
    loaded = client.get(
        "/api/apps/movement/family/movement_raw/study/raw_study/load"
    ).json()

    response = client.get(
        "/api/apps/movement/family/movement_raw/study/raw_study/"
        f"dataset/{loaded['dataset_id']}/overview",
        params={"logical_name": loaded["logical_name"]},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_rows"] == 2
    assert payload["fixes"] == []
    assert payload["overview_fix_limit"] == 0
    assert payload["overview_series_point_limit"] == 250


def test_slim_movement_can_flag_export_and_generate_report(tmp_path):
    client = create_slim_test_client(tmp_path)
    loaded = client.get("/api/apps/movement/family/movement_raw/study/raw_study/load").json()

    annotation = client.post(
        "/api/apps/movement/family/movement_raw/study/raw_study/actions/annotate-scope",
        json={
            "dataset_id": loaded["dataset_id"],
            "expected_current_dataset_id": loaded["dataset_id"],
            "expected_review_revision": loaded["edit_profile"]["review_revision"],
            "logical_name": "zebra_raw.csv",
            "scope": {"kind": "fix", "fix_keys": ["id:fix_1#row:1"]},
            "status": "suspected",
            "origin": "manual",
            "issue_type": "location review",
            "comment": "Check this location",
            "owner_question": "Is this fix valid?",
            "user": "reviewer",
        },
    )
    assert annotation.status_code == 200
    reviewed_dataset_id = annotation.json()["dataset"]["dataset_id"]

    exported = client.post(
        "/api/apps/movement/family/movement_raw/study/raw_study/actions/export-reviewed-csv",
        json={
            "dataset_id": reviewed_dataset_id,
            "logical_name": "zebra_raw.csv",
            "user": "reviewer",
        },
    )
    assert exported.status_code == 200
    assert exported.json()["summary"]["flagged_row_count"] == 1
    assert exported.json()["analysis"]["output_artifacts"] == ["zebra_raw_reviewed.csv"]

    report = client.post(
        "/api/apps/movement/family/movement_raw/study/raw_study/actions/generate-report",
        json={
            "dataset_id": reviewed_dataset_id,
            "logical_name": "zebra_raw.csv",
            "report_type": "individual_profile",
            "individuals": ["alpha"],
            "output_mode": "combined",
            "user": "reviewer",
        },
    )
    assert report.status_code == 200
    report_analysis = report.json()["analysis"]
    realized = {item["logical_name"] for item in report_analysis["realized_output_artifacts"]}
    assert "movement_individual_reports.html" in realized

    export_analysis_id = exported.json()["analysis"]["analysis_id"]
    export_path = (
        "/api/apps/movement/family/movement_raw/study/raw_study/"
        f"analysis/{export_analysis_id}/artifact/zebra_raw_reviewed.csv"
    )
    report_path = (
        "/api/apps/movement/family/movement_raw/study/raw_study/"
        f"analysis/{report_analysis['analysis_id']}/artifact/"
        "movement_individual_reports.html"
    )
    assert client.get(export_path).status_code == 200
    assert client.get(report_path).status_code == 200
    assert client.post("/api/auth/logout").status_code == 200
    assert client.get(export_path).status_code == 401
    assert client.get(report_path).status_code == 401


def test_slim_anomaly_ranking_accepts_only_movement_features(tmp_path):
    client = create_slim_test_client(tmp_path)
    loaded = client.get(
        "/api/apps/movement/family/movement_raw/study/raw_study/load"
    ).json()

    response = client.post(
        "/api/apps/movement/family/movement_raw/study/raw_study/"
        "actions/run-burst-anomaly-ranking",
        json={
            "dataset_id": loaded["dataset_id"],
            "logical_name": loaded["logical_name"],
            "feature_set": "movement_plus_context",
            "user": "reviewer",
        },
    )

    assert response.status_code == 400
    assert response.json()["error"] == "Invalid feature_set"


def test_slim_anomaly_ranking_runs_as_background_job(tmp_path):
    client = create_slim_test_client(tmp_path)
    loaded = client.get(
        "/api/apps/movement/family/movement_raw/study/raw_study/load"
    ).json()

    response = client.post(
        "/api/apps/movement/family/movement_raw/study/raw_study/"
        "actions/run-burst-anomaly-ranking",
        json={
            "dataset_id": loaded["dataset_id"],
            "logical_name": loaded["logical_name"],
            "burst_gap_mode": "manual",
            "burst_gap_seconds": 7200,
            "feature_set": "movement_only",
            "user": "reviewer",
        },
    )

    assert response.status_code == 202
    queued = response.json()
    assert queued["job_id"].startswith("analysis_job_")
    job = client.get(
        "/api/apps/movement/family/movement_raw/study/raw_study/"
        f"analysis-jobs/{queued['job_id']}"
    )
    assert job.status_code == 200
    completed = job.json()
    assert completed["status"] == "completed"
    assert completed["result"]["analysis"]["parameters"]["action"] == (
        "run_burst_anomaly_ranking"
    )
    assert completed["result"]["summary"]["run_status"] in {
        "completed",
        "unresolved",
    }
