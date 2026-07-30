import base64
from pathlib import Path
import sys

from fastapi.testclient import TestClient
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MOVEMENT_STATIC_ROOT = REPO_ROOT / "examples" / "movement" / "static"
SLIM_INDEX = REPO_ROOT / "examples" / "slim_movement" / "static" / "index.html"
MOVEMENT_APP_JS = MOVEMENT_STATIC_ROOT / "app.js"

from examples.slim_movement.app import create_slim_movement_app
from examples.slim_movement.auth import (
    DEFAULT_USERNAME,
    PASSWORD_ENV,
    USERNAME_ENV,
    startup_credentials,
)


MOVEMENT_CSV = """eventid,individual,timestamp,longitude,latitude
fix_1,alpha,2024-01-01T00:00:00Z,-70,40
fix_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1
"""


def create_slim_test_client(
    tmp_path: Path,
    *,
    credentials: tuple[str, str] | None = None,
    authenticated: bool = True,
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
        username=username,
        password=password,
    )
    client = TestClient(app)
    if authenticated:
        encoded = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode(
            "ascii"
        )
        client.headers["Authorization"] = f"Basic {encoded}"
    return client


def test_slim_movement_serves_shared_viewer_with_slim_profile(tmp_path):
    client = create_slim_test_client(tmp_path)

    response = client.get("/", headers={"Authorization": ""})
    app_js = client.get("/static/app.js", headers={"Authorization": ""})
    login_js = client.get("/slim-static/login.js", headers={"Authorization": ""})

    assert response.status_code == 200
    assert '<meta name="vibecleaning-movement-mode" content="slim_movement">' in response.text
    assert "slim movement review" in response.text
    assert 'id="login-form"' in response.text
    assert 'id="app-shell" hidden' in response.text
    assert app_js.status_code == 200
    assert "MOVEMENT_APP_CONFIG" in app_js.text
    assert login_js.status_code == 200
    assert 'import("/static/app.js")' in login_js.text


def test_slim_auth_keeps_login_assets_public_and_protects_data_routes(tmp_path):
    password = "a-long-random-password"
    client = create_slim_test_client(
        tmp_path,
        credentials=("reviewer", password),
        authenticated=False,
    )

    for path in ("/", "/static/app.js", "/slim-static/login.js"):
        response = client.get(path)
        assert response.status_code == 200
        assert "www-authenticate" not in response.headers
        assert "set-cookie" not in response.headers

    for path in (
        "/api/auth/check",
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
        response = client.get("/api/auth/check", auth=auth)
        assert response.status_code == 401
        assert "www-authenticate" not in response.headers

    authenticated = client.get("/api/auth/check", auth=("reviewer", password))
    assert authenticated.status_code == 200
    assert authenticated.json() == {
        "authenticated": True,
        "username": "reviewer",
    }
    assert authenticated.headers["cache-control"] == "no-store"
    assert (
        client.get(
            "/api/apps/movement/families",
            auth=("reviewer", password),
        ).status_code
        == 200
    )


def test_slim_login_code_keeps_credentials_in_tab_memory_only():
    source = (
        REPO_ROOT / "examples" / "slim_movement" / "static" / "login.js"
    ).read_text(encoding="utf-8")

    for forbidden in ("localStorage", "sessionStorage", "document.cookie"):
        assert forbidden not in source
    assert 'headers.set("Authorization", authorizationHeader)' in source
    assert 'window.location.reload()' in source
    assert 'logoutButton.addEventListener("click", returnToLogin)' in source


def test_slim_startup_credentials_generate_or_accept_an_override():
    generated = startup_credentials({})
    assert generated.username == DEFAULT_USERNAME
    assert generated.generated_password is True
    assert len(generated.password) >= 12

    username_override = startup_credentials({USERNAME_ENV: "field-reviewer"})
    assert username_override.username == "field-reviewer"
    assert username_override.generated_password is True

    configured = startup_credentials(
        {
            USERNAME_ENV: "reviewer",
            PASSWORD_ENV: "a-long-random-password",
        }
    )
    assert configured.username == "reviewer"
    assert configured.password == "a-long-random-password"
    assert configured.generated_password is False


def test_slim_startup_credentials_reject_invalid_overrides():
    with pytest.raises(RuntimeError, match="at least 12 characters"):
        startup_credentials(
            {
                USERNAME_ENV: "reviewer",
                PASSWORD_ENV: "too-short",
            }
        )
    with pytest.raises(RuntimeError, match="colons or whitespace"):
        startup_credentials(
            {
                USERNAME_ENV: "review user",
                PASSWORD_ENV: "a-long-random-password",
            }
        )
    with pytest.raises(RuntimeError, match="cannot be empty"):
        startup_credentials(
            {
                USERNAME_ENV: "",
                PASSWORD_ENV: "a-long-random-password",
            }
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
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/export-reviewed-csv" in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/generate-report" in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/run-burst-anomaly-ranking" in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/run-candidate-query" not in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/run-burst-feature-space" not in paths
    assert "/api/apps/movement/family/{family_name}/study/{study_name}/actions/enrich-osm-context" not in paths


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
    assert client.get(export_path, headers={"Authorization": ""}).status_code == 401
    assert client.get(report_path, headers={"Authorization": ""}).status_code == 401


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


def test_slim_profile_does_not_load_osm_interaction_module():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'mode: MOVEMENT_APP_MODE' in source
    assert 'defaultFamily: MOVEMENT_APP_MODE === "slim_movement" ? "movement_raw"' in source
    assert 'MOVEMENT_APP_MODE === "movement"' in source
    assert '? await import("/static/osm_layer.js")' in source
    assert 'from "/static/osm_layer.js"' not in source
    assert 'this.refs.removeConfirmed' not in source
    assert '!lowerName.endsWith("_osm_context.csv")' in source
    assert '!String(field?.key || "").toLowerCase().startsWith("osm:")' in source
    assert 'this.refs.sideTabRanking.textContent = "Ranking"' in source
    assert 'element.classList.add("movement-profile-hidden")' in source
    assert 'MOVEMENT_APP_CONFIG.mode === "slim_movement" || this.uiState.showTrain !== false' in source
    assert 'MOVEMENT_APP_CONFIG.mode === "slim_movement" || this.uiState.showTest !== false' in source
    assert 'this.refs.generateReport' in source
    assert 'this.refs.exportReviewedCsv' in source
    assert 'BASEMAP_PRESETS' in source


def test_shared_individual_tab_has_persisted_vertical_resize_control():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'data-role="individual-resize"' in source
    assert 'aria-orientation="horizontal"' in source
    assert "beginIndividualPaneResize" in source
    assert "applyIndividualListHeight" in source
    assert "individualListHeightPx: this.individualListHeightPx" in source


def test_slim_ranking_layout_keeps_map_full_height_and_resizes_canvas():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'if (MOVEMENT_APP_CONFIG.mode === "slim_movement") {\n      return false;' in source
    assert "SLIM_STACKED_SIDE_LAYOUT_BREAKPOINT_PX" not in source
    assert ".movement-root:not(.is-slim) .movement-main" in source
    assert ".movement-root.is-slim .movement-main" not in source
    movement_main_css = source[
        source.index("        .movement-main {"):source.index(
            "        .movement-map-wrap,",
            source.index("        .movement-main {"),
        )
    ]
    assert "height: 100%;" in movement_main_css
    assert "min-height: 0;" in movement_main_css
    assert "overflow: hidden;" in movement_main_css
    ranking_css = source[
        source.index("        .movement-anomaly-ranking {"):source.index(
            "        .movement-anomaly-meta,",
            source.index("        .movement-anomaly-ranking {"),
        )
    ]
    assert "min-height: 0;" in ranking_css
    assert "overflow-x: auto;" in ranking_css
    assert "this.map.resize();" in source[
        source.index("  setSideSheet("):source.index("  applyAppProfile(")
    ]
