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
    PASSWORD_ENV,
    USERNAME_ENV,
    credentials_from_environment,
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

    response = client.get("/")
    app_js = client.get("/static/app.js")

    assert response.status_code == 200
    assert '<meta name="vibecleaning-movement-mode" content="slim_movement">' in response.text
    assert "slim movement review" in response.text
    assert app_js.status_code == 200
    assert "MOVEMENT_APP_CONFIG" in app_js.text


def test_slim_auth_protects_page_static_assets_and_api(tmp_path):
    password = "a-long-random-password"
    client = create_slim_test_client(
        tmp_path,
        credentials=("reviewer", password),
        authenticated=False,
    )

    for path in (
        "/",
        "/static/app.js",
        "/api/apps/movement/families",
    ):
        response = client.get(path)
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == (
            'Basic realm="slim_movement", charset="UTF-8"'
        )
        assert response.headers["cache-control"] == "no-store"

    assert client.get("/", auth=("reviewer", "wrong-password")).status_code == 401
    assert client.get("/", auth=("wrong-user", password)).status_code == 401
    assert client.get("/", auth=("reviewer", password)).status_code == 200
    assert (
        client.get(
            "/api/apps/movement/families",
            auth=("reviewer", password),
        ).status_code
        == 200
    )


def test_slim_auth_credentials_fail_closed():
    with pytest.raises(RuntimeError, match=f"{USERNAME_ENV} and {PASSWORD_ENV}"):
        credentials_from_environment({})
    with pytest.raises(RuntimeError, match="at least 12 characters"):
        credentials_from_environment(
            {
                USERNAME_ENV: "reviewer",
                PASSWORD_ENV: "too-short",
            }
        )
    with pytest.raises(RuntimeError, match="colons or whitespace"):
        credentials_from_environment(
            {
                USERNAME_ENV: "review user",
                PASSWORD_ENV: "a-long-random-password",
            }
        )

    assert credentials_from_environment(
        {
            USERNAME_ENV: "reviewer",
            PASSWORD_ENV: "a-long-random-password",
        }
    ) == ("reviewer", "a-long-random-password")


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
    realized = {item["logical_name"] for item in report.json()["analysis"]["realized_output_artifacts"]}
    assert "movement_individual_reports.html" in realized


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
