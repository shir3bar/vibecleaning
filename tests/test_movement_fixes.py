from pathlib import Path
import sys

from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.state import load_project_state
from app.web import create_app
from examples.movement.routes import register_movement_routes
from examples.movement.summary import build_movement_fixes, build_movement_overview

CSV_CONTENT = """eventid,individual,timestamp,longitude,latitude,set,vc_outlier_status,vc_issue_id,vc_issue_type,vc_issue_note,vc_owner_question,vc_review_user,vc_reviewed_at
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train,suspected,issue_1,drift,first alpha issue,question 1,reviewer,2024-01-02T00:00:00Z
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train,,,,,,,
fix_b_1,beta,2024-01-01T00:30:00Z,-71.0,41.0,test,confirmed,issue_2,spike,beta confirmed issue,question 2,reviewer,2024-01-02T00:30:00Z
fix_b_2,beta,2024-01-01T01:30:00Z,-71.1,41.1,test,suspected,issue_3,loop,beta suspected issue,question 3,reviewer,2024-01-02T01:30:00Z
fix_c_1,gamma,2024-01-01T00:45:00Z,-72.0,42.0,train,,,,,,,
"""


def write_movement_csv(path: Path) -> Path:
    path.write_text(CSV_CONTENT, encoding="utf-8")
    return path


def test_build_movement_fixes_filters_multiple_individuals(tmp_path):
    csv_path = write_movement_csv(tmp_path / "movement.csv")

    payload = build_movement_fixes(csv_path, individuals=["beta", "alpha"])

    assert payload["detail_scope"]["individuals"] == ["alpha", "beta"]
    assert payload["detail_scope"]["individual"] == ""
    assert payload["returned_fix_count"] == 4
    assert {fix["individual"] for fix in payload["fixes"]} == {"alpha", "beta"}


def test_build_movement_fixes_supports_single_individual_and_truncation(tmp_path):
    csv_path = write_movement_csv(tmp_path / "movement.csv")

    payload = build_movement_fixes(csv_path, individual="beta", limit=1)

    assert payload["detail_scope"]["individual"] == "beta"
    assert payload["detail_scope"]["individuals"] == ["beta"]
    assert payload["matching_fix_count"] == 2
    assert payload["returned_fix_count"] == 1
    assert payload["truncated"] is True
    assert {fix["individual"] for fix in payload["fixes"]} == {"beta"}


def test_build_movement_fixes_loads_all_individuals_without_filter(tmp_path):
    csv_path = write_movement_csv(tmp_path / "movement.csv")

    payload = build_movement_fixes(csv_path)

    assert payload["detail_scope"]["individual"] == ""
    assert payload["detail_scope"]["individuals"] == []
    assert payload["returned_fix_count"] == 5
    assert {fix["individual"] for fix in payload["fixes"]} == {"alpha", "beta", "gamma"}


def test_build_movement_overview_includes_fix_points_not_just_reviewed_rows(tmp_path):
    csv_path = write_movement_csv(tmp_path / "movement.csv")

    payload = build_movement_overview(csv_path)

    assert payload["detail_loaded"] is False
    assert len(payload["fixes"]) == 5
    assert {fix["individual"] for fix in payload["fixes"]} == {"alpha", "beta", "gamma"}


def create_movement_test_client(tmp_path: Path) -> tuple[TestClient, str]:
    data_root = tmp_path / "data"
    study_dir = data_root / "movement_clean" / "test_study"
    study_dir.mkdir(parents=True)
    write_movement_csv(study_dir / "movement.csv")

    app = create_app(
        data_root=data_root,
        static_root=REPO_ROOT / "examples" / "movement" / "static",
    )
    register_movement_routes(app, data_root=data_root)

    dataset_id = load_project_state(study_dir)["current_dataset_id"]
    client = TestClient(app)
    return client, dataset_id


def test_movement_fixes_route_accepts_repeated_individuals(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params=[
            ("logical_name", "movement.csv"),
            ("individuals", "beta"),
            ("individuals", "alpha"),
        ],
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["detail_scope"]["individuals"] == ["alpha", "beta"]
    assert {fix["individual"] for fix in payload["fixes"]} == {"alpha", "beta"}


def test_movement_fixes_route_supports_legacy_individual_query(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params={"logical_name": "movement.csv", "individual": "beta"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["detail_scope"]["individual"] == "beta"
    assert payload["detail_scope"]["individuals"] == ["beta"]
    assert {fix["individual"] for fix in payload["fixes"]} == {"beta"}


def test_movement_fixes_route_rejects_invalid_repeated_individual(tmp_path):
    client, dataset_id = create_movement_test_client(tmp_path)

    response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{dataset_id}/fixes",
        params=[("logical_name", "movement.csv"), ("individuals", "bad\x01value")],
    )

    assert response.status_code == 404
    assert response.json()["error"] == "Invalid individual"
