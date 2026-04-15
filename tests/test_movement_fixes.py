from pathlib import Path
import sys

from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.state import load_project_state
from app.web import create_app
from examples.movement.routes import (
    GENERATE_REPORT_SCRIPT,
    REPORT_ANALYSIS_TEMPLATE_PATH,
    register_movement_routes,
)
from examples.movement.report_analysis_template import (
    build_html_report,
    build_issue_sections,
    normalize_report_records,
)
from examples.movement.summary import build_movement_fixes, build_movement_overview

CSV_CONTENT = """eventid,individual,timestamp,longitude,latitude,set,vc_outlier_status,vc_issue_id,vc_issue_type,vc_issue_field,vc_issue_note,vc_owner_question,vc_review_user,vc_reviewed_at
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train,suspected,issue_1,drift,speed_mps,first alpha issue,question 1,reviewer,2024-01-02T00:00:00Z
fix_a_2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train,,,,,,,
fix_b_1,beta,2024-01-01T00:30:00Z,-71.0,41.0,test,confirmed,issue_2,spike,step_length_m,beta confirmed issue,question 2,reviewer,2024-01-02T00:30:00Z
fix_b_2,beta,2024-01-01T01:30:00Z,-71.1,41.1,test,suspected,issue_3,loop,hdop,beta suspected issue,question 3,reviewer,2024-01-02T01:30:00Z
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
    review_by_key = {fix["fix_key"]: fix["review"] for fix in payload["fixes"]}
    assert review_by_key["id:fix_a_1#row:1"]["issue_field"] == "speed_mps"
    assert review_by_key["id:fix_b_2#row:4"]["issue_field"] == "hdop"
    assert review_by_key["id:fix_a_1#row:1"]["issues"][0]["issue_field"] == "speed_mps"
    assert review_by_key["id:fix_b_2#row:4"]["issues"][0]["issue_type"] == "loop"


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


def test_build_movement_fixes_ignores_cleared_issue_metadata(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set,vc_outlier_status,vc_issue_id,vc_issue_type,vc_issue_refs
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train,,issue_old,drift,"[{""issue_id"": ""issue_old"", ""issue_type"": ""drift""}]"
""",
        encoding="utf-8",
    )

    payload = build_movement_fixes(csv_path)

    assert payload["returned_fix_count"] == 1
    assert "review" not in payload["fixes"][0]


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


def test_movement_report_generator_uses_compilable_template_file():
    template_text = REPORT_ANALYSIS_TEMPLATE_PATH.read_text(encoding="utf-8").strip() + "\n"

    assert GENERATE_REPORT_SCRIPT == template_text
    compile(GENERATE_REPORT_SCRIPT, str(REPORT_ANALYSIS_TEMPLATE_PATH), "exec")


def test_build_issue_sections_keeps_all_issue_types_when_snapshots_are_sampled():
    matched_records = [
        {
            "fix_key": "fix_a",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 1,
            "time_text": "2024-01-01T00:00:00Z",
            "lon": -70.0,
            "lat": 40.0,
            "step_length_m": 100.0,
            "speed_mps": 2.0,
            "time_delta_s": 50.0,
            "review": {
                "vc_outlier_status": "suspected",
                "vc_issue_id": "issue_1",
                "vc_issue_type": "spike",
                "vc_issue_note": "Spike issue",
                "vc_owner_question": "Is this a spike?",
            },
            "raw": {"hdop": "1.2"},
        },
        {
            "fix_key": "fix_b",
            "individual": "beta",
            "set_name": "train",
            "time_ms": 2,
            "time_text": "2024-01-01T01:00:00Z",
            "lon": -71.0,
            "lat": 41.0,
            "step_length_m": 150.0,
            "speed_mps": 3.0,
            "time_delta_s": 50.0,
            "review": {
                "vc_outlier_status": "suspected",
                "vc_issue_id": "issue_2",
                "vc_issue_type": "drift",
                "vc_issue_note": "Drift issue",
                "vc_owner_question": "Is this drift?",
            },
            "raw": {"hdop": "3.8"},
        },
    ]
    snapshot_windows = [
        {
            "snapshot_key": "snapshot_01",
            "caption": "spike | alpha",
            "individual": "alpha",
            "set_name": "train",
            "issue_type": "spike",
            "issue_types": ["spike"],
            "anchor_fix_keys": ["fix_a"],
            "report_fix_keys": ["fix_a"],
            "start_fix_key": "fix_a",
            "end_fix_key": "fix_a",
            "start_time_ms": 1,
            "end_time_ms": 1,
            "start_time_text": "2024-01-01T00:00:00Z",
            "end_time_text": "2024-01-01T00:00:00Z",
            "window_fix_count": 1,
        }
    ]

    sections = build_issue_sections(
        matched_records,
        snapshot_windows,
        fieldnames=["hdop", *[
            "vc_outlier_status",
            "vc_issue_id",
            "vc_issue_type",
            "vc_issue_note",
            "vc_owner_question",
        ]],
        columns={},
    )

    assert [section["issue_type"] for section in sections] == ["drift", "spike"]
    assert sections[0]["examples"]
    assert sections[1]["examples"][0]["snapshot_key"] == "snapshot_01"


def test_build_issue_sections_adds_issue_field_summary_per_individual():
    matched_records = [
        {
            "fix_key": "fix_a",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 1,
            "time_text": "2024-01-01T00:00:00Z",
            "lon": -70.0,
            "lat": 40.0,
            "step_length_m": 100.0,
            "speed_mps": 2.0,
            "time_delta_s": 50.0,
            "review": {
                "vc_outlier_status": "suspected",
                "vc_issue_id": "issue_1",
                "vc_issue_type": "speed",
                "vc_issue_field": "speed_mps",
                "vc_issue_note": "Speed issue",
                "vc_owner_question": "Is this speed plausible?",
            },
            "raw": {"hdop": "1.2"},
        },
        {
            "fix_key": "fix_b",
            "individual": "alpha",
            "set_name": "train",
            "time_ms": 2,
            "time_text": "2024-01-01T01:00:00Z",
            "lon": -70.1,
            "lat": 40.1,
            "step_length_m": 150.0,
            "speed_mps": 8.0,
            "time_delta_s": 50.0,
            "review": {
                "vc_outlier_status": "suspected",
                "vc_issue_id": "issue_1",
                "vc_issue_type": "speed",
                "vc_issue_field": "speed_mps",
                "vc_issue_note": "Speed issue",
                "vc_owner_question": "Is this speed plausible?",
            },
            "raw": {"hdop": "1.8"},
        },
    ]

    sections = build_issue_sections(
        matched_records,
        snapshot_windows=[],
        fieldnames=["hdop", "vc_issue_field"],
        columns={},
    )

    assert sections[0]["issue_field"] == "speed_mps"
    assert sections[0]["individual_rows"][0]["issue_field_summary"] == "median 5.000; range 2.000 to 8.000"


def test_normalize_report_records_ignores_cleared_issue_metadata():
    records = normalize_report_records(
        [
            {
                "fix_key": "fix_a",
                "individual": "alpha",
                "set_name": "train",
                "time_ms": 1,
                "time_text": "2024-01-01T00:00:00Z",
                "lon": -70.0,
                "lat": 40.0,
                "review": {
                    "status": "",
                    "issue_id": "issue_clear",
                    "issue_type": "drift",
                    "issues": [
                        {
                            "status": "",
                            "issue_id": "issue_clear",
                            "issue_type": "drift",
                        }
                    ],
                },
            }
        ]
    )

    assert len(records) == 1
    assert records[0]["review"]["vc_outlier_status"] == ""
    assert records[0]["review"]["vc_issue_id"] == ""
    assert records[0]["review"]["vc_issue_type"] == ""
    assert records[0]["review"]["issues"] == []


def test_html_report_generates_svg_fallback_when_auto_snapshot_is_missing():
    sections = [
        {
            "issue_type": "speed",
            "records": [
                {
                    "fix_key": "fix_a",
                    "individual": "alpha",
                    "time_ms": 1,
                    "time_text": "2024-01-01T00:00:00Z",
                    "lon": -70.0,
                    "lat": 40.0,
                    "step_length_m": 100.0,
                    "speed_mps": 2.0,
                    "review": {"vc_outlier_status": "suspected"},
                },
                {
                    "fix_key": "fix_b",
                    "individual": "alpha",
                    "time_ms": 2,
                    "time_text": "2024-01-01T01:00:00Z",
                    "lon": -70.2,
                    "lat": 40.2,
                    "step_length_m": 150.0,
                    "speed_mps": 3.0,
                    "review": {"vc_outlier_status": "suspected"},
                },
            ],
            "issue_ids": ["issue_1"],
            "issue_field": "speed_mps",
            "issue_threshold": "> 2.5",
            "issue_note": "Speed issue",
            "owner_question": "Is this speed plausible?",
            "status_counts": {"suspected": 2},
            "individual_rows": [
                {
                    "individual": "alpha",
                    "fix_count": 2,
                    "issue_ids": ["issue_1"],
                    "first_time_text": "2024-01-01T00:00:00Z",
                    "last_time_text": "2024-01-01T01:00:00Z",
                    "issue_field_summary": "median 2.500; range 2.000 to 3.000",
                    "max_step": 150.0,
                    "max_speed": 3.0,
                }
            ],
            "examples": [
                {
                    "snapshot_key": "snapshot_01",
                    "caption": "speed | alpha",
                    "individual": "alpha",
                    "set_name": "train",
                    "start_time_ms": 1,
                    "end_time_ms": 2,
                    "start_time_text": "2024-01-01T00:00:00Z",
                    "end_time_text": "2024-01-01T01:00:00Z",
                    "window_fix_count": 2,
                    "suspicious_fix_count": 2,
                    "issue_ids": ["issue_1"],
                    "status_counts": {"suspected": 2},
                    "max_step": 150.0,
                    "max_speed": 3.0,
                    "quality_lines": [],
                    "map_points": [
                        {"lon": -70.0, "lat": 40.0, "fix_key": "fix_a", "time_ms": 1},
                        {"lon": -70.2, "lat": 40.2, "fix_key": "fix_b", "time_ms": 2},
                    ],
                }
            ],
            "first_time_text": "2024-01-01T00:00:00Z",
            "last_time_text": "2024-01-01T01:00:00Z",
            "quality_fields": [],
        }
    ]

    html = build_html_report(
        "movement.csv",
        "tester",
        "auto",
        sections,
        {},
        2,
    )

    assert "data:image/svg+xml;base64," in html
    assert "No auto-rendered map snapshot included for this example." not in html
