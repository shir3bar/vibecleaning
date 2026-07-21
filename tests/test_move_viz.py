import csv
import io
from pathlib import Path
import re
import sqlite3
import sys

from fastapi.testclient import TestClient


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MOVE_VIZ_STATIC_ROOT = REPO_ROOT / "examples" / "move_viz" / "static"
SAMPLE_DATABASE = REPO_ROOT / "examples" / "move_viz" / "sample_data" / "synthetic_demo_cp2.sqlite"

from app.web import create_app
from examples.move_viz.routes import register_move_viz_routes
from app.state import get_dataset_artifact, load_dataset


def create_test_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            'CREATE TABLE movement ('
            '"event-id" TEXT, "timestamp" TEXT, "location-long" REAL, '
            '"location-lat" REAL, "individual-local-identifier" TEXT, '
            '"ground-speed" REAL, "habitat" TEXT, '
            '"manually-marked-outlier" TEXT, "algorithm-marked-outlier" TEXT)'
        )
        connection.executemany(
            'INSERT INTO movement VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [
                ("fix-1", "2024-01-01 00:00:00", -70.0, 40.0, "alpha", 1.2, "forest", "true", ""),
                ("fix-2", "2024-01-01 01:00:00", -70.1, 40.1, "alpha", 3.4, "field", "", "true"),
                ("fix-3", "2024-01-01 02:00:00", -69.9, 40.2, "beta", 0.4, "forest", "", ""),
            ],
        )
        connection.execute("CREATE TABLE notes (message TEXT)")
        connection.execute("INSERT INTO notes VALUES ('not movement data')")


def create_large_test_database(path: Path, row_count: int = 30_000) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            'CREATE TABLE movement ('
            '"event-id" TEXT, "timestamp" INTEGER, "location-long" REAL, '
            '"location-lat" REAL, "individual-local-identifier" TEXT, "habitat" TEXT)'
        )
        connection.execute(
            'WITH RECURSIVE rows(value) AS ('
            'SELECT 1 UNION ALL SELECT value + 1 FROM rows WHERE value < ?'
            ') INSERT INTO movement '
            'SELECT printf("fix-%d", value), value, -70.0 + (value % 1000) / 10000.0, '
            '40.0 + (value % 1000) / 10000.0, printf("animal-%02d", value % 30), '
            'CASE WHEN value % 2 = 0 THEN "forest" ELSE "field" END FROM rows',
            (row_count,),
        )
        connection.execute(
            'CREATE INDEX movement_individual_time '
            'ON movement ("individual-local-identifier", "timestamp")'
        )


def create_client(
    tmp_path: Path,
    *,
    max_rows: int = 100_000,
    sample_database: Path | None = None,
) -> TestClient:
    app = create_app(data_root=tmp_path / "data", static_root=MOVE_VIZ_STATIC_ROOT)
    register_move_viz_routes(
        app,
        session_root=tmp_path / "sessions",
        max_rows=max_rows,
        sample_database=sample_database,
    )
    return TestClient(app)


def upload_database(client: TestClient, path: Path) -> dict:
    response = client.post(
        f"/api/apps/move-viz/sessions?filename={path.name}",
        content=path.read_bytes(),
        headers={"Content-Type": "application/octet-stream"},
    )
    assert response.status_code == 200
    return response.json()


def test_move_viz_upload_detects_movement_table_and_columns(tmp_path):
    database = tmp_path / "movement.sqlite"
    create_test_database(database)
    client = create_client(tmp_path)

    opened = upload_database(client, database)

    assert opened["filename"] == "movement.sqlite"
    assert len(opened["fingerprint"]) == 64
    assert opened["default_table"] == "movement"
    assert opened["project_name"].startswith("move_viz_")
    assert opened["dataset_id"] == opened["graph"]["root_dataset_id"]
    assert opened["graph"]["current_dataset_id"] == opened["dataset_id"]
    assert opened["flags"] == {}
    project_dir = tmp_path / "data" / opened["project_name"]
    assert (project_dir / "source.sqlite").read_bytes() == database.read_bytes()
    root_dataset = load_dataset(project_dir, opened["dataset_id"])
    assert [item["logical_name"] for item in root_dataset["artifacts"]] == ["source.sqlite"]
    assert root_dataset["artifacts"][0]["storage_type"] == "raw"
    movement = next(table for table in opened["tables"] if table["name"] == "movement")
    notes = next(table for table in opened["tables"] if table["name"] == "notes")
    assert movement["compatible"] is True
    assert movement["detected"] == {
        "longitude": "location-long",
        "latitude": "location-lat",
        "timestamp": "timestamp",
        "individual": "individual-local-identifier",
        "event_id": "event-id",
    }
    assert notes["compatible"] is False


def test_move_viz_loads_overview_then_selected_individuals_without_writes(tmp_path):
    database = tmp_path / "movement.sqlite"
    create_test_database(database)
    original = database.read_bytes()
    client = create_client(tmp_path)
    opened = upload_database(client, database)

    response = client.post(
        f"/api/apps/move-viz/sessions/{opened['session_id']}/load",
        json={"table": "movement"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["row_count"] == 3
    assert payload["loaded_count"] == 0
    assert payload["rows"] == []
    assert payload["demand_loaded"] is True
    assert payload["individuals"] == [
        {"individual": "alpha", "row_count": 2},
        {"individual": "beta", "row_count": 1},
    ]
    kinds = {column["name"]: column["kind"] for column in payload["columns"]}
    assert kinds["ground-speed"] == "numeric"
    assert kinds["habitat"] == "categorical"

    selected = client.post(
        f"/api/apps/move-viz/sessions/{opened['session_id']}/fixes",
        json={"table": "movement", "individuals": ["alpha"]},
    )
    assert selected.status_code == 200
    payload = selected.json()
    assert payload["loaded_count"] == 2
    assert payload["matching_row_count"] == 2
    assert payload["truncated"] is False
    assert {row["individual"] for row in payload["rows"]} == {"alpha"}
    assert payload["rows"][0]["key"] == "event:fix-1#row:1"
    manual_index = payload["value_columns"].index("manually-marked-outlier")
    assert payload["rows"][0]["values"][manual_index] == "true"
    assert database.read_bytes() == original


def test_move_viz_applies_row_limit_and_rejects_non_sqlite_files(tmp_path):
    database = tmp_path / "movement.db"
    create_test_database(database)
    client = create_client(tmp_path, max_rows=2)
    opened = upload_database(client, database)

    overview = client.post(
        f"/api/apps/move-viz/sessions/{opened['session_id']}/load",
        json={"table": "movement"},
    )
    loaded = client.post(
        f"/api/apps/move-viz/sessions/{opened['session_id']}/fixes",
        json={"table": "movement", "individuals": ["alpha", "beta"]},
    )
    rejected = client.post(
        "/api/apps/move-viz/sessions?filename=not-a-database.db",
        content=b"not sqlite",
        headers={"Content-Type": "application/octet-stream"},
    )

    assert overview.status_code == 200
    assert overview.json()["loaded_count"] == 0
    assert loaded.status_code == 200
    assert loaded.json()["loaded_count"] == 2
    assert loaded.json()["matching_row_count"] == 3
    assert loaded.json()["next_offset"] == 2
    assert loaded.json()["has_more"] is True
    assert loaded.json()["truncated"] is True
    assert rejected.status_code == 400
    assert "not a SQLite database" in rejected.json()["error"]


def test_move_viz_large_table_open_is_overview_only_and_detail_is_bounded(tmp_path):
    database = tmp_path / "large.sqlite"
    create_large_test_database(database)
    client = create_client(tmp_path, max_rows=500)
    opened = upload_database(client, database)

    overview = client.post(
        f"/api/apps/move-viz/sessions/{opened['session_id']}/load",
        json={"table": "movement"},
    )
    assert overview.status_code == 200
    assert overview.json()["row_count"] == 30_000
    assert overview.json()["rows"] == []
    assert len(overview.json()["individuals"]) == 30
    assert len(overview.content) < 20_000

    detail = client.post(
        f"/api/apps/move-viz/sessions/{opened['session_id']}/fixes",
        json={"table": "movement", "individuals": ["animal-00"]},
    )
    assert detail.status_code == 200
    assert detail.json()["matching_row_count"] == 1_000
    assert detail.json()["loaded_count"] == 500
    assert detail.json()["truncated"] is True
    assert detail.json()["next_offset"] == 500
    assert detail.json()["has_more"] is True
    assert isinstance(detail.json()["rows"][0]["values"], list)

    next_page = client.post(
        f"/api/apps/move-viz/sessions/{opened['session_id']}/fixes",
        json={"table": "movement", "individuals": ["animal-00"], "offset": 500},
    )
    assert next_page.status_code == 200
    assert next_page.json()["loaded_count"] == 500
    assert next_page.json()["next_offset"] == 1_000
    assert next_page.json()["has_more"] is False
    first_keys = {row["key"] for row in detail.json()["rows"]}
    next_keys = {row["key"] for row in next_page.json()["rows"]}
    assert first_keys.isdisjoint(next_keys)

    later_page_flag = client.post(
        f"/api/apps/move-viz/sessions/{opened['session_id']}/review",
        json={
            "operation": "flag",
            "dataset_id": overview.json()["dataset_id"],
            "table": "movement",
            "row_keys": [next_page.json()["rows"][0]["key"]],
            "scope": "fix",
            "comment": "Loaded from the second page",
            "user": "reviewer",
        },
    )
    assert later_page_flag.status_code == 200
    assert next_page.json()["rows"][0]["key"] in later_page_flag.json()["flags"]


def test_move_viz_health_and_bundled_example_bypass_browser_upload(tmp_path):
    database = tmp_path / "movement.sqlite"
    create_test_database(database)
    client = create_client(tmp_path, sample_database=database)

    health = client.get("/api/apps/move-viz/health")
    opened = client.post("/api/apps/move-viz/sessions/example")

    assert health.status_code == 200
    assert health.json()["protocol"] == 5
    assert health.json()["max_rows"] == 100_000
    assert health.json()["max_review_rows"] == 250_000
    assert health.json()["sample_available"] is True
    assert opened.status_code == 200
    assert opened.json()["filename"] == "movement.sqlite"
    assert opened.json()["default_table"] == "movement"


def test_move_viz_flags_are_graph_steps_and_history_is_loadable(tmp_path):
    database = tmp_path / "movement.sqlite"
    create_test_database(database)
    original = database.read_bytes()
    client = create_client(tmp_path)
    opened = upload_database(client, database)
    session_id = opened["session_id"]
    root_dataset_id = opened["dataset_id"]

    flagged = client.post(
        f"/api/apps/move-viz/sessions/{session_id}/review",
        json={
            "operation": "flag",
            "dataset_id": root_dataset_id,
            "table": "movement",
            "individuals": ["alpha"],
            "row_keys": ["event:fix-1#row:1", "event:fix-2#row:2"],
            "scope": "segment",
            "comment": "Review this segment",
            "user": "reviewer",
        },
    )

    assert flagged.status_code == 200
    flagged_payload = flagged.json()
    flagged_dataset_id = flagged_payload["dataset_id"]
    assert flagged_dataset_id != root_dataset_id
    assert len(flagged_payload["graph"]["datasets"]) == 2
    assert len(flagged_payload["graph"]["steps"]) == 1
    assert set(flagged_payload["flags"]) == {"event:fix-1#row:1", "event:fix-2#row:2"}
    assert flagged_payload["flags"]["event:fix-1#row:1"]["scope"] == "segment"
    assert flagged_payload["flags"]["event:fix-1#row:1"]["comment"] == "Review this segment"
    assert flagged_payload["flags"]["event:fix-1#row:1"]["step_id"].startswith("step_")
    step = flagged_payload["step_result"]["step"]
    assert step["user"] == "reviewer"
    assert step["parameters"]["app"] == "move_viz"
    assert step["parameters"]["action"] == "flag"
    assert step["parameters"]["scope"] == "segment"

    project_dir = tmp_path / "data" / opened["project_name"]
    dataset = load_dataset(project_dir, flagged_dataset_id)
    artifacts = {item["logical_name"]: item for item in dataset["artifacts"]}
    assert artifacts["source.sqlite"]["storage_type"] == "raw"
    assert artifacts["move_viz_review_annotations.json"]["storage_type"] == "output"
    for record_path in (step["script_path"], step["spec_path"], step["summary_path"]):
        assert (project_dir / record_path).is_file()
    _, graph_source = get_dataset_artifact(project_dir, flagged_dataset_id, "source.sqlite")
    assert graph_source.read_bytes() == original
    assert database.read_bytes() == original

    exported = client.post(
        f"/api/apps/move-viz/sessions/{session_id}/export",
        json={
            "dataset_id": flagged_dataset_id,
            "table": "movement",
            "user": "reviewer",
        },
    )
    assert exported.status_code == 200
    export_payload = exported.json()
    analysis = export_payload["analysis_result"]["analysis"]
    assert analysis["user"] == "reviewer"
    assert analysis["parameters"]["action"] == "export_flags_csv"
    assert export_payload["download_name"] == "movement_flags.csv"
    assert len(export_payload["graph"]["datasets"]) == 2
    assert len(export_payload["analyses"]) == 1
    for record_path in (analysis["script_path"], analysis["spec_path"], analysis["summary_path"]):
        assert (project_dir / record_path).is_file()
    download = client.get(export_payload["download_url"])
    assert download.status_code == 200
    exported_rows = list(csv.DictReader(io.StringIO(download.text)))
    assert len(exported_rows) == 2
    by_event = {row["event_id"]: row for row in exported_rows}
    assert by_event["fix-1"]["selection_scope"] == "segment"
    assert by_event["fix-1"]["manually-marked-outlier"] == "true"
    assert by_event["fix-1"]["flag_step_id"] == step["step_id"]
    assert "Review this segment" in by_event["fix-1"]["outlier_comments"]
    assert "Already flagged in source: manually-marked-outlier=true" in by_event["fix-1"]["outlier_comments"]
    assert "Already flagged in source: algorithm-marked-outlier=true" in by_event["fix-2"]["outlier_comments"]

    reopened = upload_database(client, database)
    assert reopened["project_name"] == opened["project_name"]
    assert reopened["dataset_id"] == flagged_dataset_id
    assert set(reopened["flags"]) == {"event:fix-1#row:1", "event:fix-2#row:2"}

    unflagged = client.post(
        f"/api/apps/move-viz/sessions/{reopened['session_id']}/review",
        json={
            "operation": "unflag",
            "dataset_id": reopened["dataset_id"],
            "table": "movement",
            "individuals": ["alpha"],
            "row_keys": ["event:fix-1#row:1"],
            "scope": "fix",
            "user": "reviewer",
        },
    )
    assert unflagged.status_code == 200
    assert set(unflagged.json()["flags"]) == {"event:fix-2#row:2"}
    assert len(unflagged.json()["graph"]["steps"]) == 2

    undone = client.post(
        f"/api/apps/move-viz/sessions/{reopened['session_id']}/undo",
        json={"table": "movement"},
    )
    assert undone.status_code == 200
    assert undone.json()["dataset_id"] == flagged_dataset_id
    assert set(undone.json()["flags"]) == {"event:fix-1#row:1", "event:fix-2#row:2"}

    initial = client.post(
        f"/api/apps/move-viz/sessions/{reopened['session_id']}/head",
        json={"table": "movement", "dataset_id": root_dataset_id},
    )
    assert initial.status_code == 200
    assert initial.json()["dataset_id"] == root_dataset_id
    assert initial.json()["flags"] == {}


def test_move_viz_frontend_is_direct_and_lightweight(tmp_path):
    client = create_client(tmp_path)

    index = client.get("/")
    source = client.get("/static/app.js")

    assert index.status_code == 200
    assert "move_viz" in index.text
    assert source.status_code == 200
    assert "Browse SQLite" in source.text
    assert 'type="file"' in source.text
    assert "XMLHttpRequest" in source.text
    assert "Checking move_viz server · protocol ${MOVE_VIZ_PROTOCOL}…" in source.text
    assert "Checking SQLite header…" in source.text
    assert "file.slice(0, 16).arrayBuffer()" in source.text
    assert "request.send(file)" in source.text
    assert "Uploading database… 0%" in source.text
    assert "Uploading database… ${percent}%" in source.text
    assert "Inspecting SQLite tables…" in source.text
    assert "The SQLite upload timed out after two minutes." in source.text
    assert "/api/apps/move-viz/sessions/example" in source.text
    assert "/api/apps/move-viz/sessions" in source.text
    assert "Color by" in source.text
    assert "Export flags CSV" in source.text
    assert "anomaly ranking" not in source.text.lower()
    assert "/api/projects" not in source.text
    assert "new graph step" in source.text
    assert "/review" in source.text
    assert "/undo" in source.text
    assert "/head" in source.text
    assert "/export" in source.text
    assert "/fixes" in source.text
    assert "No fixes are loaded into the map until you choose them" in source.text
    assert "Load more fixes" in source.text
    assert "append: true" in source.text
    assert "next_offset" in source.text
    assert "an entire-individual flag would be incomplete" in source.text
    assert "flagStorageKey" not in source.text
    assert "saveFlags" not in source.text
    assert "Reviewer name" in source.text
    assert "Data graph · initial dataset" in source.text


def test_move_viz_frontend_references_every_declared_role_consistently():
    source = (MOVE_VIZ_STATIC_ROOT / "app.js").read_text(encoding="utf-8")
    declared_roles = set(re.findall(r'data-role="([a-z0-9-]+)"', source))
    reference_keys = set(re.findall(r"this\.refs\.([A-Za-z][A-Za-z0-9]*)", source))
    declared_keys = {
        re.sub(r"-([a-z])", lambda match: match.group(1).upper(), role)
        for role in declared_roles
    }

    assert "fileMeta" in declared_keys
    assert "tableWrap" in declared_keys
    assert "clearSelection" in declared_keys
    assert reference_keys <= declared_keys
    assert "roleReferenceKey(element.dataset.role)" in source


def test_move_viz_rebuilds_overlays_after_atomic_basemap_change():
    source = (MOVE_VIZ_STATIC_ROOT / "app.js").read_text(encoding="utf-8")

    listener_position = source.index('this.map.once("style.load", resolve)')
    set_style_position = source.index('this.map.setStyle(style, { diff: false })')
    render_position = source.index("this.renderData();", set_style_position)

    assert listener_position < set_style_position < render_position
    assert "const changeId = ++this.styleChangeId" in source
    assert "if (changeId !== this.styleChangeId) return" in source
    assert 'this.map.getSource("move-viz-tracks")' in source
    assert 'this.map.getSource("move-viz-points")' in source


def test_move_viz_supports_borderless_fixes_and_compact_scope_selection():
    source = (MOVE_VIZ_STATIC_ROOT / "app.js").read_text(encoding="utf-8")

    assert '<option value="fix">Single fixes</option>' in source
    assert '<option value="segment">Track segment (2 clicks)</option>' in source
    assert '<option value="individual">Entire individual</option>' in source
    assert "selectSegmentEndpoint(row)" in source
    assert "candidate.individual === row.individual" in source
    assert "track.slice(start, end + 1)" in source
    assert 'borderColor: selected ? "#7dd3fc"' in source
    assert 'sourceFlagged ? "#fbbf24" : "rgba(0,0,0,0)"' in source
    assert '"circle-stroke-width": ["get", "borderWidth"]' in source
    assert 'scope: this.selectionMode' in source


def test_committed_sample_database_matches_raw_demo_shape():
    assert SAMPLE_DATABASE.exists()
    assert SAMPLE_DATABASE.read_bytes().startswith(b"SQLite format 3\x00")
    with sqlite3.connect(SAMPLE_DATABASE) as connection:
        row_count = connection.execute("SELECT COUNT(*) FROM movement").fetchone()[0]
        columns = [row[1] for row in connection.execute("PRAGMA table_info(movement)")]

    assert row_count == 4_800
    assert "location-long" in columns
    assert "location-lat" in columns
    assert "individual-local-identifier" in columns
    assert "synthetic:severity" in columns
