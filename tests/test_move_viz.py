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


def test_move_viz_loads_rows_color_fields_and_existing_flags_without_writes(tmp_path):
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
    assert payload["loaded_count"] == 3
    assert payload["truncated"] is False
    assert {row["individual"] for row in payload["rows"]} == {"alpha", "beta"}
    assert payload["rows"][0]["key"] == "event:fix-1#row:1"
    assert payload["rows"][0]["values"]["manually-marked-outlier"] == "true"
    kinds = {column["name"]: column["kind"] for column in payload["columns"]}
    assert kinds["ground-speed"] == "numeric"
    assert kinds["habitat"] == "categorical"
    assert database.read_bytes() == original


def test_move_viz_applies_row_limit_and_rejects_non_sqlite_files(tmp_path):
    database = tmp_path / "movement.db"
    create_test_database(database)
    client = create_client(tmp_path, max_rows=2)
    opened = upload_database(client, database)

    loaded = client.post(
        f"/api/apps/move-viz/sessions/{opened['session_id']}/load",
        json={"table": "movement"},
    )
    rejected = client.post(
        "/api/apps/move-viz/sessions?filename=not-a-database.db",
        content=b"not sqlite",
        headers={"Content-Type": "application/octet-stream"},
    )

    assert loaded.status_code == 200
    assert loaded.json()["loaded_count"] == 2
    assert loaded.json()["truncated"] is True
    assert rejected.status_code == 400
    assert "not a SQLite database" in rejected.json()["error"]


def test_move_viz_health_and_bundled_example_bypass_browser_upload(tmp_path):
    database = tmp_path / "movement.sqlite"
    create_test_database(database)
    client = create_client(tmp_path, sample_database=database)

    health = client.get("/api/apps/move-viz/health")
    opened = client.post("/api/apps/move-viz/sessions/example")

    assert health.status_code == 200
    assert health.json()["protocol"] == 2
    assert health.json()["sample_available"] is True
    assert opened.status_code == 200
    assert opened.json()["filename"] == "movement.sqlite"
    assert opened.json()["default_table"] == "movement"


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
    assert "Reading selected file… 0%" in source.text
    assert "Uploading database… 0%" in source.text
    assert "Uploading database… ${percent}%" in source.text
    assert "Inspecting SQLite tables…" in source.text
    assert "The SQLite upload timed out after two minutes." in source.text
    assert "/api/apps/move-viz/sessions/example" in source.text
    assert "/api/apps/move-viz/sessions" in source.text
    assert "Color by" in source.text
    assert "Export flags CSV" in source.text
    assert "Already flagged in source: manually-marked-outlier=true" in source.text
    assert "Already flagged in source: algorithm-marked-outlier=true" in source.text
    assert "anomaly ranking" not in source.text.lower()
    assert "/api/projects" not in source.text


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
