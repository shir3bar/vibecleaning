from __future__ import annotations

from contextlib import closing
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
from urllib.parse import quote
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from app.execution import create_analysis, create_step, set_current_head, undo_to_parent
from app.state import (
    ProjectStateError,
    get_dataset_artifact,
    graph_payload,
    load_dataset,
    load_json,
    project_state_payload,
)


SQLITE_HEADER = b"SQLite format 3\x00"
DEFAULT_MAX_UPLOAD_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_ROWS = 100_000
DEFAULT_MAX_REVIEW_ROWS = 250_000
NUMERIC_DECLARATIONS = ("INT", "REAL", "FLOA", "DOUB", "NUM", "DEC")
MOVE_VIZ_PROTOCOL = 6
SOURCE_ARTIFACT = "source.sqlite"
REVIEW_ARTIFACT = "move_viz_review_annotations.json"
REVIEW_STEP_SCRIPT = Path(__file__).with_name("review_step.py").read_text(encoding="utf-8")
compile(REVIEW_STEP_SCRIPT, str(Path(__file__).with_name("review_step.py")), "exec")
EXPORT_FLAGS_SCRIPT = Path(__file__).with_name("export_flags_analysis.py").read_text(encoding="utf-8")
compile(EXPORT_FLAGS_SCRIPT, str(Path(__file__).with_name("export_flags_analysis.py")), "exec")
EXPORT_FLAGS_ARTIFACT = "move_viz_flags.csv"

COLUMN_ALIASES = {
    "longitude": ("location-long", "longitude", "lon", "lng", "long", "x"),
    "latitude": ("location-lat", "latitude", "lat", "y"),
    "timestamp": ("timestamp", "time", "datetime", "date_time", "recorded_at"),
    "individual": (
        "individual-local-identifier",
        "individual-id",
        "individual",
        "individual_id",
        "track_id",
        "animal_id",
        "tag-local-identifier",
    ),
    "event_id": ("event-id", "eventid", "event_id", "id", "fix_id"),
}


def _json_error(message: str, status_code: int) -> JSONResponse:
    return JSONResponse({"error": message}, status_code=status_code)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _readonly_connection(path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(path.resolve()))}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def _normalized_column_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")


def _detect_column(columns: list[dict], role: str) -> str | None:
    by_normalized = {
        _normalized_column_name(str(column["name"])): str(column["name"])
        for column in columns
    }
    for alias in COLUMN_ALIASES[role]:
        match = by_normalized.get(_normalized_column_name(alias))
        if match:
            return match
    return None


def _column_kind(connection: sqlite3.Connection, table_name: str, column: dict) -> str:
    declared_type = str(column.get("type") or "").upper()
    if any(token in declared_type for token in NUMERIC_DECLARATIONS):
        return "numeric"
    identifier = _quoted_identifier(str(column["name"]))
    table = _quoted_identifier(table_name)
    values = [
        row[0]
        for row in connection.execute(
            f"SELECT {identifier} FROM {table} WHERE {identifier} IS NOT NULL LIMIT 40"
        )
    ]
    if not values:
        return "categorical"
    try:
        for value in values:
            float(value)
    except (TypeError, ValueError):
        return "categorical"
    return "numeric"


def inspect_sqlite(path: Path) -> list[dict]:
    with closing(_readonly_connection(path)) as connection:
        table_names = [
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        tables = []
        for table_name in table_names:
            quoted_table = _quoted_identifier(table_name)
            columns = [
                {
                    "name": str(row[1]),
                    "type": str(row[2] or ""),
                    "nullable": not bool(row[3]),
                    "primary_key": bool(row[5]),
                }
                for row in connection.execute(f"PRAGMA table_info({quoted_table})")
            ]
            mappings = {
                role: _detect_column(columns, role)
                for role in COLUMN_ALIASES
            }
            row_count = int(connection.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0])
            tables.append(
                {
                    "name": table_name,
                    "row_count": row_count,
                    "compatible": bool(mappings["longitude"] and mappings["latitude"]),
                    "columns": columns,
                    "detected": mappings,
                }
            )
        return tables


def _json_value(value: object) -> object:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _movement_row_key(event_value: object, row_number: object) -> str:
    return (
        f"event:{event_value}#row:{row_number}"
        if event_value not in (None, "")
        else f"row:{row_number}"
    )


def _row_number_from_key(row_key: str) -> int:
    suffix = row_key.rsplit("#row:", 1)[-1] if "#row:" in row_key else row_key.removeprefix("row:")
    try:
        value = int(suffix)
    except ValueError as exc:
        raise ValueError("Invalid selected fix key") from exc
    if value < 1:
        raise ValueError("Invalid selected fix key")
    return value


def _validated_mapping(table: dict, requested: dict) -> dict[str, str | None]:
    columns = {str(column["name"]) for column in table["columns"]}
    detected = dict(table["detected"])
    mapping: dict[str, str | None] = {}
    for role in COLUMN_ALIASES:
        raw_value = requested.get(role, detected.get(role))
        value = str(raw_value).strip() if raw_value is not None else ""
        if value and value not in columns:
            raise ValueError(f"Unknown {role} column: {value}")
        mapping[role] = value or None
    if not mapping["longitude"] or not mapping["latitude"]:
        raise ValueError("Select longitude and latitude columns")
    return mapping


def _movement_table_context(path: Path, payload: dict) -> tuple[dict, dict[str, str | None]]:
    tables = inspect_sqlite(path)
    compatible = [table for table in tables if table["compatible"]]
    requested_table = str(payload.get("table") or "").strip()
    table = next((item for item in tables if item["name"] == requested_table), None)
    if table is None and not requested_table and compatible:
        table = compatible[0]
    if table is None:
        raise ValueError("Select a movement-compatible table")
    mapping = _validated_mapping(table, payload)
    return table, mapping


def _color_columns(connection: sqlite3.Connection, table: dict, mapping: dict) -> list[dict]:
    color_columns = []
    for column in table["columns"]:
        name = str(column["name"])
        if name in {mapping["longitude"], mapping["latitude"]}:
            continue
        color_columns.append(
            {
                "name": name,
                "kind": _column_kind(connection, str(table["name"]), column),
            }
        )
    return color_columns


def load_movement_overview(path: Path, payload: dict, *, max_rows: int) -> dict:
    table, mapping = _movement_table_context(path, payload)
    table_identifier = _quoted_identifier(str(table["name"]))
    individual_column = mapping["individual"]
    with closing(_readonly_connection(path)) as connection:
        color_columns = _color_columns(connection, table, mapping)
        if individual_column:
            individual_identifier = _quoted_identifier(str(individual_column))
            individual_rows = connection.execute(
                f"SELECT {individual_identifier}, COUNT(*) FROM {table_identifier} "
                f"GROUP BY {individual_identifier} ORDER BY {individual_identifier}"
            ).fetchall()
            counts: dict[str, int] = {}
            for row in individual_rows:
                individual = str(row[0]) if row[0] not in (None, "") else "All fixes"
                counts[individual] = counts.get(individual, 0) + int(row[1])
            individuals = [
                {"individual": individual, "row_count": count}
                for individual, count in sorted(counts.items())
            ]
        else:
            individuals = [{"individual": "All fixes", "row_count": int(table["row_count"])}]

    return {
        "table": table["name"],
        "row_count": table["row_count"],
        "loaded_count": 0,
        "matching_row_count": 0,
        "skipped_count": 0,
        "truncated": False,
        "max_rows": max_rows,
        "mapping": mapping,
        "columns": color_columns,
        "individuals": individuals,
        "loaded_individuals": [],
        "rows": [],
        "demand_loaded": True,
    }


def load_movement_table(path: Path, payload: dict, *, max_rows: int) -> dict:
    table, mapping = _movement_table_context(path, payload)

    table_identifier = _quoted_identifier(str(table["name"]))
    timestamp_column = mapping["timestamp"]
    order_clause = f" ORDER BY {_quoted_identifier(timestamp_column)}" if timestamp_column else ""
    rowid_order_clause = (
        f" ORDER BY {_quoted_identifier(timestamp_column)}, rowid"
        if timestamp_column
        else " ORDER BY rowid"
    )
    raw_individuals = payload.get("individuals")
    if raw_individuals is None:
        individuals = []
    elif not isinstance(raw_individuals, list):
        raise ValueError("Invalid individual selection")
    else:
        individuals = sorted({str(value) for value in raw_individuals if str(value)})
    if len(individuals) > 10_000:
        raise ValueError("Too many selected individuals")
    try:
        offset = int(payload.get("offset") or 0)
        requested_limit = int(payload.get("limit") or max_rows)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid detail page") from exc
    if offset < 0 or requested_limit < 1:
        raise ValueError("Invalid detail page")
    page_limit = min(requested_limit, max_rows)
    where_clause = ""
    query_parameters: list[object] = []
    if individuals and mapping["individual"]:
        placeholders = ", ".join("?" for _ in individuals)
        individual_identifier = _quoted_identifier(str(mapping["individual"]))
        conditions = [f"{individual_identifier} IN ({placeholders})"]
        if "All fixes" in individuals:
            conditions.append(f"{individual_identifier} IS NULL")
            conditions.append(f"CAST({individual_identifier} AS TEXT) = ''")
        where_clause = f" WHERE ({' OR '.join(conditions)})"
        query_parameters.extend(individuals)
    query_with_rowid = (
        f'SELECT rowid AS "__move_viz_rowid__", * FROM {table_identifier}'
        f"{where_clause}{rowid_order_clause} LIMIT ? OFFSET ?"
    )
    query_without_rowid = (
        f"SELECT * FROM {table_identifier}{where_clause}{order_clause} LIMIT ? OFFSET ?"
    )

    with closing(_readonly_connection(path)) as connection:
        matching_row_count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {table_identifier}{where_clause}",
                query_parameters,
            ).fetchone()[0]
        )
        limited_parameters = [*query_parameters, page_limit, offset]
        try:
            records = connection.execute(query_with_rowid, limited_parameters).fetchall()
            has_rowid = True
        except sqlite3.OperationalError:
            records = connection.execute(query_without_rowid, limited_parameters).fetchall()
            has_rowid = False

    longitude_column = str(mapping["longitude"])
    latitude_column = str(mapping["latitude"])
    value_columns = [str(column["name"]) for column in table["columns"]]
    rows = []
    skipped_rows = 0
    for index, record in enumerate(records):
        try:
            longitude = float(record[longitude_column])
            latitude = float(record[latitude_column])
        except (TypeError, ValueError):
            skipped_rows += 1
            continue
        if not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
            skipped_rows += 1
            continue
        values = [_json_value(record[column]) for column in value_columns]
        event_value = record[str(mapping["event_id"])] if mapping["event_id"] else None
        rowid_value = record["__move_viz_rowid__"] if has_rowid else offset + index + 1
        rows.append(
            {
                "key": _movement_row_key(event_value, rowid_value),
                "rowid": rowid_value,
                "longitude": longitude,
                "latitude": latitude,
                "timestamp": _json_value(record[str(timestamp_column)]) if timestamp_column else None,
                "individual": str(record[str(mapping["individual"])])
                if mapping["individual"] and record[str(mapping["individual"])] not in (None, "")
                else "All fixes",
                "values": values,
            }
        )

    return {
        "table": table["name"],
        "row_count": table["row_count"],
        "loaded_count": len(rows),
        "returned_row_count": len(records),
        "skipped_count": skipped_rows,
        "matching_row_count": matching_row_count,
        "offset": offset,
        "next_offset": offset + len(records),
        "has_more": offset + len(records) < matching_row_count,
        "truncated": offset + len(records) < matching_row_count,
        "max_rows": max_rows,
        "mapping": mapping,
        "value_columns": value_columns,
        "loaded_individuals": individuals,
        "rows": rows,
    }


def validate_movement_row_keys(path: Path, payload: dict, row_keys: list[str]) -> None:
    table, mapping = _movement_table_context(path, payload)
    table_identifier = _quoted_identifier(str(table["name"]))
    event_column = mapping["event_id"]
    event_expression = _quoted_identifier(str(event_column)) if event_column else "NULL"
    requested = set(row_keys)
    row_numbers = {_row_number_from_key(row_key) for row_key in requested}
    available: set[str] = set()
    with closing(_readonly_connection(path)) as connection:
        try:
            connection.execute(f"SELECT rowid FROM {table_identifier} LIMIT 0")
            has_rowid = True
        except sqlite3.OperationalError:
            has_rowid = False
        if has_rowid:
            sorted_rowids = sorted(row_numbers)
            for offset in range(0, len(sorted_rowids), 500):
                chunk = sorted_rowids[offset : offset + 500]
                placeholders = ", ".join("?" for _ in chunk)
                records = connection.execute(
                    f'SELECT rowid AS "__move_viz_rowid__", {event_expression} AS "__move_viz_event__" '
                    f"FROM {table_identifier} WHERE rowid IN ({placeholders})",
                    chunk,
                )
                for record in records:
                    available.add(
                        _movement_row_key(record["__move_viz_event__"], record["__move_viz_rowid__"])
                    )
        else:
            timestamp_column = mapping["timestamp"]
            order_clause = f" ORDER BY {_quoted_identifier(str(timestamp_column))}" if timestamp_column else ""
            maximum = max(row_numbers)
            records = connection.execute(
                f'SELECT {event_expression} AS "__move_viz_event__" '
                f"FROM {table_identifier}{order_clause} LIMIT ?",
                (maximum,),
            )
            for index, record in enumerate(records, start=1):
                if index in row_numbers:
                    available.add(_movement_row_key(record["__move_viz_event__"], index))
    if requested - available:
        raise ValueError("Some selected fixes are not present in the loaded SQLite table")


def register_move_viz_routes(
    app: FastAPI,
    *,
    session_root: Path | None = None,
    max_upload_bytes: int | None = None,
    max_rows: int | None = None,
    sample_database: Path | None = None,
) -> None:
    data_root = Path(app.state.data_root).resolve()
    data_root.mkdir(parents=True, exist_ok=True)
    owned_session_root = None
    if session_root is None:
        owned_session_root = tempfile.TemporaryDirectory(prefix="vibecleaning-move-viz-")
        session_root = Path(owned_session_root.name)
    session_root = session_root.resolve()
    session_root.mkdir(parents=True, exist_ok=True)
    app.state.move_viz_session_root = session_root
    app.state.move_viz_temporary_directory = owned_session_root
    upload_limit = max_upload_bytes or int(os.environ.get("MOVE_VIZ_MAX_UPLOAD_BYTES", DEFAULT_MAX_UPLOAD_BYTES))
    row_limit = max_rows or int(os.environ.get("MOVE_VIZ_MAX_ROWS", DEFAULT_MAX_ROWS))
    review_row_limit = int(os.environ.get("MOVE_VIZ_MAX_REVIEW_ROWS", DEFAULT_MAX_REVIEW_ROWS))
    resolved_sample_database = sample_database.resolve() if sample_database else None

    def project_dir_for_name(project_name: str) -> Path:
        if not re.fullmatch(r"move_viz_[a-f0-9]{16}", project_name):
            raise ValueError("Unknown move_viz graph")
        project_dir = (data_root / project_name).resolve()
        if data_root not in project_dir.parents:
            raise ValueError("Unknown move_viz graph")
        if not project_dir.is_dir():
            raise ValueError("Unknown move_viz graph")
        return project_dir

    def ensure_graph_project(
        source_path: Path,
        fingerprint: str,
        *,
        adopt_source: bool,
    ) -> tuple[str, Path, Path]:
        project_name = f"move_viz_{fingerprint[:16]}"
        project_dir = data_root / project_name
        project_dir.mkdir(parents=True, exist_ok=True)
        graph_source = project_dir / SOURCE_ARTIFACT
        if graph_source.exists():
            if _file_sha256(graph_source) != fingerprint:
                raise ValueError("The existing move_viz graph has a different source fingerprint")
            if adopt_source:
                source_path.unlink(missing_ok=True)
        else:
            if adopt_source:
                source_path.replace(graph_source)
            else:
                shutil.copyfile(source_path, graph_source)
        project_state_payload(project_dir)
        return project_name, project_dir, graph_source

    def session_record(session_id: str) -> dict:
        if not re.fullmatch(r"[a-f0-9]{32}", session_id):
            raise ValueError("Unknown SQLite session")
        record_path = session_root / f"{session_id}.json"
        if not record_path.is_file():
            raise ValueError("Unknown SQLite session")
        try:
            record = load_json(record_path)
        except ProjectStateError as exc:
            raise ValueError("Unknown SQLite session") from exc
        project_dir = project_dir_for_name(str(record.get("project_name") or ""))
        if not (project_dir / SOURCE_ARTIFACT).is_file():
            raise ValueError("Unknown SQLite session")
        return record

    def session_path(session_id: str) -> Path:
        record = session_record(session_id)
        project_dir = project_dir_for_name(str(record["project_name"]))
        return project_dir / SOURCE_ARTIFACT

    def flags_for_dataset(project_dir: Path, dataset_id: str, table_name: str) -> dict:
        dataset = load_dataset(project_dir, dataset_id)
        if not any(item.get("logical_name") == REVIEW_ARTIFACT for item in dataset.get("artifacts", [])):
            return {}
        _, artifact_path = get_dataset_artifact(project_dir, dataset_id, REVIEW_ARTIFACT)
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("move_viz review annotations are invalid") from exc
        tables = payload.get("tables") if isinstance(payload, dict) else None
        table = tables.get(table_name) if isinstance(tables, dict) else None
        flags = table.get("flags") if isinstance(table, dict) else None
        return {
            str(key): dict(value)
            for key, value in (flags.items() if isinstance(flags, dict) else [])
            if isinstance(value, dict)
        }

    def review_state(project_dir: Path, table_name: str) -> dict:
        state = project_state_payload(project_dir)
        dataset_id = str(state["project"]["current_dataset_id"])
        return {
            "project_name": project_dir.name,
            "dataset_id": dataset_id,
            "graph": graph_payload(project_dir),
            "flags": flags_for_dataset(project_dir, dataset_id, table_name),
            "analyses": state["history"]["analyses"],
        }

    def session_payload(
        source_path: Path,
        session_id: str,
        filename: str,
        fingerprint: str,
        *,
        adopt_source: bool,
    ) -> dict:
        project_name, project_dir, graph_source = ensure_graph_project(
            source_path,
            fingerprint,
            adopt_source=adopt_source,
        )
        tables = inspect_sqlite(graph_source)
        compatible = [table for table in tables if table["compatible"]]
        default_table = compatible[0]["name"] if compatible else ""
        current_review = review_state(project_dir, default_table)
        record = {
            "session_id": session_id,
            "filename": filename,
            "fingerprint": fingerprint,
            "project_name": project_name,
            "default_table": default_table,
        }
        (session_root / f"{session_id}.json").write_text(
            json.dumps(record, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        payload = {
            "session_id": session_id,
            "filename": filename,
            "size": graph_source.stat().st_size,
            "fingerprint": fingerprint,
            "tables": tables,
            "default_table": default_table,
        }
        payload.update(current_review)
        return payload

    @app.get("/api/apps/move-viz/health")
    async def move_viz_health():
        return JSONResponse(
            {
                "status": "ok",
                "protocol": MOVE_VIZ_PROTOCOL,
                "max_upload_bytes": upload_limit,
                "max_rows": row_limit,
                "max_review_rows": review_row_limit,
                "sample_available": bool(resolved_sample_database and resolved_sample_database.is_file()),
            }
        )

    @app.post("/api/apps/move-viz/sessions")
    async def create_session(request: Request, filename: str = "movement.sqlite"):
        safe_filename = Path(filename).name.strip() or "movement.sqlite"
        session_id = uuid.uuid4().hex
        destination = data_root / f".move_viz-upload-{session_id}.sqlite"
        digest = hashlib.sha256()
        size = 0
        try:
            with destination.open("wb") as handle:
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > upload_limit:
                        raise OverflowError
                    digest.update(chunk)
                    handle.write(chunk)
            with destination.open("rb") as handle:
                header = handle.read(len(SQLITE_HEADER))
            if size < len(SQLITE_HEADER) or header != SQLITE_HEADER:
                raise sqlite3.DatabaseError("The selected file is not a SQLite database")
            payload = await run_in_threadpool(
                session_payload,
                destination,
                session_id,
                safe_filename,
                digest.hexdigest(),
                adopt_source=True,
            )
        except OverflowError:
            destination.unlink(missing_ok=True)
            return _json_error(f"SQLite file exceeds the {upload_limit}-byte upload limit", 413)
        except (OSError, sqlite3.DatabaseError, ValueError, ProjectStateError) as exc:
            destination.unlink(missing_ok=True)
            return _json_error(str(exc), 400)
        return JSONResponse(payload)

    @app.post("/api/apps/move-viz/sessions/example")
    async def create_example_session():
        if not resolved_sample_database or not resolved_sample_database.is_file():
            return _json_error("The bundled SQLite example is not available", 404)
        session_id = uuid.uuid4().hex
        try:
            fingerprint = await run_in_threadpool(_file_sha256, resolved_sample_database)
            payload = await run_in_threadpool(
                session_payload,
                resolved_sample_database,
                session_id,
                resolved_sample_database.name,
                fingerprint,
                adopt_source=False,
            )
            return JSONResponse(payload)
        except (OSError, sqlite3.DatabaseError, ValueError, ProjectStateError) as exc:
            return _json_error(str(exc), 400)

    @app.post("/api/apps/move-viz/sessions/{session_id}/load")
    async def load_session_table(session_id: str, request: Request):
        try:
            path = session_path(session_id)
            record = session_record(session_id)
        except ValueError as exc:
            return _json_error(str(exc), 404)
        try:
            payload = await request.json()
        except Exception:
            return _json_error("Invalid JSON body", 400)
        if not isinstance(payload, dict):
            return _json_error("Invalid JSON body", 400)
        try:
            result = await run_in_threadpool(load_movement_overview, path, payload, max_rows=row_limit)
            project_dir = project_dir_for_name(str(record["project_name"]))
            result.update(await run_in_threadpool(review_state, project_dir, str(result["table"])))
            return JSONResponse(result)
        except (ValueError, sqlite3.DatabaseError) as exc:
            return _json_error(str(exc), 400)

    @app.post("/api/apps/move-viz/sessions/{session_id}/fixes")
    async def load_session_fixes(session_id: str, request: Request):
        try:
            path = session_path(session_id)
            session_record(session_id)
        except ValueError as exc:
            return _json_error(str(exc), 404)
        try:
            payload = await request.json()
        except Exception:
            return _json_error("Invalid JSON body", 400)
        if not isinstance(payload, dict):
            return _json_error("Invalid JSON body", 400)
        raw_individuals = payload.get("individuals")
        if not isinstance(raw_individuals, list) or not raw_individuals:
            return _json_error("Select at least one individual", 400)
        try:
            result = await run_in_threadpool(load_movement_table, path, payload, max_rows=row_limit)
            return JSONResponse(result)
        except (ValueError, sqlite3.DatabaseError) as exc:
            return _json_error(str(exc), 400)

    @app.post("/api/apps/move-viz/sessions/{session_id}/review")
    async def review_session_rows(session_id: str, request: Request):
        try:
            path = session_path(session_id)
            record = session_record(session_id)
            project_dir = project_dir_for_name(str(record["project_name"]))
        except ValueError as exc:
            return _json_error(str(exc), 404)
        try:
            body = await request.json()
        except Exception:
            return _json_error("Invalid JSON body", 400)
        if not isinstance(body, dict):
            return _json_error("Invalid JSON body", 400)
        try:
            operation = str(body.get("operation") or "").strip().lower()
            if operation not in {"flag", "unflag"}:
                raise ValueError("Invalid review operation")
            table_name = str(body.get("table") or "").strip()
            raw_keys = body.get("row_keys")
            if not isinstance(raw_keys, list):
                raise ValueError("Select at least one fix")
            row_keys = sorted({str(item).strip() for item in raw_keys if str(item).strip()})
            if not row_keys:
                raise ValueError("Select at least one fix")
            if len(row_keys) > review_row_limit:
                raise ValueError(f"A review step can include at most {review_row_limit} fixes")
            await run_in_threadpool(
                validate_movement_row_keys,
                path,
                {"table": table_name},
                row_keys,
            )
            scope = str(body.get("scope") or "fix").strip().lower()
            if scope not in {"fix", "segment", "individual"}:
                raise ValueError("Invalid review scope")
            comment = str(body.get("comment") or "").strip()
            if len(comment) > 1200:
                raise ValueError("Review note is too long")
            user = body.get("user")
            state = project_state_payload(project_dir)
            dataset_id = str(state["project"]["current_dataset_id"])
            requested_dataset_id = str(body.get("dataset_id") or "").strip()
            if requested_dataset_id and requested_dataset_id != dataset_id:
                return _json_error("The review graph changed; reload the current stage", 409)
            dataset = load_dataset(project_dir, dataset_id)
            get_dataset_artifact(project_dir, dataset_id, SOURCE_ARTIFACT)
            input_artifacts = [SOURCE_ARTIFACT]
            if any(item.get("logical_name") == REVIEW_ARTIFACT for item in dataset.get("artifacts", [])):
                input_artifacts.append(REVIEW_ARTIFACT)
            step_payload = {
                "user": user,
                "title": f"{operation.title()} {len(row_keys)} fixes ({scope}) in {table_name}",
                "kind": "python",
                "script": REVIEW_STEP_SCRIPT,
                "parameters": {
                    "app": "move_viz",
                    "action": operation,
                    "operation": operation,
                    "table": table_name,
                    "row_keys": row_keys,
                    "scope": scope,
                    "comment": comment,
                    "source_filename": str(record.get("filename") or ""),
                    "source_fingerprint": str(record.get("fingerprint") or ""),
                    "user": user,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": input_artifacts,
                "output_artifacts": [REVIEW_ARTIFACT],
                "set_as_head": True,
            }
            step_result = await run_in_threadpool(create_step, project_dir, step_payload)
            result = {"step_result": step_result}
            result.update(await run_in_threadpool(review_state, project_dir, table_name))
            return JSONResponse(result)
        except (ValueError, sqlite3.DatabaseError, ProjectStateError) as exc:
            return _json_error(str(exc), 400)

    @app.post("/api/apps/move-viz/sessions/{session_id}/undo")
    async def undo_session_review(session_id: str, request: Request):
        try:
            record = session_record(session_id)
            project_dir = project_dir_for_name(str(record["project_name"]))
        except ValueError as exc:
            return _json_error(str(exc), 404)
        try:
            body = await request.json()
        except Exception:
            body = {}
        table_name = str(body.get("table") or record.get("default_table") or "") if isinstance(body, dict) else ""
        try:
            undo_result = await run_in_threadpool(undo_to_parent, project_dir)
            result = {"undo": undo_result}
            result.update(await run_in_threadpool(review_state, project_dir, table_name))
            return JSONResponse(result)
        except ProjectStateError as exc:
            return _json_error(str(exc), 400)

    @app.post("/api/apps/move-viz/sessions/{session_id}/export")
    async def export_session_flags(session_id: str, request: Request):
        try:
            path = session_path(session_id)
            record = session_record(session_id)
            project_dir = project_dir_for_name(str(record["project_name"]))
        except ValueError as exc:
            return _json_error(str(exc), 404)
        try:
            body = await request.json()
        except Exception:
            return _json_error("Invalid JSON body", 400)
        if not isinstance(body, dict):
            return _json_error("Invalid JSON body", 400)
        try:
            table_name = str(body.get("table") or "").strip()
            movement = await run_in_threadpool(
                load_movement_overview,
                path,
                {"table": table_name},
                max_rows=row_limit,
            )
            state = project_state_payload(project_dir)
            dataset_id = str(state["project"]["current_dataset_id"])
            requested_dataset_id = str(body.get("dataset_id") or "").strip()
            if requested_dataset_id and requested_dataset_id != dataset_id:
                return _json_error("The review graph changed; reload the current stage", 409)
            dataset = load_dataset(project_dir, dataset_id)
            if not any(item.get("logical_name") == REVIEW_ARTIFACT for item in dataset.get("artifacts", [])):
                raise ValueError("The current dataset has no flagged fixes to export")
            if not flags_for_dataset(project_dir, dataset_id, table_name):
                raise ValueError("The current dataset has no flagged fixes to export")
            user = body.get("user")
            analysis_payload = {
                "user": user,
                "title": f"Export move_viz flags from {table_name}",
                "kind": "python",
                "script": EXPORT_FLAGS_SCRIPT,
                "dataset_id": dataset_id,
                "input_artifacts": [SOURCE_ARTIFACT, REVIEW_ARTIFACT],
                "output_artifacts": [EXPORT_FLAGS_ARTIFACT],
                "parameters": {
                    "app": "move_viz",
                    "action": "export_flags_csv",
                    "table": table_name,
                    "mapping": movement["mapping"],
                    "source_filename": str(record.get("filename") or ""),
                    "source_fingerprint": str(record.get("fingerprint") or ""),
                    "user": user,
                },
            }
            analysis_result = await run_in_threadpool(create_analysis, project_dir, analysis_payload)
            analysis_id = str(analysis_result["analysis"]["analysis_id"])
            result = {
                "analysis_result": analysis_result,
                "download_url": f"/api/project/{project_dir.name}/analysis/{analysis_id}/artifact/{EXPORT_FLAGS_ARTIFACT}",
                "download_name": f"{Path(str(record.get('filename') or 'movement')).stem}_flags.csv",
            }
            result.update(await run_in_threadpool(review_state, project_dir, table_name))
            return JSONResponse(result)
        except (ValueError, ProjectStateError) as exc:
            return _json_error(str(exc), 400)

    @app.post("/api/apps/move-viz/sessions/{session_id}/head")
    async def set_session_head(session_id: str, request: Request):
        try:
            record = session_record(session_id)
            project_dir = project_dir_for_name(str(record["project_name"]))
        except ValueError as exc:
            return _json_error(str(exc), 404)
        try:
            body = await request.json()
        except Exception:
            return _json_error("Invalid JSON body", 400)
        if not isinstance(body, dict):
            return _json_error("Invalid JSON body", 400)
        table_name = str(body.get("table") or record.get("default_table") or "")
        dataset_id = str(body.get("dataset_id") or "").strip()
        try:
            head = await run_in_threadpool(set_current_head, project_dir, dataset_id)
            result = {"head": head}
            result.update(await run_in_threadpool(review_state, project_dir, table_name))
            return JSONResponse(result)
        except ProjectStateError as exc:
            return _json_error(str(exc), 400)

    @app.delete("/api/apps/move-viz/sessions/{session_id}")
    async def delete_session(session_id: str):
        try:
            session_record(session_id)
        except ValueError as exc:
            return _json_error(str(exc), 404)
        (session_root / f"{session_id}.json").unlink(missing_ok=True)
        return JSONResponse({"deleted": True})
