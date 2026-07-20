from __future__ import annotations

from contextlib import closing
import hashlib
import math
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from urllib.parse import quote
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool


SQLITE_HEADER = b"SQLite format 3\x00"
DEFAULT_MAX_UPLOAD_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_ROWS = 100_000
NUMERIC_DECLARATIONS = ("INT", "REAL", "FLOA", "DOUB", "NUM", "DEC")

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


def load_movement_table(path: Path, payload: dict, *, max_rows: int) -> dict:
    tables = inspect_sqlite(path)
    compatible = [table for table in tables if table["compatible"]]
    requested_table = str(payload.get("table") or "").strip()
    table = next((item for item in tables if item["name"] == requested_table), None)
    if table is None and not requested_table and compatible:
        table = compatible[0]
    if table is None:
        raise ValueError("Select a movement-compatible table")
    mapping = _validated_mapping(table, payload)

    table_identifier = _quoted_identifier(str(table["name"]))
    timestamp_column = mapping["timestamp"]
    order_clause = f" ORDER BY {_quoted_identifier(timestamp_column)}" if timestamp_column else ""
    query_with_rowid = f'SELECT rowid AS "__move_viz_rowid__", * FROM {table_identifier}{order_clause} LIMIT ?'
    query_without_rowid = f"SELECT * FROM {table_identifier}{order_clause} LIMIT ?"

    with closing(_readonly_connection(path)) as connection:
        try:
            records = connection.execute(query_with_rowid, (max_rows,)).fetchall()
            has_rowid = True
        except sqlite3.OperationalError:
            records = connection.execute(query_without_rowid, (max_rows,)).fetchall()
            has_rowid = False

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

    longitude_column = str(mapping["longitude"])
    latitude_column = str(mapping["latitude"])
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
        values = {
            str(column["name"]): _json_value(record[str(column["name"])])
            for column in table["columns"]
        }
        event_value = values.get(str(mapping["event_id"])) if mapping["event_id"] else None
        rowid_value = record["__move_viz_rowid__"] if has_rowid else index + 1
        rows.append(
            {
                "key": f"event:{event_value}#row:{rowid_value}" if event_value not in (None, "") else f"row:{rowid_value}",
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
        "skipped_count": skipped_rows,
        "truncated": table["row_count"] > max_rows,
        "max_rows": max_rows,
        "mapping": mapping,
        "columns": color_columns,
        "rows": rows,
    }


def register_move_viz_routes(
    app: FastAPI,
    *,
    session_root: Path | None = None,
    max_upload_bytes: int | None = None,
    max_rows: int | None = None,
) -> None:
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

    def session_path(session_id: str) -> Path:
        if not re.fullmatch(r"[a-f0-9]{32}", session_id):
            raise ValueError("Unknown SQLite session")
        path = session_root / f"{session_id}.sqlite"
        if not path.exists():
            raise ValueError("Unknown SQLite session")
        return path

    @app.post("/api/apps/move-viz/sessions")
    async def create_session(request: Request, filename: str = "movement.sqlite"):
        safe_filename = Path(filename).name.strip() or "movement.sqlite"
        session_id = uuid.uuid4().hex
        destination = session_root / f"{session_id}.sqlite"
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
            tables = await run_in_threadpool(inspect_sqlite, destination)
        except OverflowError:
            destination.unlink(missing_ok=True)
            return _json_error(f"SQLite file exceeds the {upload_limit}-byte upload limit", 413)
        except (OSError, sqlite3.DatabaseError) as exc:
            destination.unlink(missing_ok=True)
            return _json_error(str(exc), 400)
        compatible = [table for table in tables if table["compatible"]]
        return JSONResponse(
            {
                "session_id": session_id,
                "filename": safe_filename,
                "size": size,
                "fingerprint": digest.hexdigest(),
                "tables": tables,
                "default_table": compatible[0]["name"] if compatible else "",
            }
        )

    @app.post("/api/apps/move-viz/sessions/{session_id}/load")
    async def load_session_table(session_id: str, request: Request):
        try:
            path = session_path(session_id)
        except ValueError as exc:
            return _json_error(str(exc), 404)
        try:
            payload = await request.json()
        except Exception:
            return _json_error("Invalid JSON body", 400)
        if not isinstance(payload, dict):
            return _json_error("Invalid JSON body", 400)
        try:
            result = await run_in_threadpool(load_movement_table, path, payload, max_rows=row_limit)
            return JSONResponse(result)
        except (ValueError, sqlite3.DatabaseError) as exc:
            return _json_error(str(exc), 400)

    @app.delete("/api/apps/move-viz/sessions/{session_id}")
    async def delete_session(session_id: str):
        try:
            path = session_path(session_id)
        except ValueError as exc:
            return _json_error(str(exc), 404)
        path.unlink(missing_ok=True)
        return JSONResponse({"deleted": True})
