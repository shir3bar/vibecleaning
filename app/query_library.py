import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .state import META_DIR_NAME, ProjectStateError


SAFE_QUERY_ID = re.compile(r"^[A-Za-z0-9._-]+$")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def query_library_path(data_root: Path) -> Path:
    return data_root.resolve() / META_DIR_NAME / "query_library.json"


def load_query_library(data_root: Path) -> dict:
    path = query_library_path(data_root)
    if not path.exists():
        return {"queries": []}
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ProjectStateError("Invalid query library JSON") from exc
    if not isinstance(payload, dict):
        raise ProjectStateError("Invalid query library")
    queries = payload.get("queries")
    if not isinstance(queries, list):
        raise ProjectStateError("Invalid query library")
    records = []
    seen_versions = set()
    for index, query in enumerate(queries):
        record = _validate_query_record(query, index=index)
        key = (record["query_id"], record["version"])
        if key in seen_versions:
            raise ProjectStateError("Duplicate query version in library")
        seen_versions.add(key)
        records.append(record)
    return {"queries": records}


def save_query_library(data_root: Path, payload: dict):
    path = query_library_path(data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.parent / f"{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        temp_path.replace(path)
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass


def list_queries(data_root: Path, app: str | None = None) -> list[dict]:
    app_filter = _optional_text(app, "app", max_length=80)
    queries = load_query_library(data_root)["queries"]
    if app_filter:
        queries = [query for query in queries if query.get("app") == app_filter]
    return sorted(
        queries,
        key=lambda item: (
            str(item.get("app") or ""),
            str(item.get("query_id") or ""),
            item["version"],
        ),
    )


def get_query(data_root: Path, query_id: str, version: int | None = None) -> dict:
    normalized_id = _query_id(query_id)
    matches = [
        query
        for query in load_query_library(data_root)["queries"]
        if query.get("query_id") == normalized_id
    ]
    if not matches:
        raise ProjectStateError("Unknown query")
    if version is not None:
        requested_version = _query_version(version)
        for query in matches:
            if query.get("version") == requested_version:
                return dict(query)
        raise ProjectStateError("Unknown query version")
    latest = max(matches, key=lambda item: item["version"])
    return dict(latest)


def save_query(data_root: Path, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ProjectStateError("Invalid query payload")
    library = load_query_library(data_root)
    queries = library["queries"]

    query_id = payload.get("query_id")
    if query_id in (None, ""):
        query_id = f"query_{uuid.uuid4().hex[:12]}"
    else:
        query_id = _query_id(query_id)

    existing_versions = [
        query["version"]
        for query in queries
        if query.get("query_id") == query_id
    ]
    version = max(existing_versions, default=0) + 1

    record = {
        "query_id": query_id,
        "version": version,
        "app": _required_text(payload.get("app"), "app", max_length=80),
        "name": _required_text(payload.get("name"), "name", max_length=160),
        "description": _optional_text(payload.get("description"), "description", max_length=1200),
        "candidate_kind": _required_text(payload.get("candidate_kind"), "candidate_kind", max_length=80),
        "evaluator": _required_mapping(payload.get("evaluator"), "evaluator"),
        "definition": _required_mapping(payload.get("definition"), "definition"),
        "parameters": _optional_mapping(payload.get("parameters"), "parameters"),
        "required_fields": _optional_list(payload.get("required_fields"), "required_fields"),
        "created_by": _required_text(payload.get("created_by"), "created_by", max_length=80),
        "created_at": now_iso(),
    }
    queries.append(record)
    save_query_library(data_root, {"queries": queries})
    return dict(record)


def _query_id(raw_value: object) -> str:
    if not isinstance(raw_value, str):
        raise ProjectStateError("Invalid query id")
    value = raw_value.strip()
    if not value or value.startswith(".") or not SAFE_QUERY_ID.fullmatch(value):
        raise ProjectStateError("Invalid query id")
    return value


def _query_version(raw_value: object) -> int:
    if isinstance(raw_value, bool):
        raise ProjectStateError("Invalid query version")
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ProjectStateError("Invalid query version") from exc
    if value <= 0:
        raise ProjectStateError("Invalid query version")
    return value


def _validate_query_record(raw_value: object, *, index: int) -> dict:
    if not isinstance(raw_value, dict):
        raise ProjectStateError(f"Invalid query record at index {index}")
    record = dict(raw_value)
    _query_id(record.get("query_id"))
    version = record.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        raise ProjectStateError(f"Invalid query version at index {index}")
    _required_text(record.get("app"), "app", max_length=80)
    _required_text(record.get("name"), "name", max_length=160)
    _optional_text(record.get("description"), "description", max_length=1200)
    _required_text(record.get("candidate_kind"), "candidate_kind", max_length=80)
    _required_mapping(record.get("evaluator"), "evaluator")
    _required_mapping(record.get("definition"), "definition")
    _optional_mapping(record.get("parameters"), "parameters")
    _optional_list(record.get("required_fields"), "required_fields")
    _required_text(record.get("created_by"), "created_by", max_length=80)
    _required_text(record.get("created_at"), "created_at", max_length=80)
    return record


def _required_text(raw_value: object, label: str, *, max_length: int) -> str:
    value = _optional_text(raw_value, label, max_length=max_length)
    if not value:
        raise ProjectStateError(f"Missing {label}")
    return value


def _optional_text(raw_value: object, label: str, *, max_length: int) -> str:
    if raw_value in (None, ""):
        return ""
    if not isinstance(raw_value, str):
        raise ProjectStateError(f"Invalid {label}")
    value = " ".join(raw_value.strip().split())
    if len(value) > max_length:
        raise ProjectStateError(f"{label.capitalize()} is too long")
    return value


def _required_mapping(raw_value: object, label: str) -> dict:
    if not isinstance(raw_value, dict):
        raise ProjectStateError(f"Missing {label}")
    return dict(raw_value)


def _optional_mapping(raw_value: object, label: str) -> dict:
    if raw_value in (None, ""):
        return {}
    if not isinstance(raw_value, dict):
        raise ProjectStateError(f"Invalid {label}")
    return dict(raw_value)


def _optional_list(raw_value: object, label: str) -> list:
    if raw_value in (None, ""):
        return []
    if not isinstance(raw_value, list):
        raise ProjectStateError(f"Invalid {label}")
    return list(raw_value)
