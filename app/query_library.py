import json
import re
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from .state import META_DIR_NAME, ProjectStateError


SAFE_QUERY_ID = re.compile(r"^[A-Za-z0-9._-]+$")
BUILTIN_CREATED_AT = "2026-05-30T00:00:00+00:00"


BUILTIN_QUERY_RECORDS = [
    {
        "query_id": "precomputed_near_road_50m",
        "version": 1,
        "app": "movement",
        "name": "Near precomputed road context (<= 50 m)",
        "description": (
            "Find fixes within 50 m of the nearest road using offline precomputed "
            "osm:nearest_road_distance_m columns from an OSM-enriched movement artifact."
        ),
        "candidate_kind": "fix",
        "evaluator": {"type": "fix_numeric_comparison"},
        "definition": {"field": "osm:nearest_road_distance_m", "op": "<=", "value": 50},
        "segment_grouping": {
            "enabled": True,
            "min_fixes": 2,
            "min_duration_s": 0,
            "max_gap_s": None,
            "preview_limit": 200,
        },
        "parameters": {},
        "required_fields": ["osm:nearest_road_distance_m"],
        "created_by": "system",
        "created_at": BUILTIN_CREATED_AT,
    },
    {
        "query_id": "precomputed_near_railway_50m",
        "version": 1,
        "app": "movement",
        "name": "Near precomputed railway context (<= 50 m)",
        "description": (
            "Find fixes within 50 m of the nearest railway using offline precomputed "
            "osm:nearest_railway_distance_m columns from an OSM-enriched movement artifact."
        ),
        "candidate_kind": "fix",
        "evaluator": {"type": "fix_numeric_comparison"},
        "definition": {"field": "osm:nearest_railway_distance_m", "op": "<=", "value": 50},
        "segment_grouping": {
            "enabled": True,
            "min_fixes": 2,
            "min_duration_s": 0,
            "max_gap_s": None,
            "preview_limit": 200,
        },
        "parameters": {},
        "required_fields": ["osm:nearest_railway_distance_m"],
        "created_by": "system",
        "created_at": BUILTIN_CREATED_AT,
    },
    {
        "query_id": "precomputed_road_context_not_matched",
        "version": 1,
        "app": "movement",
        "name": "Road context not matched",
        "description": (
            "Find fixes whose offline road context status is not matched. This uses "
            "precomputed osm:road_match_status values and does not fetch OSM."
        ),
        "candidate_kind": "fix",
        "evaluator": {"type": "fix_string_comparison"},
        "definition": {"field": "osm:road_match_status", "op": "!=", "value": "matched"},
        "parameters": {},
        "required_fields": ["osm:road_match_status"],
        "created_by": "system",
        "created_at": BUILTIN_CREATED_AT,
    },
    {
        "query_id": "precomputed_railway_context_not_matched",
        "version": 1,
        "app": "movement",
        "name": "Railway context not matched",
        "description": (
            "Find fixes whose offline railway context status is not matched. This uses "
            "precomputed osm:railway_match_status values and does not fetch OSM."
        ),
        "candidate_kind": "fix",
        "evaluator": {"type": "fix_string_comparison"},
        "definition": {"field": "osm:railway_match_status", "op": "!=", "value": "matched"},
        "parameters": {},
        "required_fields": ["osm:railway_match_status"],
        "created_by": "system",
        "created_at": BUILTIN_CREATED_AT,
    },
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def query_library_path(data_root: Path) -> Path:
    return data_root.resolve() / META_DIR_NAME / "query_library.json"


def load_query_library(data_root: Path) -> dict:
    persisted = _load_persisted_query_library(data_root)
    return {"queries": _merge_builtin_queries(persisted["queries"])}


def _load_persisted_query_library(data_root: Path) -> dict:
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


def _merge_builtin_queries(persisted_records: list[dict]) -> list[dict]:
    records = [dict(record) for record in persisted_records]
    seen_versions = {(record["query_id"], record["version"]) for record in records}
    for query in BUILTIN_QUERY_RECORDS:
        record = _validate_query_record(deepcopy(query), index=-1)
        key = (record["query_id"], record["version"])
        if key in seen_versions:
            continue
        records.append(record)
        seen_versions.add(key)
    return records


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
    persisted_library = _load_persisted_query_library(data_root)
    persisted_queries = persisted_library["queries"]
    existing_queries = _merge_builtin_queries(persisted_queries)

    query_id = payload.get("query_id")
    if query_id in (None, ""):
        query_id = f"query_{uuid.uuid4().hex[:12]}"
    else:
        query_id = _query_id(query_id)

    existing_versions = [
        query["version"]
        for query in existing_queries
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
    persisted_queries.append(record)
    save_query_library(data_root, {"queries": persisted_queries})
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
