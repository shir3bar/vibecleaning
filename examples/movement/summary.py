import csv
import gzip
import hashlib
import json
import random
import uuid
from datetime import datetime
from functools import lru_cache
from math import isfinite
from pathlib import Path


REVIEW_COLUMNS = [
    "vc_outlier_status",
    "vc_issue_id",
    "vc_issue_type",
    "vc_issue_field",
    "vc_issue_threshold",
    "vc_issue_refs",
    "vc_issue_note",
    "vc_owner_question",
    "vc_review_user",
    "vc_reviewed_at",
]

SEGMENT_REVIEW_COLUMNS = [
    "vc_segment_status",
    "vc_segment_id",
    "vc_segment_type",
    "vc_segment_note",
    "vc_segment_owner_question",
    "vc_segment_review_user",
    "vc_segment_reviewed_at",
    "vc_segment_refs",
]

ALL_REVIEW_COLUMNS = REVIEW_COLUMNS + SEGMENT_REVIEW_COLUMNS

DERIVED_FIELDS = [
    {"key": "step_length_m", "label": "Step length (m)", "kind": "numeric", "source": "derived"},
    {"key": "speed_mps", "label": "Speed (m/s)", "kind": "numeric", "source": "derived"},
    {"key": "time_delta_s", "label": "Time delta (s)", "kind": "numeric", "source": "derived"},
]

QUALITY_KEYWORDS = (
    "gps",
    "quality",
    "fix",
    "visible",
    "outlier",
    "manual",
    "algorithm",
    "hdop",
    "pdop",
    "dop",
    "satellite",
    "sat",
    "accuracy",
    "precision",
    "error",
    "usedtime",
    "timetogetfix",
    "heightabovemsl",
)

MAX_SERIES_POINTS = 1500
MAX_STAT_SAMPLES = 2000
DEFAULT_FIX_LIMIT = 1000000
SUMMARY_CACHE_VERSION = 12
DEFAULT_BURST_GAP_MODE = "manual"
DEFAULT_BURST_GAP_SECONDS = 3600.0
DEFAULT_BURST_GAP_QUANTILE = 0.999


def normalize_header(header: str | None) -> str:
    return str(header or "").lower().replace("-", "").replace("_", "").replace(":", "").replace(" ", "")


def find_column(normalized_map: dict[str, str], aliases: list[str]) -> str | None:
    for alias in aliases:
        if alias in normalized_map:
            return normalized_map[alias]
    return None


def detect_columns(fieldnames: list[str]) -> dict[str, str | None]:
    normalized = {normalize_header(name): name for name in fieldnames}
    return {
        "fix_id": find_column(normalized, [
            "eventid",
            "fixid",
            "observationid",
            "rowid",
            "recordid",
            "id",
        ]),
        "individual": find_column(normalized, [
            "individual",
            "individualid",
            "individuallocalidentifier",
            "animalid",
            "trackid",
            "taglocalidentifier",
        ]),
        "time": find_column(normalized, [
            "timestamp",
            "time",
            "datetime",
            "eventtime",
            "transmissiontimestamp",
            "studylocaltimestamp",
        ]),
        "lon": find_column(normalized, [
            "longitude",
            "lon",
            "locationlong",
            "stependlocationlong",
            "x",
        ]),
        "lat": find_column(normalized, [
            "latitude",
            "lat",
            "locationlat",
            "stependlocationlat",
            "y",
        ]),
        "common_name": find_column(normalized, [
            "individualtaxoncommonname",
            "taxoncommonname",
            "commonname",
            "speciescommonname",
            "vernacularname",
            "animalcommonname",
        ]),
        "scientific_name": find_column(normalized, [
            "individualtaxoncanonicalname",
            "taxoncanonicalname",
            "scientificname",
            "species",
            "taxon",
        ]),
        "set": find_column(normalized, ["set", "split", "partition"]),
        **{name: normalized.get(normalize_header(name)) for name in ALL_REVIEW_COLUMNS},
    }


def normalize_individual_filters(
    *,
    individual: str = "",
    individuals: list[str] | tuple[str, ...] | set[str] | None = None,
) -> tuple[str, ...]:
    if individuals is not None:
        raw_values = [str(value or "").strip() for value in individuals]
    else:
        raw_values = [str(individual or "").strip()]
    return tuple(sorted({value for value in raw_values if value}))


def parse_time_ms(raw_value: object) -> int | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    for parser in (
        lambda item: datetime.fromisoformat(item),
        lambda item: datetime.strptime(item, "%Y-%m-%d %H:%M:%S"),
        lambda item: datetime.strptime(item, "%Y-%m-%d %H:%M:%S.%f"),
    ):
        try:
            return int(parser(normalized).timestamp() * 1000)
        except ValueError:
            continue
    return None


def try_float(raw_value: object) -> float | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return number if isfinite(number) else None


def parse_bool(raw_value: object) -> bool | None:
    value = str(raw_value or "").strip().lower()
    if value in {"true", "t", "yes", "y", "1"}:
        return True
    if value in {"false", "f", "no", "n", "0"}:
        return False
    return None


def reservoir_append(sample: list, item, seen_count: int, limit: int):
    if limit <= 0:
        return
    if len(sample) < limit:
        sample.append(item)
        return
    slot = random.randrange(seen_count)
    if slot < limit:
        sample[slot] = item


def median(values: list[float]) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return float(sorted_values[mid])
    return float((sorted_values[mid - 1] + sorted_values[mid]) / 2)


def quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    idx = (len(sorted_values) - 1) * q
    lower = int(idx)
    upper = min(len(sorted_values) - 1, lower + 1)
    if lower == upper:
        return float(sorted_values[lower])
    ratio = idx - lower
    return float(sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * ratio)


def normalize_burst_gap_seconds(value: object = None) -> float:
    gap = DEFAULT_BURST_GAP_SECONDS if value in (None, "") else float(value)
    if not isfinite(gap) or gap <= 0.0:
        raise ValueError("burst_gap_seconds must be positive")
    return float(gap)


def normalize_burst_gap_mode(value: object = None) -> str:
    mode = DEFAULT_BURST_GAP_MODE if value in (None, "") else str(value).strip().lower()
    if mode not in {"manual", "quantile"}:
        raise ValueError("burst_gap_mode must be 'manual' or 'quantile'")
    return mode


def normalize_burst_gap_quantile(value: object = None) -> float:
    quantile_value = DEFAULT_BURST_GAP_QUANTILE if value in (None, "") else float(value)
    if not isfinite(quantile_value) or quantile_value <= 0.0 or quantile_value > 1.0:
        raise ValueError("burst_gap_quantile must satisfy 0 < q <= 1")
    return float(quantile_value)


def _track_gap_seconds(records_by_group: dict[tuple[str, str], list[dict]]) -> list[float]:
    gaps: list[float] = []
    for _group_key, sorted_records in _sorted_track_records(records_by_group):
        previous_time_ms = None
        for record in sorted_records:
            time_ms = record.get("time_ms")
            if previous_time_ms is not None:
                gap = (time_ms - previous_time_ms) / 1000.0
                if isfinite(gap) and gap >= 0.0:
                    gaps.append(float(gap))
            previous_time_ms = time_ms
    return gaps


def resolve_burst_gap_strategy(
    records_by_group: dict[tuple[str, str], list[dict]],
    *,
    burst_gap_mode: object = DEFAULT_BURST_GAP_MODE,
    burst_gap_seconds: object = DEFAULT_BURST_GAP_SECONDS,
    burst_gap_quantile: object = DEFAULT_BURST_GAP_QUANTILE,
) -> dict:
    mode = normalize_burst_gap_mode(burst_gap_mode)
    fallback_seconds = normalize_burst_gap_seconds(burst_gap_seconds)
    quantile_value = normalize_burst_gap_quantile(burst_gap_quantile)
    gaps = _track_gap_seconds(records_by_group)

    effective_seconds = fallback_seconds
    used_fallback = False
    if mode == "quantile":
        quantile_seconds = quantile(gaps, quantile_value)
        if quantile_seconds is None or not isfinite(quantile_seconds) or quantile_seconds <= 0.0:
            used_fallback = True
        else:
            effective_seconds = float(quantile_seconds)

    return {
        "mode": mode,
        "quantile": float(quantile_value),
        "fallback_seconds": float(fallback_seconds),
        "effective_seconds": float(effective_seconds),
        "gap_count": int(len(gaps)),
        "used_fallback": bool(used_fallback),
    }


def _burst_gap_metadata(burst_gap: dict) -> dict:
    return {
        "burst_gap": {
            "mode": burst_gap["mode"],
            "quantile": float(burst_gap["quantile"]),
            "fallback_seconds": float(burst_gap["fallback_seconds"]),
            "effective_seconds": float(burst_gap["effective_seconds"]),
            "gap_count": int(burst_gap["gap_count"]),
            "used_fallback": bool(burst_gap["used_fallback"]),
        },
        "burst_gap_mode": burst_gap["mode"],
        "burst_gap_quantile": float(burst_gap["quantile"]),
        "burst_gap_fallback_seconds": float(burst_gap["fallback_seconds"]),
        "burst_gap_gap_count": int(burst_gap["gap_count"]),
        "burst_gap_used_fallback": bool(burst_gap["used_fallback"]),
        "burst_gap_seconds": float(burst_gap["effective_seconds"]),
    }


def haversine_meters(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    from math import atan2, cos, radians, sin, sqrt

    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    return 6371000.0 * 2 * atan2(sqrt(a), sqrt(1 - a))


def is_valid_coordinate(lon: float, lat: float) -> bool:
    return isfinite(lon) and isfinite(lat) and -180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0


def span_to_zoom(span_deg: float) -> float:
    if span_deg <= 0:
        return 2.0
    if span_deg > 120:
        return 1.0
    if span_deg > 60:
        return 2.0
    if span_deg > 30:
        return 3.0
    if span_deg > 15:
        return 4.0
    if span_deg > 8:
        return 5.0
    if span_deg > 4:
        return 6.0
    if span_deg > 2:
        return 7.0
    if span_deg > 1:
        return 8.0
    if span_deg > 0.5:
        return 9.0
    if span_deg > 0.25:
        return 10.0
    return 11.0


def _cache_metadata(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return str(path.resolve()), stat.st_mtime_ns, stat.st_size


def _project_root_for_path(path: Path) -> Path | None:
    resolved = path.resolve()
    for candidate in [resolved.parent, *resolved.parents]:
        if (candidate / ".vibecleaning").is_dir():
            return candidate
    return None


def _cache_key(kind: str, params: dict | None = None) -> str:
    payload = {"kind": kind, "params": params or {}, "version": SUMMARY_CACHE_VERSION}
    return hashlib.sha1(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _summary_disk_cache_path(path: Path, kind: str, params: dict | None = None) -> Path | None:
    project_root = _project_root_for_path(path)
    if project_root is None:
        return None
    digest = _cache_key(kind, params)
    return project_root / ".vibecleaning" / "cache" / "movement_summary" / f"{digest}.json.gz"


def _load_cached_response(path: Path, *, kind: str, params: dict | None, mtime_ns: int, size: int) -> dict | None:
    cache_path = _summary_disk_cache_path(path, kind, params)
    if cache_path is None or not cache_path.exists():
        return None
    try:
        with gzip.open(cache_path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if (
        payload.get("version") != SUMMARY_CACHE_VERSION
        or payload.get("path") != str(path.resolve())
        or payload.get("mtime_ns") != mtime_ns
        or payload.get("size") != size
        or payload.get("kind") != kind
        or payload.get("params") != (params or {})
    ):
        return None
    summary = payload.get("summary")
    return summary if isinstance(summary, dict) else None


def _save_cached_response(path: Path, *, kind: str, params: dict | None, mtime_ns: int, size: int, summary: dict):
    cache_path = _summary_disk_cache_path(path, kind, params)
    if cache_path is None:
        return
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": SUMMARY_CACHE_VERSION,
        "path": str(path.resolve()),
        "mtime_ns": mtime_ns,
        "size": size,
        "kind": kind,
        "params": params or {},
        "summary": summary,
    }
    temp_path = cache_path.parent / f"{cache_path.name}.{uuid.uuid4().hex}.tmp"
    try:
        with gzip.open(temp_path, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"))
        temp_path.replace(cache_path)
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass


def _categorical_value(raw_value: str) -> str:
    value = raw_value.strip()
    return value if value else "Missing"


def _is_present(value: object) -> bool:
    return value is not None and value != ""


def _candidate_field_kind(stats: dict) -> str | None:
    nonempty = stats["nonempty"]
    if nonempty <= 0:
        return None
    if stats["bool_count"] == nonempty:
        return "boolean"
    if stats["numeric_count"] == nonempty:
        return "numeric"
    if len(stats["unique_values"]) <= 12:
        return "categorical"
    return None


def _should_include_quality_field(fieldname: str, stats: dict) -> bool:
    normalized = normalize_header(fieldname)
    if fieldname in ALL_REVIEW_COLUMNS:
        return True
    if any(keyword in normalized for keyword in QUALITY_KEYWORDS):
        return True
    return stats["bool_count"] == stats["nonempty"] and stats["nonempty"] > 0


def _make_fix_key(row_index: int, fix_id: str, individual: str, time_ms: int) -> str:
    if fix_id:
        return f"id:{fix_id}#row:{row_index}"
    return f"row:{row_index}|{individual}|{time_ms}"


def _normalize_review_status(raw_value: object) -> str:
    value = str(raw_value or "").strip().lower()
    return value if value in {"suspected", "confirmed"} else ""


def _normalize_segment_status(raw_value: object) -> str:
    return _normalize_review_status(raw_value)


def _normalize_issue_payload(item: dict, *, fallback_status: str = "") -> dict:
    status = _normalize_review_status(item.get("status")) or _normalize_review_status(fallback_status)
    if not status:
        return {}
    issue = {
        "status": status,
        "issue_id": str(item.get("issue_id", "")).strip(),
        "issue_type": str(item.get("issue_type", "")).strip(),
        "issue_field": str(item.get("issue_field", "")).strip(),
        "issue_threshold": str(item.get("issue_threshold", "")).strip(),
        "issue_note": str(item.get("issue_note", "")).strip(),
        "owner_question": str(item.get("owner_question", "")).strip(),
        "review_user": str(item.get("review_user", "")).strip(),
        "reviewed_at": str(item.get("reviewed_at", "")).strip(),
    }
    cleaned = {key: value for key, value in issue.items() if _is_present(value)}
    if not cleaned.get("issue_id") and not cleaned.get("issue_type"):
        return {}
    return cleaned


def _normalize_segment_payload(item: dict, *, fallback_status: str = "") -> dict:
    status = _normalize_segment_status(item.get("status")) or _normalize_segment_status(fallback_status)
    if not status:
        return {}
    segment = {
        "status": status,
        "segment_id": str(item.get("segment_id", "")).strip(),
        "issue_type": str(item.get("issue_type", "")).strip(),
        "start_fix_key": str(item.get("start_fix_key", "")).strip(),
        "end_fix_key": str(item.get("end_fix_key", "")).strip(),
        "issue_note": str(item.get("issue_note", "")).strip(),
        "owner_question": str(item.get("owner_question", "")).strip(),
        "review_user": str(item.get("review_user", "")).strip(),
        "reviewed_at": str(item.get("reviewed_at", "")).strip(),
    }
    cleaned = {key: value for key, value in segment.items() if _is_present(value)}
    if not cleaned.get("segment_id"):
        return {}
    return cleaned


def _prepare_scan_context(path: Path) -> tuple[list[str], dict[str, str | None], dict[str, dict]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        columns = detect_columns(fieldnames)
        if not columns["individual"] or not columns["time"] or not columns["lon"] or not columns["lat"]:
            raise ValueError("CSV is missing required columns for movement visualization")

        field_stats = {
            name: {
                "nonempty": 0,
                "numeric_count": 0,
                "bool_count": 0,
                "unique_values": set(),
            }
            for name in fieldnames
        }
        for raw in reader:
            for fieldname, raw_value in raw.items():
                value = str(raw_value or "").strip()
                if not value:
                    continue
                stats = field_stats[fieldname]
                stats["nonempty"] += 1
                if len(stats["unique_values"]) < 24:
                    stats["unique_values"].add(value)
                if try_float(value) is not None:
                    stats["numeric_count"] += 1
                if parse_bool(value) is not None:
                    stats["bool_count"] += 1

    return fieldnames, columns, field_stats


@lru_cache(maxsize=32)
def _prepare_scan_context_cached(path_str: str, _mtime_ns: int, _size: int):
    return _prepare_scan_context(Path(path_str))


def _build_color_fields(fieldnames: list[str], columns: dict[str, str | None], field_stats: dict[str, dict]) -> list[dict]:
    excluded_fields = {
        value
        for key, value in columns.items()
        if value and key not in ALL_REVIEW_COLUMNS
    }
    color_fields = list(DERIVED_FIELDS)
    for review_field in REVIEW_COLUMNS:
        if columns.get(review_field):
            color_fields.append(
                {
                    "key": review_field,
                    "label": review_field,
                    "kind": "categorical",
                    "source": "review",
                    "column_name": review_field,
                }
            )

    for fieldname in fieldnames:
        if fieldname in excluded_fields or fieldname in ALL_REVIEW_COLUMNS:
            continue
        stats = field_stats[fieldname]
        if not _should_include_quality_field(fieldname, stats):
            continue
        kind = _candidate_field_kind(stats)
        if not kind:
            continue
        color_fields.append(
            {
                "key": fieldname,
                "label": fieldname,
                "kind": kind,
                "source": "raw",
                "column_name": fieldname,
            }
        )
    return color_fields


def _compact_review(raw: dict) -> dict:
    issues = _review_issues(raw)
    status = _normalize_review_status(raw.get("vc_outlier_status"))
    has_active_review = bool(status or issues)
    review = {
        "status": status,
        "issue_id": str(raw.get("vc_issue_id", "")).strip() if has_active_review else "",
        "issue_type": str(raw.get("vc_issue_type", "")).strip() if has_active_review else "",
        "issue_field": str(raw.get("vc_issue_field", "")).strip() if has_active_review else "",
        "issue_threshold": str(raw.get("vc_issue_threshold", "")).strip() if has_active_review else "",
        "issue_note": str(raw.get("vc_issue_note", "")).strip() if has_active_review else "",
        "owner_question": str(raw.get("vc_owner_question", "")).strip() if has_active_review else "",
        "review_user": str(raw.get("vc_review_user", "")).strip() if has_active_review else "",
        "reviewed_at": str(raw.get("vc_reviewed_at", "")).strip() if has_active_review else "",
    }
    if issues:
        review["issues"] = issues
    return {key: value for key, value in review.items() if _is_present(value)}


def _review_issues(raw: dict) -> list[dict]:
    row_status = _normalize_review_status(raw.get("vc_outlier_status"))
    raw_refs = str(raw.get("vc_issue_refs", "")).strip()
    if raw_refs:
        try:
            parsed = json.loads(raw_refs)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            issues = []
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                cleaned = _normalize_issue_payload(item, fallback_status=row_status)
                if cleaned:
                    issues.append(cleaned)
            if issues:
                return issues
    legacy_issue = _normalize_issue_payload(
        {
            "status": row_status,
            "issue_id": str(raw.get("vc_issue_id", "")).strip(),
            "issue_type": str(raw.get("vc_issue_type", "")).strip(),
            "issue_field": str(raw.get("vc_issue_field", "")).strip(),
            "issue_threshold": str(raw.get("vc_issue_threshold", "")).strip(),
            "issue_note": str(raw.get("vc_issue_note", "")).strip(),
            "owner_question": str(raw.get("vc_owner_question", "")).strip(),
            "review_user": str(raw.get("vc_review_user", "")).strip(),
            "reviewed_at": str(raw.get("vc_reviewed_at", "")).strip(),
        },
        fallback_status=row_status,
    )
    return [legacy_issue] if legacy_issue else []


def _segment_memberships(raw: dict) -> list[dict]:
    row_status = _normalize_segment_status(raw.get("vc_segment_status"))
    raw_refs = str(raw.get("vc_segment_refs", "")).strip()
    if raw_refs:
        try:
            parsed = json.loads(raw_refs)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            segments = []
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                cleaned = _normalize_segment_payload(item, fallback_status=row_status)
                if cleaned:
                    segments.append(cleaned)
            if segments:
                return segments
    legacy_segment = _normalize_segment_payload(
        {
            "status": row_status,
            "segment_id": str(raw.get("vc_segment_id", "")).strip(),
            "issue_type": str(raw.get("vc_segment_type", "")).strip(),
            "issue_note": str(raw.get("vc_segment_note", "")).strip(),
            "owner_question": str(raw.get("vc_segment_owner_question", "")).strip(),
            "review_user": str(raw.get("vc_segment_review_user", "")).strip(),
            "reviewed_at": str(raw.get("vc_segment_reviewed_at", "")).strip(),
        },
        fallback_status=row_status,
    )
    return [legacy_segment] if legacy_segment else []


def _build_attributes(raw: dict, *, color_fields: list[dict], step_length_m, speed_mps, time_delta_s) -> dict:
    attributes = {
        "step_length_m": step_length_m,
        "speed_mps": speed_mps,
        "time_delta_s": time_delta_s,
    }
    for field in color_fields:
        key = field["key"]
        if key in {"step_length_m", "speed_mps", "time_delta_s"}:
            continue
        raw_value = str(raw.get(key, "")).strip()
        if field["kind"] == "numeric":
            attributes[key] = try_float(raw_value)
        elif field["kind"] == "boolean":
            parsed = parse_bool(raw_value)
            attributes[key] = parsed if parsed is not None else None
        else:
            attributes[key] = _categorical_value(raw_value)
    return {key: value for key, value in attributes.items() if _is_present(value)}


def _build_fix_record(
    *,
    row_index: int,
    fix_id: str,
    individual: str,
    set_name: str,
    time_ms: int,
    lon: float,
    lat: float,
    attributes: dict,
    review: dict,
    segment_memberships: list[dict],
) -> dict:
    fix = {
        "fix_key": _make_fix_key(row_index, fix_id, individual, time_ms),
        "individual": individual,
        "time_ms": int(time_ms),
        "lon": float(lon),
        "lat": float(lat),
    }
    if set_name != "train":
        fix["set"] = set_name
    if attributes:
        fix["attributes"] = attributes
    if review:
        fix["review"] = review
    if segment_memberships:
        fix["segments"] = [dict(item) for item in segment_memberships]
    return fix


def _track_key(individual: str, set_name: str) -> tuple[str, str]:
    return str(individual), str(set_name or "train")


def _record_sort_key(record: dict) -> tuple[int, int, str]:
    return int(record["time_ms"]), int(record["row_index"]), str(record["fix_key"])


def _group_track_records(records: list[dict]) -> dict[tuple[str, str], list[dict]]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    for record in records:
        grouped.setdefault(_track_key(record["individual"], record["set_name"]), []).append(record)
    return grouped


def _sorted_track_records(records_by_group: dict[tuple[str, str], list[dict]]):
    for group_key in sorted(records_by_group):
        yield group_key, sorted(records_by_group[group_key], key=_record_sort_key)


def _downsample_sorted_records(records: list[dict], limit: int) -> list[dict]:
    if limit <= 0 or len(records) <= limit:
        return records
    if limit == 1:
        return [records[0]]
    last_index = len(records) - 1
    indexes = []
    previous_index = -1
    for output_index in range(limit):
        source_index = round((output_index * last_index) / (limit - 1))
        source_index = min(last_index, max(0, int(source_index)))
        if source_index != previous_index:
            indexes.append(source_index)
            previous_index = source_index
    return [records[index] for index in indexes]


def _compute_track_movement(
    records_by_group: dict[tuple[str, str], list[dict]]
) -> tuple[dict[str, dict[str, float | None]], dict[str, dict[str, list[float] | int]]]:
    movement_by_fix_key: dict[str, dict[str, float | None]] = {}
    stat_samples: dict[str, dict[str, list[float] | int]] = {}

    for (_individual, _set_name), sorted_records in _sorted_track_records(records_by_group):
        previous = None
        for record in sorted_records:
            individual = record["individual"]
            indiv_stats = stat_samples.setdefault(
                individual,
                {"seen_fix": 0, "seen_step": 0, "seen_speed": 0, "fix": [], "step": [], "speed": []},
            )
            step_length_m = None
            time_delta_s = None
            speed_mps = None
            if previous and record["time_ms"] > previous["time_ms"]:
                time_delta_s = (record["time_ms"] - previous["time_ms"]) / 1000.0
                step_length_m = haversine_meters(previous["lon"], previous["lat"], record["lon"], record["lat"])
                speed_mps = step_length_m / time_delta_s if time_delta_s > 0 else None
                indiv_stats["seen_fix"] += 1
                indiv_stats["seen_step"] += 1
                reservoir_append(indiv_stats["fix"], time_delta_s, indiv_stats["seen_fix"], MAX_STAT_SAMPLES)
                reservoir_append(indiv_stats["step"], step_length_m, indiv_stats["seen_step"], MAX_STAT_SAMPLES)
                if speed_mps is not None:
                    indiv_stats["seen_speed"] += 1
                    reservoir_append(indiv_stats["speed"], speed_mps, indiv_stats["seen_speed"], MAX_STAT_SAMPLES)
            movement_by_fix_key[record["fix_key"]] = {
                "step_length_m": step_length_m,
                "speed_mps": speed_mps,
                "time_delta_s": time_delta_s,
            }
            previous = record

    return movement_by_fix_key, stat_samples


def _accumulate_segments(
    *,
    segments_by_id: dict[str, dict],
    row_index: int,
    fix_key: str,
    individual: str,
    set_name: str,
    time_ms: int,
    lon: float,
    lat: float,
    memberships: list[dict],
):
    for membership in memberships:
        segment_id = str(membership.get("segment_id", "")).strip()
        if not segment_id:
            continue
        segment = segments_by_id.setdefault(
            segment_id,
            {
                "segment_id": segment_id,
                "status": str(membership.get("status", "")).strip(),
                "issue_type": str(membership.get("issue_type", "")).strip(),
                "issue_note": str(membership.get("issue_note", "")).strip(),
                "owner_question": str(membership.get("owner_question", "")).strip(),
                "review_user": str(membership.get("review_user", "")).strip(),
                "reviewed_at": str(membership.get("reviewed_at", "")).strip(),
                "start_fix_key": str(membership.get("start_fix_key", "")).strip(),
                "end_fix_key": str(membership.get("end_fix_key", "")).strip(),
                "individual": individual,
                "set_name": set_name,
                "rows": [],
            },
        )
        segment["rows"].append(
            {
                "row_index": row_index,
                "fix_key": fix_key,
                "time_ms": int(time_ms),
                "position": [float(lon), float(lat)],
            }
        )


def _finalize_segments(segments_by_id: dict[str, dict]) -> list[dict]:
    segments = []
    for segment in segments_by_id.values():
        rows = sorted(
            segment.get("rows", []),
            key=lambda item: (item["time_ms"], item["row_index"], item["fix_key"]),
        )
        if not rows:
            continue
        segments.append(
            {
                "segment_id": segment["segment_id"],
                "individual": segment.get("individual", ""),
                "set_name": segment.get("set_name", "train") or "train",
                "start_fix_key": segment.get("start_fix_key") or rows[0]["fix_key"],
                "end_fix_key": segment.get("end_fix_key") or rows[-1]["fix_key"],
                "start_time_ms": int(rows[0]["time_ms"]),
                "end_time_ms": int(rows[-1]["time_ms"]),
                "fix_count": len(rows),
                "status": segment.get("status", ""),
                "issue_type": segment.get("issue_type", ""),
                "issue_note": segment.get("issue_note", ""),
                "owner_question": segment.get("owner_question", ""),
                "review_user": segment.get("review_user", ""),
                "reviewed_at": segment.get("reviewed_at", ""),
                "fix_keys": [row["fix_key"] for row in rows],
                "path": [row["position"] for row in rows],
            }
        )
    segments.sort(
        key=lambda item: (
            item["individual"],
            item["set_name"],
            item["start_time_ms"],
            item["segment_id"],
        )
    )
    return segments


def _build_auto_bursts(records: list[dict], *, burst_gap_seconds: float) -> list[dict]:
    gap_seconds = normalize_burst_gap_seconds(burst_gap_seconds)
    grouped: dict[tuple[str, str], list[dict]] = {}
    for record in records:
        grouped.setdefault((record["individual"], record["set_name"]), []).append(record)

    bursts: list[dict] = []
    for (individual, set_name), group_records in grouped.items():
        sorted_records = sorted(
            group_records,
            key=lambda item: (item["time_ms"], item["row_index"], item["fix_key"]),
        )
        burst_idx = -1
        current_rows: list[dict] = []
        previous_time_ms = None
        for record in sorted_records:
            starts_new = previous_time_ms is None or ((record["time_ms"] - previous_time_ms) / 1000.0) > gap_seconds
            if starts_new:
                if current_rows:
                    bursts.append(_finalize_auto_burst(individual, set_name, burst_idx, current_rows, gap_seconds))
                burst_idx += 1
                current_rows = []
            current_rows.append(record)
            previous_time_ms = record["time_ms"]
        if current_rows:
            bursts.append(_finalize_auto_burst(individual, set_name, burst_idx, current_rows, gap_seconds))

    bursts.sort(
        key=lambda item: (
            item["individual"],
            item["set_name"],
            item["start_time_ms"],
            item["burst_idx"],
        )
    )
    return bursts


def _finalize_auto_burst(
    individual: str,
    set_name: str,
    burst_idx: int,
    rows: list[dict],
    burst_gap_seconds: float,
) -> dict:
    burst_id = f"{individual}:{set_name}:burst_{int(burst_idx):06d}"
    return {
        "burst_id": burst_id,
        "burst_idx": int(burst_idx),
        "individual": individual,
        "set_name": set_name,
        "start_fix_key": rows[0]["fix_key"],
        "end_fix_key": rows[-1]["fix_key"],
        "start_time_ms": int(rows[0]["time_ms"]),
        "end_time_ms": int(rows[-1]["time_ms"]),
        "fix_count": len(rows),
        "burst_gap_seconds": float(burst_gap_seconds),
        "fix_keys": [row["fix_key"] for row in rows],
        "path": [row["position"] for row in rows],
    }


def _valid_movement_row(raw: dict, columns: dict[str, str | None]) -> dict | None:
    individual = str(raw.get(columns["individual"], "")).strip()
    if not individual:
        return None
    time_ms = parse_time_ms(raw.get(columns["time"]))
    if time_ms is None:
        return None
    lon = try_float(raw.get(columns["lon"]))
    lat = try_float(raw.get(columns["lat"]))
    if lon is None or lat is None or not is_valid_coordinate(lon, lat):
        return None
    set_name = str(raw.get(columns["set"], "")).strip().lower() if columns["set"] else "train"
    if set_name != "test":
        set_name = "train"
    fix_id = str(raw.get(columns["fix_id"], "")).strip() if columns["fix_id"] else ""
    common_name = str(raw.get(columns["common_name"], "")).strip() if columns["common_name"] else ""
    scientific_name = str(raw.get(columns["scientific_name"], "")).strip() if columns["scientific_name"] else ""
    return {
        "fix_id": fix_id,
        "individual": individual,
        "time_ms": int(time_ms),
        "lon": float(lon),
        "lat": float(lat),
        "set_name": set_name,
        "common_name": common_name,
        "scientific_name": scientific_name,
        "species": common_name or scientific_name or "Unknown species",
    }


def diagnose_track_topology(path: Path) -> dict:
    """Return lightweight topology diagnostics for movement CSV development checks."""
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        columns = detect_columns(fieldnames)
        if not columns["individual"] or not columns["time"] or not columns["lon"] or not columns["lat"]:
            raise ValueError("CSV is missing required columns for movement visualization")

        total_rows = 0
        valid_rows = 0
        duplicate_fix_ids: dict[str, int] = {}
        duplicate_track_timestamps: dict[tuple[str, str, int], int] = {}
        repeated_coordinates: dict[tuple[str, str, float, float], int] = {}
        records_by_group: dict[tuple[str, str], list[dict]] = {}
        previous_file_time_by_group: dict[tuple[str, str], int] = {}
        file_order_regressions_by_group: dict[tuple[str, str], int] = {}

        for row_index, raw in enumerate(reader, start=1):
            total_rows += 1
            valid = _valid_movement_row(raw, columns)
            if valid is None:
                continue

            individual = valid["individual"]
            set_name = valid["set_name"]
            time_ms = valid["time_ms"]
            lon = valid["lon"]
            lat = valid["lat"]
            group_key = _track_key(individual, set_name)
            fix_id = valid["fix_id"]
            fix_key = _make_fix_key(row_index, fix_id, individual, time_ms)

            valid_rows += 1
            if fix_id:
                duplicate_fix_ids[fix_id] = duplicate_fix_ids.get(fix_id, 0) + 1
            timestamp_key = (individual, set_name, time_ms)
            duplicate_track_timestamps[timestamp_key] = duplicate_track_timestamps.get(timestamp_key, 0) + 1
            coordinate_key = (individual, set_name, lon, lat)
            repeated_coordinates[coordinate_key] = repeated_coordinates.get(coordinate_key, 0) + 1

            previous_file_time = previous_file_time_by_group.get(group_key)
            if previous_file_time is not None and time_ms < previous_file_time:
                file_order_regressions_by_group[group_key] = file_order_regressions_by_group.get(group_key, 0) + 1
            previous_file_time_by_group[group_key] = time_ms

            records_by_group.setdefault(group_key, []).append(
                {
                    "row_index": row_index,
                    "fix_key": fix_key,
                    "individual": individual,
                    "set_name": set_name,
                    "time_ms": time_ms,
                    "lon": lon,
                    "lat": lat,
                    "position": [float(lon), float(lat)],
                }
            )

    coordinate_neighbors: dict[tuple[str, str, float, float], set[tuple[float, float]]] = {}
    max_fix_topological_degree = 0
    for (individual, set_name), sorted_records in _sorted_track_records(records_by_group):
        max_fix_topological_degree = max(max_fix_topological_degree, 2 if len(sorted_records) > 2 else max(0, len(sorted_records) - 1))
        for left, right in zip(sorted_records, sorted_records[1:]):
            left_coord = (individual, set_name, left["lon"], left["lat"])
            right_coord = (individual, set_name, right["lon"], right["lat"])
            if left_coord == right_coord:
                continue
            coordinate_neighbors.setdefault(left_coord, set()).add((right["lon"], right["lat"]))
            coordinate_neighbors.setdefault(right_coord, set()).add((left["lon"], left["lat"]))

    duplicate_fix_id_values = [count for count in duplicate_fix_ids.values() if count > 1]
    duplicate_timestamp_values = [count for count in duplicate_track_timestamps.values() if count > 1]
    repeated_coordinate_values = [count for count in repeated_coordinates.values() if count > 1]
    coordinate_degree_values = [len(neighbors) for neighbors in coordinate_neighbors.values()]
    coordinate_degree_gt2_values = [degree for degree in coordinate_degree_values if degree > 2]

    return {
        "total_rows": int(total_rows),
        "valid_rows": int(valid_rows),
        "track_count": int(len(records_by_group)),
        "duplicate_fix_id_count": int(len(duplicate_fix_id_values)),
        "max_duplicate_fix_id_count": int(max(duplicate_fix_id_values, default=1)),
        "duplicate_track_timestamp_count": int(len(duplicate_timestamp_values)),
        "max_duplicate_track_timestamp_count": int(max(duplicate_timestamp_values, default=1)),
        "file_order_regression_count": int(sum(file_order_regressions_by_group.values())),
        "file_order_regression_group_count": int(len(file_order_regressions_by_group)),
        "repeated_coordinate_count": int(len(repeated_coordinate_values)),
        "max_fixes_at_coordinate": int(max(repeated_coordinate_values, default=1)),
        "coordinate_degree_gt2_count": int(len(coordinate_degree_gt2_values)),
        "max_coordinate_degree": int(max(coordinate_degree_values, default=0)),
        "max_fix_topological_degree": int(max_fix_topological_degree),
    }


def build_movement_overview(
    path: Path,
    *,
    burst_gap_mode: str = DEFAULT_BURST_GAP_MODE,
    burst_gap_seconds: float = DEFAULT_BURST_GAP_SECONDS,
    burst_gap_quantile: float = DEFAULT_BURST_GAP_QUANTILE,
) -> dict:
    normalized_burst_gap_mode = normalize_burst_gap_mode(burst_gap_mode)
    normalized_burst_gap_seconds = normalize_burst_gap_seconds(burst_gap_seconds)
    normalized_burst_gap_quantile = normalize_burst_gap_quantile(burst_gap_quantile)
    path_str, mtime_ns, size = _cache_metadata(path)
    params = {
        "burst_gap_mode": normalized_burst_gap_mode,
        "burst_gap_seconds": normalized_burst_gap_seconds,
        "burst_gap_quantile": normalized_burst_gap_quantile,
    }
    cached = _load_cached_response(path, kind="overview", params=params, mtime_ns=mtime_ns, size=size)
    if cached is not None:
        return cached
    overview = _build_movement_overview_cached(
        path_str,
        mtime_ns,
        size,
        normalized_burst_gap_mode,
        normalized_burst_gap_seconds,
        normalized_burst_gap_quantile,
    )
    _save_cached_response(path, kind="overview", params=params, mtime_ns=mtime_ns, size=size, summary=overview)
    return overview


@lru_cache(maxsize=16)
def _build_movement_overview_cached(
    path_str: str,
    mtime_ns: int,
    size: int,
    burst_gap_mode: str,
    burst_gap_seconds: float,
    burst_gap_quantile: float,
) -> dict:
    path = Path(path_str)
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
    columns = detect_columns(fieldnames)
    if not columns["individual"] or not columns["time"] or not columns["lon"] or not columns["lat"]:
        raise ValueError("CSV is missing required columns for movement visualization")

    excluded_fields = {
        value
        for key, value in columns.items()
        if value and key not in ALL_REVIEW_COLUMNS
    }
    overview_quality_fields = [
        fieldname
        for fieldname in fieldnames
        if fieldname not in excluded_fields
        and fieldname not in REVIEW_COLUMNS
        and any(keyword in normalize_header(fieldname) for keyword in QUALITY_KEYWORDS)
    ]
    overview_field_stats = {
        fieldname: {
            "nonempty": 0,
            "numeric_count": 0,
            "bool_count": 0,
            "unique_values": set(),
        }
        for fieldname in overview_quality_fields
    }

    species_by_individual: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    track_records_by_group: dict[tuple[str, str], list[dict]] = {}
    review_counts = {"suspected": 0, "confirmed": 0}
    review_counts_by_individual: dict[str, dict[str, int]] = {}
    overview_fix_contexts: list[dict] = []
    overview_segments_by_id: dict[str, dict] = {}
    overview_truncated = False

    total_rows = 0
    min_lon = float("inf")
    max_lon = float("-inf")
    min_lat = float("inf")
    max_lat = float("-inf")
    min_time_ms = None
    max_time_ms = None

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row_index, raw in enumerate(reader, start=1):
            valid = _valid_movement_row(raw, columns)
            if valid is None:
                continue

            individual = valid["individual"]
            time_ms = valid["time_ms"]
            lon = valid["lon"]
            lat = valid["lat"]
            set_name = valid["set_name"]
            species_by_individual.setdefault(individual, valid["species"])
            total_rows += 1
            row_counts[individual] = row_counts.get(individual, 0) + 1

            min_lon = min(min_lon, lon)
            max_lon = max(max_lon, lon)
            min_lat = min(min_lat, lat)
            max_lat = max(max_lat, lat)
            min_time_ms = time_ms if min_time_ms is None else min(min_time_ms, time_ms)
            max_time_ms = time_ms if max_time_ms is None else max(max_time_ms, time_ms)

            for fieldname, stats in overview_field_stats.items():
                value = str(raw.get(fieldname, "")).strip()
                if not value:
                    continue
                stats["nonempty"] += 1
                if len(stats["unique_values"]) < 24:
                    stats["unique_values"].add(value)
                if try_float(value) is not None:
                    stats["numeric_count"] += 1
                if parse_bool(value) is not None:
                    stats["bool_count"] += 1

            review = _compact_review(raw)
            segment_memberships = _segment_memberships(raw)
            fix_key = _make_fix_key(row_index, valid["fix_id"], individual, time_ms)
            overview_record = {
                "row_index": row_index,
                "fix_key": fix_key,
                "individual": individual,
                "set_name": set_name,
                "time_ms": time_ms,
                "lon": lon,
                "lat": lat,
                "position": [float(lon), float(lat)],
            }
            track_records_by_group.setdefault(_track_key(individual, set_name), []).append(overview_record)

            if segment_memberships:
                _accumulate_segments(
                    segments_by_id=overview_segments_by_id,
                    row_index=row_index,
                    fix_key=fix_key,
                    individual=individual,
                    set_name=set_name,
                    time_ms=time_ms,
                    lon=lon,
                    lat=lat,
                    memberships=segment_memberships,
                )
            review_status = str(review.get("status", "")).strip().lower()
            if review_status in review_counts:
                review_counts[review_status] += 1
                individual_review_counts = review_counts_by_individual.setdefault(
                    individual,
                    {"suspected": 0, "confirmed": 0},
                )
                individual_review_counts[review_status] += 1
            if len(overview_fix_contexts) < DEFAULT_FIX_LIMIT:
                overview_fix_contexts.append(
                    {
                        "row_index": row_index,
                        "fix_key": fix_key,
                        "fix_id": valid["fix_id"],
                        "individual": individual,
                        "set_name": set_name,
                        "time_ms": time_ms,
                        "lon": lon,
                        "lat": lat,
                        "raw": raw,
                        "review": review,
                        "segment_memberships": segment_memberships,
                    }
                )
            elif not overview_truncated:
                overview_fix_contexts.clear()
                overview_truncated = True

    if total_rows == 0 or min_time_ms is None or max_time_ms is None:
        raise ValueError("CSV did not contain any valid movement rows")

    movement_by_fix_key, stat_samples = _compute_track_movement(track_records_by_group)

    color_fields = list(DERIVED_FIELDS)
    for review_field in REVIEW_COLUMNS:
        if columns.get(review_field):
            color_fields.append(
                {
                    "key": review_field,
                    "label": review_field,
                    "kind": "categorical",
                    "source": "review",
                    "column_name": review_field,
                }
            )
    for fieldname in overview_quality_fields:
        kind = _candidate_field_kind(overview_field_stats[fieldname])
        if not kind:
            continue
        color_fields.append(
            {
                "key": fieldname,
                "label": fieldname,
                "kind": kind,
                "source": "raw",
                "column_name": fieldname,
            }
        )

    overview_fixes = [] if overview_truncated else [
        _build_fix_record(
            row_index=context["row_index"],
            fix_id=context["fix_id"],
            individual=context["individual"],
            set_name=context["set_name"],
            time_ms=context["time_ms"],
            lon=context["lon"],
            lat=context["lat"],
            attributes=_build_attributes(
                context["raw"],
                color_fields=color_fields,
                step_length_m=movement_by_fix_key.get(context["fix_key"], {}).get("step_length_m"),
                speed_mps=movement_by_fix_key.get(context["fix_key"], {}).get("speed_mps"),
                time_delta_s=movement_by_fix_key.get(context["fix_key"], {}).get("time_delta_s"),
            ),
            review=context["review"],
            segment_memberships=context["segment_memberships"],
        )
        for context in sorted(overview_fix_contexts, key=_record_sort_key)
    ]
    overview_segments = _finalize_segments(overview_segments_by_id)
    burst_gap = resolve_burst_gap_strategy(
        track_records_by_group,
        burst_gap_mode=burst_gap_mode,
        burst_gap_seconds=burst_gap_seconds,
        burst_gap_quantile=burst_gap_quantile,
    )
    auto_bursts = [] if overview_truncated else _build_auto_bursts(
        [record for _, sorted_records in _sorted_track_records(track_records_by_group) for record in sorted_records],
        burst_gap_seconds=burst_gap["effective_seconds"],
    )

    individuals = sorted(row_counts)
    series_by_individual: dict[str, dict[str, dict[str, list]]] = {}
    coverage_by_individual: dict[str, dict[str, dict[str, int]]] = {}
    for (individual, set_name), sorted_records in _sorted_track_records(track_records_by_group):
        sorted_samples = _downsample_sorted_records(sorted_records, MAX_SERIES_POINTS)
        series_by_individual.setdefault(individual, {})[set_name] = {
            "times": [int(item["time_ms"]) for item in sorted_samples],
            "positions": [[float(item["lon"]), float(item["lat"])] for item in sorted_samples],
        }
        coverage_by_individual.setdefault(individual, {})[set_name] = {
            "start_ms": int(sorted_records[0]["time_ms"]),
            "end_ms": int(sorted_records[-1]["time_ms"]),
        }

    stats = {}
    for individual in individuals:
        interval_values = list(stat_samples.get(individual, {}).get("fix", []))
        step_values = list(stat_samples.get(individual, {}).get("step", []))
        speed_values = list(stat_samples.get(individual, {}).get("speed", []))
        individual_review_counts = review_counts_by_individual.get(individual, {})
        stats[individual] = {
            "row_count": int(row_counts.get(individual, 0)),
            "median_fix_s": median(interval_values),
            "median_step_m": median(step_values),
            "median_speed_mps": median(speed_values),
            "p95_step_m": quantile(step_values, 0.95),
            "p95_speed_mps": quantile(speed_values, 0.95),
            "suspected_count": int(individual_review_counts.get("suspected", 0)),
            "confirmed_count": int(individual_review_counts.get("confirmed", 0)),
        }

    span = max(max_lon - min_lon, max_lat - min_lat)
    return {
        "total_rows": int(total_rows),
        "columns": columns,
        "individuals": individuals,
        "species_by_individual": species_by_individual,
        "stats": stats,
        "coverage_by_individual": coverage_by_individual,
        "series_by_individual": series_by_individual,
        "color_fields": color_fields,
        "review_counts": review_counts,
        "fixes": overview_fixes,
        "segments": overview_segments,
        "auto_bursts": auto_bursts,
        "auto_bursts_truncated": bool(overview_truncated),
        "overview_truncated": bool(overview_truncated),
        "overview_fix_limit": int(DEFAULT_FIX_LIMIT),
        **_burst_gap_metadata(burst_gap),
        "initial_view": {
            "longitude": float((min_lon + max_lon) / 2),
            "latitude": float((min_lat + max_lat) / 2),
            "zoom": float(span_to_zoom(float(span))),
        },
        "min_time_ms": int(min_time_ms),
        "max_time_ms": int(max_time_ms),
        "detail_scope": {
            "individual": "",
            "individuals": [],
            "start_ms": None,
            "end_ms": None,
            "review_status": "reviewed",
            "limit": None,
            "burst_gap_mode": burst_gap["mode"],
            "burst_gap_seconds": float(burst_gap["effective_seconds"]),
            "burst_gap_fallback_seconds": float(burst_gap["fallback_seconds"]),
            "burst_gap_quantile": float(burst_gap["quantile"]),
            "burst_gap_gap_count": int(burst_gap["gap_count"]),
            "burst_gap_used_fallback": bool(burst_gap["used_fallback"]),
        },
        "detail_loaded": False,
    }


def build_movement_fixes(
    path: Path,
    *,
    individual: str = "",
    individuals: list[str] | tuple[str, ...] | set[str] | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
    review_status: str = "",
    limit: int | None = DEFAULT_FIX_LIMIT,
    burst_gap_mode: str = DEFAULT_BURST_GAP_MODE,
    burst_gap_seconds: float = DEFAULT_BURST_GAP_SECONDS,
    burst_gap_quantile: float = DEFAULT_BURST_GAP_QUANTILE,
) -> dict:
    normalized_burst_gap_mode = normalize_burst_gap_mode(burst_gap_mode)
    normalized_burst_gap_seconds = normalize_burst_gap_seconds(burst_gap_seconds)
    normalized_burst_gap_quantile = normalize_burst_gap_quantile(burst_gap_quantile)
    normalized_status = str(review_status or "").strip().lower()
    if normalized_status == "reviewed":
        normalized_status = "reviewed"
    elif normalized_status not in {"", "suspected", "confirmed"}:
        raise ValueError("Invalid review status")
    limit_value = None if limit is None else max(1, int(limit))
    normalized_individuals = normalize_individual_filters(individual=individual, individuals=individuals)
    params = {
        "individual": normalized_individuals[0] if len(normalized_individuals) == 1 else "",
        "individuals": normalized_individuals,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "review_status": normalized_status,
        "limit": limit_value,
        "burst_gap_mode": normalized_burst_gap_mode,
        "burst_gap_seconds": normalized_burst_gap_seconds,
        "burst_gap_quantile": normalized_burst_gap_quantile,
    }
    path_str, mtime_ns, size = _cache_metadata(path)
    cached = _load_cached_response(path, kind="fixes", params=params, mtime_ns=mtime_ns, size=size)
    if cached is not None:
        return cached
    payload = _build_movement_fixes_cached(
        path_str,
        mtime_ns,
        size,
        normalized_individuals,
        start_ms,
        end_ms,
        normalized_status,
        limit_value,
        normalized_burst_gap_mode,
        normalized_burst_gap_seconds,
        normalized_burst_gap_quantile,
    )
    _save_cached_response(path, kind="fixes", params=params, mtime_ns=mtime_ns, size=size, summary=payload)
    return payload


@lru_cache(maxsize=64)
def _build_movement_fixes_cached(
    path_str: str,
    mtime_ns: int,
    size: int,
    individuals: tuple[str, ...],
    start_ms: int | None,
    end_ms: int | None,
    review_status: str,
    limit: int | None,
    burst_gap_mode: str,
    burst_gap_seconds: float,
    burst_gap_quantile: float,
) -> dict:
    path = Path(path_str)
    fieldnames, columns, field_stats = _prepare_scan_context_cached(path_str, mtime_ns, size)
    color_fields = _build_color_fields(fieldnames, columns, field_stats)
    fixes: list[dict] = []
    segments_by_id: dict[str, dict] = {}
    auto_burst_records: list[dict] = []
    matching_fix_count = 0
    truncated = False
    records: list[dict] = []
    gap_records_by_group: dict[tuple[str, str], list[dict]] = {}
    individual_filters = set(individuals)

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row_index, raw in enumerate(reader, start=1):
            valid = _valid_movement_row(raw, columns)
            if valid is None:
                continue

            item_individual = valid["individual"]
            time_ms = valid["time_ms"]
            lon = valid["lon"]
            lat = valid["lat"]
            set_name = valid["set_name"]
            fix_key = _make_fix_key(row_index, valid["fix_id"], item_individual, time_ms)
            gap_records_by_group.setdefault(_track_key(item_individual, set_name), []).append(
                {
                    "row_index": row_index,
                    "fix_key": fix_key,
                    "individual": item_individual,
                    "set_name": set_name,
                    "time_ms": time_ms,
                }
            )
            if individual_filters and item_individual not in individual_filters:
                continue
            records.append(
                {
                    "row_index": row_index,
                    "fix_key": fix_key,
                    "fix_id": valid["fix_id"],
                    "individual": item_individual,
                    "set_name": set_name,
                    "time_ms": time_ms,
                    "lon": lon,
                    "lat": lat,
                    "position": [float(lon), float(lat)],
                    "raw": raw,
                    "review": _compact_review(raw),
                    "segment_memberships": _segment_memberships(raw),
                }
            )

    records_by_group = _group_track_records(records)
    movement_by_fix_key, _stat_samples = _compute_track_movement(records_by_group)
    burst_gap = resolve_burst_gap_strategy(
        gap_records_by_group,
        burst_gap_mode=burst_gap_mode,
        burst_gap_seconds=burst_gap_seconds,
        burst_gap_quantile=burst_gap_quantile,
    )

    for _group_key, sorted_records in _sorted_track_records(records_by_group):
        for record in sorted_records:
            time_ms = record["time_ms"]
            if start_ms is not None and time_ms < start_ms:
                continue
            if end_ms is not None and time_ms > end_ms:
                continue

            auto_burst_records.append(record)
            segment_memberships = record["segment_memberships"]
            if segment_memberships:
                _accumulate_segments(
                    segments_by_id=segments_by_id,
                    row_index=record["row_index"],
                    fix_key=record["fix_key"],
                    individual=record["individual"],
                    set_name=record["set_name"],
                    time_ms=record["time_ms"],
                    lon=record["lon"],
                    lat=record["lat"],
                    memberships=segment_memberships,
                )
            review = record["review"]
            status = str(review.get("status", "")).strip().lower()
            if review_status == "reviewed" and not review:
                continue
            if review_status in {"suspected", "confirmed"} and status != review_status:
                continue

            matching_fix_count += 1
            if limit is not None and len(fixes) >= limit:
                truncated = True
                continue

            fixes.append(
                _build_fix_record(
                    row_index=record["row_index"],
                    fix_id=record["fix_id"],
                    individual=record["individual"],
                    set_name=record["set_name"],
                    time_ms=record["time_ms"],
                    lon=record["lon"],
                    lat=record["lat"],
                    attributes=_build_attributes(
                        record["raw"],
                        color_fields=color_fields,
                        step_length_m=movement_by_fix_key.get(record["fix_key"], {}).get("step_length_m"),
                        speed_mps=movement_by_fix_key.get(record["fix_key"], {}).get("speed_mps"),
                        time_delta_s=movement_by_fix_key.get(record["fix_key"], {}).get("time_delta_s"),
                    ),
                    review=review,
                    segment_memberships=segment_memberships,
                )
            )

    return {
        "fixes": fixes,
        "segments": _finalize_segments(segments_by_id),
        "auto_bursts": _build_auto_bursts(auto_burst_records, burst_gap_seconds=burst_gap["effective_seconds"]),
        "matching_fix_count": int(matching_fix_count),
        "returned_fix_count": int(len(fixes)),
        "truncated": bool(truncated),
        **_burst_gap_metadata(burst_gap),
        "detail_scope": {
            "individual": individuals[0] if len(individuals) == 1 else "",
            "individuals": list(individuals),
            "start_ms": start_ms,
            "end_ms": end_ms,
            "review_status": review_status,
            "limit": limit,
            "burst_gap_mode": burst_gap["mode"],
            "burst_gap_seconds": float(burst_gap["effective_seconds"]),
            "burst_gap_fallback_seconds": float(burst_gap["fallback_seconds"]),
            "burst_gap_quantile": float(burst_gap["quantile"]),
            "burst_gap_gap_count": int(burst_gap["gap_count"]),
            "burst_gap_used_fallback": bool(burst_gap["used_fallback"]),
        },
        "detail_loaded": True,
    }


def build_movement_summary(path: Path) -> dict:
    overview = build_movement_overview(path)
    full_detail = build_movement_fixes(path, limit=None)
    payload = dict(overview)
    payload["fixes"] = full_detail["fixes"]
    payload["segments"] = full_detail["segments"]
    payload["auto_bursts"] = full_detail["auto_bursts"]
    payload["detail_scope"] = full_detail["detail_scope"]
    payload["detail_loaded"] = True
    return payload
