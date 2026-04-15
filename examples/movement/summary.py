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
SUMMARY_CACHE_VERSION = 7


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
        **{name: normalized.get(normalize_header(name)) for name in REVIEW_COLUMNS},
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
    payload = {"kind": kind, "params": params or {}}
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
    if fieldname in REVIEW_COLUMNS:
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
        if value and key not in REVIEW_COLUMNS
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
        if fieldname in excluded_fields or fieldname in REVIEW_COLUMNS:
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
    return fix


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
        "species": common_name or scientific_name or "Unknown species",
    }


def build_movement_overview(path: Path) -> dict:
    path_str, mtime_ns, size = _cache_metadata(path)
    params = {}
    cached = _load_cached_response(path, kind="overview", params=params, mtime_ns=mtime_ns, size=size)
    if cached is not None:
        return cached
    overview = _build_movement_overview_cached(path_str, mtime_ns, size)
    _save_cached_response(path, kind="overview", params=params, mtime_ns=mtime_ns, size=size, summary=overview)
    return overview


@lru_cache(maxsize=16)
def _build_movement_overview_cached(path_str: str, mtime_ns: int, size: int) -> dict:
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
        if value and key not in REVIEW_COLUMNS
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
    group_samples: dict[tuple[str, str], dict] = {}
    stat_samples: dict[str, dict[str, list[float] | int]] = {}
    review_counts = {"suspected": 0, "confirmed": 0}
    review_counts_by_individual: dict[str, dict[str, int]] = {}
    overview_fix_contexts: list[dict] = []

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

            group_key = (individual, set_name)
            group = group_samples.setdefault(
                group_key,
                {
                    "seen": 0,
                    "samples": [],
                    "start_ms": time_ms,
                    "end_ms": time_ms,
                    "prev": None,
                },
            )
            group["seen"] += 1
            group["start_ms"] = min(group["start_ms"], time_ms)
            group["end_ms"] = max(group["end_ms"], time_ms)
            reservoir_append(group["samples"], (time_ms, lon, lat), group["seen"], MAX_SERIES_POINTS)

            indiv_stats = stat_samples.setdefault(
                individual,
                {"seen_fix": 0, "seen_step": 0, "seen_speed": 0, "fix": [], "step": [], "speed": []},
            )
            step_length_m = None
            time_delta_s = None
            speed_mps = None
            previous = group["prev"]
            if previous and time_ms > previous["time_ms"]:
                time_delta_s = (time_ms - previous["time_ms"]) / 1000.0
                step_length_m = haversine_meters(previous["lon"], previous["lat"], lon, lat)
                speed_mps = step_length_m / time_delta_s if time_delta_s > 0 else None
                indiv_stats["seen_fix"] += 1
                indiv_stats["seen_step"] += 1
                reservoir_append(indiv_stats["fix"], time_delta_s, indiv_stats["seen_fix"], MAX_STAT_SAMPLES)
                reservoir_append(indiv_stats["step"], step_length_m, indiv_stats["seen_step"], MAX_STAT_SAMPLES)
                if speed_mps is not None:
                    indiv_stats["seen_speed"] += 1
                    reservoir_append(indiv_stats["speed"], speed_mps, indiv_stats["seen_speed"], MAX_STAT_SAMPLES)
            group["prev"] = {"time_ms": time_ms, "lon": lon, "lat": lat}

            review = _compact_review(raw)
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
                        "fix_id": valid["fix_id"],
                        "individual": individual,
                        "set_name": set_name,
                        "time_ms": time_ms,
                        "lon": lon,
                        "lat": lat,
                        "raw": raw,
                        "step_length_m": step_length_m,
                        "speed_mps": speed_mps,
                        "time_delta_s": time_delta_s,
                        "review": review,
                    }
                )

    if total_rows == 0 or min_time_ms is None or max_time_ms is None:
        raise ValueError("CSV did not contain any valid movement rows")

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

    overview_fixes = [
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
                step_length_m=context["step_length_m"],
                speed_mps=context["speed_mps"],
                time_delta_s=context["time_delta_s"],
            ),
            review=context["review"],
        )
        for context in overview_fix_contexts
    ]

    individuals = sorted(row_counts)
    series_by_individual: dict[str, dict[str, dict[str, list]]] = {}
    coverage_by_individual: dict[str, dict[str, dict[str, int]]] = {}
    for (individual, set_name), payload in group_samples.items():
        sorted_samples = sorted(payload["samples"], key=lambda item: item[0])
        series_by_individual.setdefault(individual, {})[set_name] = {
            "times": [int(item[0]) for item in sorted_samples],
            "positions": [[float(item[1]), float(item[2])] for item in sorted_samples],
        }
        coverage_by_individual.setdefault(individual, {})[set_name] = {
            "start_ms": int(payload["start_ms"]),
            "end_ms": int(payload["end_ms"]),
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
) -> dict:
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
) -> dict:
    path = Path(path_str)
    fieldnames, columns, field_stats = _prepare_scan_context_cached(path_str, mtime_ns, size)
    color_fields = _build_color_fields(fieldnames, columns, field_stats)
    fixes: list[dict] = []
    matching_fix_count = 0
    truncated = False
    previous_by_group: dict[tuple[str, str], dict] = {}
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
            group_key = (item_individual, set_name)
            previous = previous_by_group.get(group_key)
            step_length_m = None
            time_delta_s = None
            speed_mps = None
            if previous and time_ms > previous["time_ms"]:
                time_delta_s = (time_ms - previous["time_ms"]) / 1000.0
                step_length_m = haversine_meters(previous["lon"], previous["lat"], lon, lat)
                speed_mps = step_length_m / time_delta_s if time_delta_s > 0 else None
            previous_by_group[group_key] = {"time_ms": time_ms, "lon": lon, "lat": lat}

            review = _compact_review(raw)
            status = str(review.get("status", "")).strip().lower()
            if individual_filters and item_individual not in individual_filters:
                continue
            if start_ms is not None and time_ms < start_ms:
                continue
            if end_ms is not None and time_ms > end_ms:
                continue
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
                    row_index=row_index,
                    fix_id=valid["fix_id"],
                    individual=item_individual,
                    set_name=set_name,
                    time_ms=time_ms,
                    lon=lon,
                    lat=lat,
                    attributes=_build_attributes(
                        raw,
                        color_fields=color_fields,
                        step_length_m=step_length_m,
                        speed_mps=speed_mps,
                        time_delta_s=time_delta_s,
                    ),
                    review=review,
                )
            )

    return {
        "fixes": fixes,
        "matching_fix_count": int(matching_fix_count),
        "returned_fix_count": int(len(fixes)),
        "truncated": bool(truncated),
        "detail_scope": {
            "individual": individuals[0] if len(individuals) == 1 else "",
            "individuals": list(individuals),
            "start_ms": start_ms,
            "end_ms": end_ms,
            "review_status": review_status,
            "limit": limit,
        },
        "detail_loaded": True,
    }


def build_movement_summary(path: Path) -> dict:
    overview = build_movement_overview(path)
    full_detail = build_movement_fixes(path, limit=None)
    payload = dict(overview)
    payload["fixes"] = full_detail["fixes"]
    payload["detail_scope"] = full_detail["detail_scope"]
    payload["detail_loaded"] = True
    return payload
