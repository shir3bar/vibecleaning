import csv
from datetime import datetime
from functools import lru_cache
from math import isfinite
from pathlib import Path

from .bursts import (
    DEFAULT_BURST_GAP_MODE,
    DEFAULT_BURST_GAP_QUANTILE,
    DEFAULT_BURST_GAP_SECONDS,
    build_auto_bursts,
    burst_gap_metadata,
    normalize_burst_gap_mode,
    normalize_burst_gap_quantile,
    normalize_burst_gap_seconds,
    resolve_burst_gap_strategy,
)
from .movement_features import compute_track_movement


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
DEFAULT_OVERVIEW_FIX_LIMIT = 25000
DEFAULT_FIX_LIMIT = 1000000


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


def _attribute_field_kind(fieldname: str, stats: dict) -> str | None:
    kind = _candidate_field_kind(stats)
    if kind is None and str(fieldname).lower().startswith("osm:") and stats["nonempty"] > 0:
        return "categorical"
    return kind


def _should_include_quality_field(fieldname: str, stats: dict) -> bool:
    normalized = normalize_header(fieldname)
    if str(fieldname).lower().startswith("osm:"):
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


@lru_cache(maxsize=4)
def _prepare_scan_context_cached(path_str: str, _mtime_ns: int, _size: int):
    return _prepare_scan_context(Path(path_str))


def _build_color_fields(fieldnames: list[str], columns: dict[str, str | None], field_stats: dict[str, dict]) -> list[dict]:
    excluded_fields = {
        value
        for value in columns.values()
        if value
    }
    color_fields = list(DERIVED_FIELDS)

    for fieldname in fieldnames:
        if fieldname in excluded_fields:
            continue
        stats = field_stats[fieldname]
        if not _should_include_quality_field(fieldname, stats):
            continue
        kind = _attribute_field_kind(fieldname, stats)
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
    status = _normalize_review_status(raw.get("outlier_status"))
    if not status:
        return {}
    review = {
        "status": status,
        "issue_type": str(raw.get("outlier_issue_type", "")).strip(),
        "comments": str(raw.get("outlier_comments", "")).strip(),
    }
    return {key: value for key, value in review.items() if _is_present(value)}


def _portable_row_is_visible(raw: dict) -> bool:
    value = str(raw.get("visible") or "").strip().lower()
    return value not in {"false", "f", "no", "n", "0"}


def _row_is_analytically_excluded(
    raw: dict,
    *,
    fix_key: str,
    individual: str,
    set_name: str,
    confirmed_fix_keys: set[str],
    confirmed_individual_tracks: set[tuple[str, str]],
) -> bool:
    review_status = _normalize_review_status(raw.get("outlier_status"))
    if review_status == "confirmed":
        return True
    if fix_key in confirmed_fix_keys:
        return True
    if (
        (individual, "") in confirmed_individual_tracks
        or (individual, set_name) in confirmed_individual_tracks
    ):
        return True
    if review_status == "suspected":
        return False
    return not _portable_row_is_visible(raw)


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
    analytically_excluded: bool = False,
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
    if analytically_excluded:
        fix["analytically_excluded"] = True
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
    confirmed_fix_keys: list[str] | tuple[str, ...] | set[str] | None = None,
    confirmed_individual_tracks: list[tuple[str, str]] | tuple[tuple[str, str], ...] | set[tuple[str, str]] | None = None,
    burst_gap_mode: str = DEFAULT_BURST_GAP_MODE,
    burst_gap_seconds: float = DEFAULT_BURST_GAP_SECONDS,
    burst_gap_quantile: float = DEFAULT_BURST_GAP_QUANTILE,
) -> dict:
    normalized_burst_gap_mode = normalize_burst_gap_mode(burst_gap_mode)
    normalized_burst_gap_seconds = normalize_burst_gap_seconds(burst_gap_seconds)
    normalized_burst_gap_quantile = normalize_burst_gap_quantile(burst_gap_quantile)
    normalized_confirmed_fix_keys = tuple(sorted({
        str(item).strip() for item in (confirmed_fix_keys or []) if str(item).strip()
    }))
    normalized_confirmed_individual_tracks = tuple(sorted({
        (str(item[0]).strip(), str(item[1]).strip())
        for item in (confirmed_individual_tracks or [])
        if isinstance(item, (list, tuple)) and len(item) == 2 and str(item[0]).strip()
    }))
    overview_fix_limit = max(0, int(DEFAULT_OVERVIEW_FIX_LIMIT))
    path_str, mtime_ns, size = _cache_metadata(path)
    return _build_movement_overview_cached(
        path_str,
        mtime_ns,
        size,
        normalized_confirmed_fix_keys,
        normalized_confirmed_individual_tracks,
        normalized_burst_gap_mode,
        normalized_burst_gap_seconds,
        normalized_burst_gap_quantile,
        overview_fix_limit,
    )


@lru_cache(maxsize=1)
def _build_movement_overview_cached(
    path_str: str,
    mtime_ns: int,
    size: int,
    confirmed_fix_keys: tuple[str, ...],
    confirmed_individual_tracks: tuple[tuple[str, str], ...],
    burst_gap_mode: str,
    burst_gap_seconds: float,
    burst_gap_quantile: float,
    overview_fix_limit: int,
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
        for value in columns.values()
        if value
    }
    overview_quality_fields = [
        fieldname
        for fieldname in fieldnames
        if fieldname not in excluded_fields
        and (
            str(fieldname).lower().startswith("osm:")
            or any(keyword in normalize_header(fieldname) for keyword in QUALITY_KEYWORDS)
        )
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
    eligible_track_records_by_group: dict[tuple[str, str], list[dict]] = {}
    review_counts = {"suspected": 0, "confirmed": 0}
    review_counts_by_individual: dict[str, dict[str, int]] = {}
    overview_fix_contexts: list[dict] = []
    overview_segments_by_id: dict[str, dict] = {}
    overview_truncated = False
    confirmed_fix_key_set = set(confirmed_fix_keys)
    confirmed_individual_track_set = set(confirmed_individual_tracks)

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
            segment_memberships = []
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
            analytically_excluded = _row_is_analytically_excluded(
                raw,
                fix_key=fix_key,
                individual=individual,
                set_name=set_name,
                confirmed_fix_keys=confirmed_fix_key_set,
                confirmed_individual_tracks=confirmed_individual_track_set,
            )
            if not analytically_excluded:
                eligible_track_records_by_group.setdefault(
                    _track_key(individual, set_name),
                    [],
                ).append(overview_record)

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
            if len(overview_fix_contexts) < overview_fix_limit:
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
                        "analytically_excluded": analytically_excluded,
                    }
                )
            elif not overview_truncated:
                overview_truncated = True

    if total_rows == 0 or min_time_ms is None or max_time_ms is None:
        raise ValueError("CSV did not contain any valid movement rows")

    movement_by_fix_key, stat_samples = compute_track_movement(eligible_track_records_by_group)

    color_fields = list(DERIVED_FIELDS)
    for fieldname in overview_quality_fields:
        kind = _attribute_field_kind(fieldname, overview_field_stats[fieldname])
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
                step_length_m=movement_by_fix_key.get(context["fix_key"], {}).get("step_length_m"),
                speed_mps=movement_by_fix_key.get(context["fix_key"], {}).get("speed_mps"),
                time_delta_s=movement_by_fix_key.get(context["fix_key"], {}).get("time_delta_s"),
            ),
            review=context["review"],
            segment_memberships=context["segment_memberships"],
            analytically_excluded=context["analytically_excluded"],
        )
        for context in sorted(overview_fix_contexts, key=_record_sort_key)
    ]
    overview_segments = _finalize_segments(overview_segments_by_id)
    burst_gap = resolve_burst_gap_strategy(
        eligible_track_records_by_group,
        burst_gap_mode=burst_gap_mode,
        burst_gap_seconds=burst_gap_seconds,
        burst_gap_quantile=burst_gap_quantile,
    )
    auto_bursts = [] if overview_truncated else build_auto_bursts(
        [
            record
            for _, sorted_records in _sorted_track_records(eligible_track_records_by_group)
            for record in sorted_records
        ],
        burst_gap_seconds=burst_gap["effective_seconds"],
    )

    individuals = sorted(row_counts)
    series_by_individual: dict[str, dict[str, dict[str, list]]] = {}
    coverage_by_individual: dict[str, dict[str, dict[str, int]]] = {}
    for (individual, set_name), sorted_records in _sorted_track_records(eligible_track_records_by_group):
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
        "overview_fix_limit": int(overview_fix_limit),
        **burst_gap_metadata(burst_gap),
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
    additional_review_fix_keys: list[str] | tuple[str, ...] | set[str] | None = None,
    additional_review_individuals: list[str] | tuple[str, ...] | set[str] | None = None,
    confirmed_fix_keys: list[str] | tuple[str, ...] | set[str] | None = None,
    confirmed_individual_tracks: list[tuple[str, str]] | tuple[tuple[str, str], ...] | set[tuple[str, str]] | None = None,
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
    normalized_review_fix_keys = tuple(sorted({
        str(item).strip()
        for item in (additional_review_fix_keys or [])
        if str(item).strip()
    }))
    normalized_review_individuals = tuple(sorted({
        str(item).strip()
        for item in (additional_review_individuals or [])
        if str(item).strip()
    }))
    normalized_confirmed_fix_keys = tuple(sorted({
        str(item).strip() for item in (confirmed_fix_keys or []) if str(item).strip()
    }))
    normalized_confirmed_individual_tracks = tuple(sorted({
        (str(item[0]).strip(), str(item[1]).strip())
        for item in (confirmed_individual_tracks or [])
        if isinstance(item, (list, tuple)) and len(item) == 2 and str(item[0]).strip()
    }))
    path_str, mtime_ns, size = _cache_metadata(path)
    return _build_movement_fixes(
        path_str,
        mtime_ns,
        size,
        normalized_individuals,
        normalized_review_fix_keys,
        normalized_review_individuals,
        normalized_confirmed_fix_keys,
        normalized_confirmed_individual_tracks,
        start_ms,
        end_ms,
        normalized_status,
        limit_value,
        normalized_burst_gap_mode,
        normalized_burst_gap_seconds,
        normalized_burst_gap_quantile,
    )


def _build_movement_fixes(
    path_str: str,
    mtime_ns: int,
    size: int,
    individuals: tuple[str, ...],
    additional_review_fix_keys: tuple[str, ...],
    additional_review_individuals: tuple[str, ...],
    confirmed_fix_keys: tuple[str, ...],
    confirmed_individual_tracks: tuple[tuple[str, str], ...],
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
    additional_review_fix_key_set = set(additional_review_fix_keys)
    additional_review_individual_set = set(additional_review_individuals)
    confirmed_fix_key_set = set(confirmed_fix_keys)
    confirmed_individual_track_set = set(confirmed_individual_tracks)

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
            analytically_excluded = _row_is_analytically_excluded(
                raw,
                fix_key=fix_key,
                individual=item_individual,
                set_name=set_name,
                confirmed_fix_keys=confirmed_fix_key_set,
                confirmed_individual_tracks=confirmed_individual_track_set,
            )
            if not analytically_excluded:
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
                    "segment_memberships": [],
                    "analytically_excluded": analytically_excluded,
                }
            )

    records_by_group = _group_track_records(records)
    eligible_records = [record for record in records if not record["analytically_excluded"]]
    eligible_records_by_group = _group_track_records(eligible_records)
    movement_by_fix_key, _stat_samples = compute_track_movement(eligible_records_by_group)
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

            if not record["analytically_excluded"]:
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
            is_additional_review_candidate = (
                record["fix_key"] in additional_review_fix_key_set
                or record["individual"] in additional_review_individual_set
            )
            if review_status == "reviewed" and not review and not is_additional_review_candidate:
                continue
            if (
                review_status in {"suspected", "confirmed"}
                and status != review_status
                and not is_additional_review_candidate
            ):
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
                    analytically_excluded=record["analytically_excluded"],
                )
            )

    return {
        "fixes": fixes,
        "segments": _finalize_segments(segments_by_id),
        "auto_bursts": build_auto_bursts(auto_burst_records, burst_gap_seconds=burst_gap["effective_seconds"]),
        "matching_fix_count": int(matching_fix_count),
        "returned_fix_count": int(len(fixes)),
        "truncated": bool(truncated),
        **burst_gap_metadata(burst_gap),
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


def build_movement_summary(
    path: Path,
    *,
    confirmed_fix_keys: list[str] | tuple[str, ...] | set[str] | None = None,
    confirmed_individual_tracks: list[tuple[str, str]] | tuple[tuple[str, str], ...] | set[tuple[str, str]] | None = None,
) -> dict:
    overview = build_movement_overview(
        path,
        confirmed_fix_keys=confirmed_fix_keys,
        confirmed_individual_tracks=confirmed_individual_tracks,
    )
    full_detail = build_movement_fixes(
        path,
        limit=None,
        confirmed_fix_keys=confirmed_fix_keys,
        confirmed_individual_tracks=confirmed_individual_tracks,
    )
    payload = dict(overview)
    payload["fixes"] = full_detail["fixes"]
    payload["segments"] = full_detail["segments"]
    payload["auto_bursts"] = full_detail["auto_bursts"]
    payload["detail_scope"] = full_detail["detail_scope"]
    payload["detail_loaded"] = True
    return payload
