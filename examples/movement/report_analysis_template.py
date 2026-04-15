import base64
import csv
import html
import json
import os
from datetime import datetime, timezone
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


def normalize_header(header):
    return str(header or "").lower().replace("-", "").replace("_", "").replace(":", "").replace(" ", "")


def find_column(normalized_map, aliases):
    for alias in aliases:
        if alias in normalized_map:
            return normalized_map[alias]
    return None


def detect_columns(fieldnames):
    normalized = {normalize_header(name): name for name in fieldnames}
    columns = {
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
        "study_name": find_column(normalized, ["studyname"]),
        "study_id": find_column(normalized, ["studyid"]),
        "animal_id": find_column(normalized, [
            "individuallocalidentifier",
            "animalid",
            "individualid",
            "individual",
            "trackid",
            "taglocalidentifier",
        ]),
        "source": find_column(normalized, [
            "source",
            "datasource",
            "datasetsource",
            "importsource",
            "recordsource",
        ]),
        "burst": find_column(normalized, [
            "burst",
            "burstid",
            "burstidentifier",
            "burstgroup",
            "burstindex",
        ]),
        "set": find_column(normalized, ["set", "split", "partition"]),
    }
    for name in REVIEW_COLUMNS:
        columns[name] = normalized.get(normalize_header(name))
    return columns


def parse_time_ms(raw_value):
    value = str(raw_value or "").strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsers = (
        lambda item: datetime.fromisoformat(item),
        lambda item: datetime.strptime(item, "%Y-%m-%d %H:%M:%S"),
        lambda item: datetime.strptime(item, "%Y-%m-%d %H:%M:%S.%f"),
    )
    for parser in parsers:
        try:
            return int(parser(normalized).timestamp() * 1000)
        except ValueError:
            continue
    return None


def try_float(raw_value):
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def is_valid_coordinate(lon, lat):
    return lon is not None and lat is not None and -180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0


def make_fix_key(row_index, fix_id, fix_id_is_unique, individual, time_ms):
    if fix_id:
        return f"id:{fix_id}#row:{row_index}"
    return f"row:{row_index}|{individual}|{time_ms}"


def format_timestamp(time_ms):
    return datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc).isoformat()


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_review_status(raw_value):
    value = str(raw_value or "").strip().lower()
    return value if value in {"suspected", "confirmed"} else ""


def clean_issue_payload(item, fallback_status=""):
    status = normalize_review_status(item.get("status")) or normalize_review_status(fallback_status)
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
    cleaned = {key: value for key, value in issue.items() if value}
    if not cleaned.get("issue_id") and not cleaned.get("issue_type"):
        return {}
    return cleaned


def parse_issue_refs(raw):
    row_status = normalize_review_status(raw.get("vc_outlier_status"))
    raw_refs = str(raw.get("vc_issue_refs", "")).strip()
    if raw_refs:
        try:
            parsed = json.loads(raw_refs)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            issues = [clean_issue_payload(item, row_status) for item in parsed if isinstance(item, dict)]
            issues = [item for item in issues if item]
            if issues:
                return issues
    legacy = clean_issue_payload({
        "status": raw.get("vc_outlier_status"),
        "issue_id": raw.get("vc_issue_id"),
        "issue_type": raw.get("vc_issue_type"),
        "issue_field": raw.get("vc_issue_field"),
        "issue_threshold": raw.get("vc_issue_threshold"),
        "issue_note": raw.get("vc_issue_note"),
        "owner_question": raw.get("vc_owner_question"),
        "review_user": raw.get("vc_review_user"),
        "reviewed_at": raw.get("vc_reviewed_at"),
    }, row_status)
    return [legacy] if legacy else []


def extract_quality_fields(fieldnames, columns):
    excluded = {
        value
        for key, value in columns.items()
        if value and key not in REVIEW_COLUMNS
    }
    result = []
    for name in fieldnames:
        if name in excluded or name in REVIEW_COLUMNS:
            continue
        normalized = normalize_header(name)
        if any(keyword in normalized for keyword in QUALITY_KEYWORDS):
            result.append(name)
    return result


def load_rows_with_context(source_path):
    with Path(source_path).open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            raise SystemExit("CSV did not contain a header row")
        columns = detect_columns(fieldnames)
        if not columns["individual"] or not columns["time"] or not columns["lon"] or not columns["lat"]:
            raise SystemExit("CSV is missing required columns for movement visualization")
        rows = list(reader)

    valid_records = []
    fix_id_counts = {}
    for row_index, raw in enumerate(rows, start=1):
        individual = str(raw.get(columns["individual"], "")).strip()
        if not individual:
            continue
        time_ms = parse_time_ms(raw.get(columns["time"]))
        lon = try_float(raw.get(columns["lon"]))
        lat = try_float(raw.get(columns["lat"]))
        if time_ms is None or not is_valid_coordinate(lon, lat):
            continue
        fix_id = str(raw.get(columns["fix_id"], "")).strip() if columns["fix_id"] else ""
        if fix_id:
            fix_id_counts[fix_id] = fix_id_counts.get(fix_id, 0) + 1

    previous_by_group = {}
    for row_index, raw in enumerate(rows, start=1):
        individual = str(raw.get(columns["individual"], "")).strip()
        if not individual:
            continue
        time_ms = parse_time_ms(raw.get(columns["time"]))
        lon = try_float(raw.get(columns["lon"]))
        lat = try_float(raw.get(columns["lat"]))
        if time_ms is None or not is_valid_coordinate(lon, lat):
            continue

        set_name = str(raw.get(columns["set"], "")).strip().lower() if columns["set"] else "train"
        if set_name != "test":
            set_name = "train"
        group_key = (individual, set_name)
        previous = previous_by_group.get(group_key)
        time_delta_s = None
        step_length_m = None
        speed_mps = None
        if previous and time_ms > previous["time_ms"]:
            time_delta_s = (time_ms - previous["time_ms"]) / 1000.0
            from math import atan2, cos, radians, sin, sqrt
            phi1 = radians(previous["lat"])
            phi2 = radians(lat)
            delta_phi = radians(lat - previous["lat"])
            delta_lambda = radians(lon - previous["lon"])
            a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
            step_length_m = 6371000.0 * 2 * atan2(sqrt(a), sqrt(1 - a))
            speed_mps = step_length_m / time_delta_s if time_delta_s > 0 else None
        previous_by_group[group_key] = {"time_ms": time_ms, "lon": lon, "lat": lat}

        fix_id = str(raw.get(columns["fix_id"], "")).strip() if columns["fix_id"] else ""
        fix_key = make_fix_key(row_index, fix_id, bool(fix_id) and fix_id_counts.get(fix_id, 0) == 1, individual, time_ms)
        review = {}
        for name in REVIEW_COLUMNS:
            review[name] = str(raw.get(name, "")).strip()
        review["issues"] = parse_issue_refs(raw)
        valid_records.append(
            {
                "row_index": row_index,
                "fix_key": fix_key,
                "individual": individual,
                "set_name": set_name,
                "time_ms": time_ms,
                "lon": lon,
                "lat": lat,
                "time_text": str(raw.get(columns["time"], "")).strip(),
                "raw": dict(raw),
                "review": review,
                "step_length_m": step_length_m,
                "speed_mps": speed_mps,
                "time_delta_s": time_delta_s,
            }
        )
    return fieldnames, columns, rows, valid_records


def selected_contexts(valid_records, selected_fix_keys=None, selected_issue_ids=None):
    fix_keys = set(selected_fix_keys or [])
    issue_ids = set(selected_issue_ids or [])
    result = []
    for record in valid_records:
        if issue_ids and set(issue_ids_for(record)).intersection(issue_ids):
            result.append(record)
            continue
        if fix_keys and record["fix_key"] in fix_keys:
            result.append(record)
    return result


def write_json(path, payload):
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def decode_data_url(data_url):
    raw = str(data_url or "").strip()
    if not raw:
        raise ValueError("Missing snapshot data")
    if "," in raw:
        _, encoded = raw.split(",", 1)
    else:
        encoded = raw
    return base64.b64decode(encoded)


def html_escape(value):
    return html.escape(str(value or ""), quote=True)
MAX_EXAMPLES_PER_ISSUE = 6


def issue_type_for(record):
    if not normalize_review_status(record["review"].get("vc_outlier_status")):
        return "Unspecified issue"
    return record["review"].get("vc_issue_type", "").strip() or "Unspecified issue"


def issue_types_for(record):
    issues = record["review"].get("issues", [])
    issue_types = sorted({
        str(item.get("issue_type", "")).strip() or "Unspecified issue"
        for item in issues
    })
    return issue_types or [issue_type_for(record)]


def issue_id_for(record):
    return record["review"].get("vc_issue_id", "").strip()


def issue_ids_for(record):
    issues = record["review"].get("issues", [])
    issue_ids = sorted({
        str(item.get("issue_id", "")).strip()
        for item in issues
        if str(item.get("issue_id", "")).strip()
    })
    if issue_ids:
        return issue_ids
    if not normalize_review_status(record["review"].get("vc_outlier_status")):
        return []
    return [issue_id_for(record)] if issue_id_for(record) else []


def issue_field_for(record):
    if not normalize_review_status(record["review"].get("vc_outlier_status")):
        return ""
    return record["review"].get("vc_issue_field", "").strip()


def issue_threshold_for_type(record, issue_type):
    for item in record["review"].get("issues", []):
        item_issue_type = str(item.get("issue_type", "")).strip() or "Unspecified issue"
        if item_issue_type == issue_type:
            return str(item.get("issue_threshold", "")).strip()
    if not normalize_review_status(record["review"].get("vc_outlier_status")):
        return ""
    return str(record["review"].get("vc_issue_threshold", "")).strip()


def issue_field_for_type(record, issue_type):
    for item in record["review"].get("issues", []):
        item_issue_type = str(item.get("issue_type", "")).strip() or "Unspecified issue"
        if item_issue_type == issue_type:
            return str(item.get("issue_field", "")).strip()
    return issue_field_for(record)


def issue_detail_values_for_type(record, issue_type, key):
    values = []
    for item in record["review"].get("issues", []):
        item_issue_type = str(item.get("issue_type", "")).strip() or "Unspecified issue"
        if item_issue_type != issue_type:
            continue
        value = str(item.get(key, "")).strip()
        if value:
            values.append(value)
    if values:
        return values
    fallback_key = {
        "issue_note": "vc_issue_note",
        "owner_question": "vc_owner_question",
    }.get(key, "")
    if not normalize_review_status(record["review"].get("vc_outlier_status")):
        return []
    fallback_value = str(record["review"].get(fallback_key, "")).strip() if fallback_key else ""
    return [fallback_value] if fallback_value else []


def status_for(record):
    return record["review"].get("vc_outlier_status", "").strip().lower() or "unreviewed"


def format_metric(value, digits):
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def quantile(sorted_values, q):
    if not sorted_values:
        return 0
    idx = (len(sorted_values) - 1) * q
    lower = int(idx)
    upper = min(len(sorted_values) - 1, lower + 1)
    if lower == upper:
        return sorted_values[lower]
    ratio = idx - lower
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * ratio


def format_issue_ids(issue_ids):
    return ", ".join(issue_ids) if issue_ids else "none yet"


def format_status_counts(status_counts):
    if not status_counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(status_counts.items()))


def most_common_non_empty(values, fallback):
    counts = {}
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        counts[text] = counts.get(text, 0) + 1
    if not counts:
        return fallback
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def summarize_status_counts(records):
    counts = {}
    for record in records:
        status = status_for(record)
        counts[status] = counts.get(status, 0) + 1
    return counts


def build_quality_lines(records, quality_fields, limit=4):
    lines = []
    for name in quality_fields[:6]:
        values = sorted({str(record["raw"].get(name, "")).strip() for record in records if str(record["raw"].get(name, "")).strip()})
        if values:
            lines.append(f"{name}: {', '.join(values[:8])}")
        if len(lines) >= limit:
            break
    return lines


def issue_field_value(record, issue_field):
    if not issue_field:
        return None
    if issue_field in record.get("raw", {}):
        raw_value = record["raw"].get(issue_field)
        text = str(raw_value or "").strip()
        return text if text else None
    if issue_field == "step_length_m":
        return record.get("step_length_m")
    if issue_field == "speed_mps":
        return record.get("speed_mps")
    if issue_field == "time_delta_s":
        return record.get("time_delta_s")
    return None


def summarize_issue_field_values(values):
    present = [value for value in values if value not in (None, "")]
    if not present:
        return "n/a"
    numeric_values = []
    non_numeric_seen = False
    for value in present:
        number = try_float(value)
        if number is None:
            non_numeric_seen = True
            break
        numeric_values.append(number)
    if numeric_values and not non_numeric_seen:
        numeric_values.sort()
        median = quantile(numeric_values, 0.5)
        return f"median {format_metric(median, 3)}; range {format_metric(numeric_values[0], 3)} to {format_metric(numeric_values[-1], 3)}"
    counts = {}
    for value in present:
        text = str(value).strip()
        if not text:
            continue
        counts[text] = counts.get(text, 0) + 1
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return ", ".join(f"{value} ({count})" for value, count in ordered[:3])


def build_individual_summary_rows(records, issue_field):
    by_individual = {}
    for record in records:
        individual = record["individual"]
        row = by_individual.setdefault(
            individual,
            {
                "individual": individual,
                "fix_count": 0,
                "issue_ids": set(),
                "first_time_ms": record["time_ms"],
                "last_time_ms": record["time_ms"],
                "first_time_text": record["time_text"],
                "last_time_text": record["time_text"],
                "max_step": None,
                "max_speed": None,
                "issue_field_values": [],
            },
        )
        row["fix_count"] += 1
        issue_id = issue_id_for(record)
        if issue_id:
            row["issue_ids"].add(issue_id)
        row["issue_field_values"].append(issue_field_value(record, issue_field))
        if record["time_ms"] < row["first_time_ms"]:
            row["first_time_ms"] = record["time_ms"]
            row["first_time_text"] = record["time_text"]
        if record["time_ms"] > row["last_time_ms"]:
            row["last_time_ms"] = record["time_ms"]
            row["last_time_text"] = record["time_text"]
        if record["step_length_m"] is not None:
            row["max_step"] = record["step_length_m"] if row["max_step"] is None else max(row["max_step"], record["step_length_m"])
        if record["speed_mps"] is not None:
            row["max_speed"] = record["speed_mps"] if row["max_speed"] is None else max(row["max_speed"], record["speed_mps"])
    rows = []
    for row in by_individual.values():
        rows.append(
            {
                "individual": row["individual"],
                "fix_count": row["fix_count"],
                "issue_ids": sorted(row["issue_ids"]),
                "first_time_ms": row["first_time_ms"],
                "last_time_ms": row["last_time_ms"],
                "first_time_text": row["first_time_text"],
                "last_time_text": row["last_time_text"],
                "max_step": row["max_step"],
                "max_speed": row["max_speed"],
                "issue_field_summary": summarize_issue_field_values(row["issue_field_values"]),
            }
        )
    return sorted(rows, key=lambda item: (item["individual"], item["first_time_ms"]))


def round_robin_examples(examples, limit):
    if len(examples) <= limit:
        return examples
    buckets = {}
    for example in examples:
        buckets.setdefault(example["individual"], []).append(example)
    ordered = []
    while len(ordered) < limit:
        added = False
        for individual in sorted(buckets):
            items = buckets[individual]
            if not items:
                continue
            ordered.append(items.pop(0))
            added = True
            if len(ordered) >= limit:
                break
        if not added:
            break
    return ordered


def build_example_entry(window, records, quality_fields):
    sorted_records = sorted(records, key=lambda record: (record["time_ms"], record["fix_key"]))
    first = sorted_records[0]
    last = sorted_records[-1]
    issue_ids = sorted({issue_id_for(record) for record in sorted_records if issue_id_for(record)})
    max_step = max((record["step_length_m"] or 0.0) for record in sorted_records)
    max_speed = max((record["speed_mps"] or 0.0) for record in sorted_records)
    return {
        "snapshot_key": str(window.get("snapshot_key", "")).strip(),
        "caption": str(window.get("caption", "")).strip(),
        "individual": window.get("individual") or first["individual"],
        "set_name": window.get("set_name") or first.get("set_name", "train"),
        "start_time_ms": int(window.get("start_time_ms") or first["time_ms"]),
        "end_time_ms": int(window.get("end_time_ms") or last["time_ms"]),
        "start_time_text": str(window.get("start_time_text") or first["time_text"]).strip(),
        "end_time_text": str(window.get("end_time_text") or last["time_text"]).strip(),
        "window_fix_count": int(window.get("window_fix_count") or len(sorted_records)),
        "suspicious_fix_count": len(sorted_records),
        "issue_ids": issue_ids,
        "status_counts": summarize_status_counts(sorted_records),
        "max_step": max_step,
        "max_speed": max_speed,
        "quality_lines": build_quality_lines(sorted_records, quality_fields),
        "map_points": [
            {
                "lon": float(record["lon"]),
                "lat": float(record["lat"]),
                "fix_key": record["fix_key"],
                "time_ms": int(record["time_ms"]),
            }
            for record in sorted_records
            if record.get("lon") is not None and record.get("lat") is not None
        ],
    }


def nice_tick_step(span):
    if span <= 0:
        return 1.0
    rough = span / 4.0
    from math import floor, log10

    magnitude = 10 ** floor(log10(abs(rough)))
    for multiplier in (1, 2, 5, 10):
        step = magnitude * multiplier
        if step >= rough:
            return float(step)
    return float(magnitude * 10)


def axis_ticks(min_value, max_value, count=4):
    span = max_value - min_value
    if span <= 0:
        return [float(min_value)]
    step = nice_tick_step(span)
    start = int(min_value / step) * step
    if start > min_value:
        start -= step
    values = []
    value = start
    upper = max_value + step
    while value <= upper:
        if min_value - (step * 0.1) <= value <= max_value + (step * 0.1):
            values.append(round(float(value), 6))
        value += step
    if len(values) < 2:
        values = [round(float(min_value), 6), round(float(max_value), 6)]
    return values[: max(count + 2, len(values))]


def format_lon_label(value):
    direction = "E" if value >= 0 else "W"
    return f"{abs(value):.2f}°{direction}"


def format_lat_label(value):
    direction = "N" if value >= 0 else "S"
    return f"{abs(value):.2f}°{direction}"


def build_track_data_url(points, title, subtitle):
    clean_points = [
        point
        for point in points
        if is_valid_coordinate(point.get("lon"), point.get("lat"))
    ]
    if not clean_points:
        return ""

    width = 880
    height = 440
    padding_left = 72.0
    padding_right = 36.0
    padding_top = 42.0
    padding_bottom = 58.0
    min_lon = min(point["lon"] for point in clean_points)
    max_lon = max(point["lon"] for point in clean_points)
    min_lat = min(point["lat"] for point in clean_points)
    max_lat = max(point["lat"] for point in clean_points)
    lon_span = max(max_lon - min_lon, 0.001)
    lat_span = max(max_lat - min_lat, 0.001)
    lon_pad = lon_span * 0.08
    lat_pad = lat_span * 0.10
    min_lon -= lon_pad
    max_lon += lon_pad
    min_lat -= lat_pad
    max_lat += lat_pad
    lon_span = max(max_lon - min_lon, 0.001)
    lat_span = max(max_lat - min_lat, 0.001)

    def project(point):
        x = padding_left + ((point["lon"] - min_lon) / lon_span) * (width - padding_left - padding_right)
        y = height - padding_bottom - ((point["lat"] - min_lat) / lat_span) * (height - padding_top - padding_bottom)
        return x, y

    projected = [project(point) for point in clean_points]
    path_d = " ".join(
        ("M" if index == 0 else "L") + f" {x:.2f} {y:.2f}"
        for index, (x, y) in enumerate(projected)
    )
    circles = []
    for index, (x, y) in enumerate(projected):
        radius = 7 if index == len(projected) - 1 else 5
        fill = "#f25067" if index == len(projected) - 1 else "#4e90b8"
        stroke = "#ffffff" if index == len(projected) - 1 else "#183447"
        circles.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="2" />'
        )

    lon_ticks = axis_ticks(min_lon, max_lon)
    lat_ticks = axis_ticks(min_lat, max_lat)
    grid_lines = []
    x_labels = []
    y_labels = []
    map_left = padding_left
    map_right = width - padding_right
    map_top = padding_top
    map_bottom = height - padding_bottom

    for tick in lon_ticks:
        x = padding_left + ((tick - min_lon) / lon_span) * (width - padding_left - padding_right)
        grid_lines.append(
            f'<line x1="{x:.2f}" y1="{map_top:.2f}" x2="{x:.2f}" y2="{map_bottom:.2f}" stroke="#9eb7c4" stroke-width="1" stroke-dasharray="4 8" opacity="0.75" />'
        )
        grid_lines.append(
            f'<line x1="{x:.2f}" y1="{map_bottom:.2f}" x2="{x:.2f}" y2="{map_bottom + 8:.2f}" stroke="#6b8796" stroke-width="1.2" />'
        )
        x_labels.append(
            f'<text x="{x:.2f}" y="{height - 16:.2f}" text-anchor="middle" font-size="12" font-family="Arial, sans-serif" fill="#334e5c">{html_escape(format_lon_label(tick))}</text>'
        )

    for tick in lat_ticks:
        y = height - padding_bottom - ((tick - min_lat) / lat_span) * (height - padding_top - padding_bottom)
        grid_lines.append(
            f'<line x1="{map_left:.2f}" y1="{y:.2f}" x2="{map_right:.2f}" y2="{y:.2f}" stroke="#b5c9d4" stroke-width="1" stroke-dasharray="4 8" opacity="0.7" />'
        )
        y_labels.append(
            f'<text x="{padding_left - 10:.2f}" y="{y + 4:.2f}" text-anchor="end" font-size="12" font-family="Arial, sans-serif" fill="#486581">{html_escape(format_lat_label(tick))}</text>'
        )

    svg = "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
            "<defs>",
            '<linearGradient id="bg" x1="0" x2="1" y1="0" y2="1">',
            '<stop offset="0%" stop-color="#edf5f8" />',
            '<stop offset="100%" stop-color="#d7e7ee" />',
            "</linearGradient>",
            '<linearGradient id="mapPanel" x1="0" x2="0" y1="0" y2="1">',
            '<stop offset="0%" stop-color="#f5fbfd" />',
            '<stop offset="100%" stop-color="#e2eff4" />',
            "</linearGradient>",
            "</defs>",
            f'<rect width="{width}" height="{height}" rx="20" fill="url(#bg)" />',
            f'<rect x="{map_left - 16:.2f}" y="{map_top - 16:.2f}" width="{map_right - map_left + 32:.2f}" height="{map_bottom - map_top + 32:.2f}" rx="16" fill="#f8fcfe" stroke="#bfd5e2" />',
            f'<rect x="{map_left:.2f}" y="{map_top:.2f}" width="{map_right - map_left:.2f}" height="{map_bottom - map_top:.2f}" rx="12" fill="url(#mapPanel)" stroke="#acc4d0" />',
            f'<rect x="{map_left:.2f}" y="{map_top:.2f}" width="{map_right - map_left:.2f}" height="{map_bottom - map_top:.2f}" rx="12" fill="none" stroke="#dbe8ee" stroke-width="6" opacity="0.45" />',
            *grid_lines,
            f'<path d="{path_d}" fill="none" stroke="#183447" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" />',
            *circles,
            f'<text x="{padding_left:.2f}" y="28" font-size="18" font-family="Arial, sans-serif" fill="#102a43">{title}</text>',
            f'<text x="{padding_left:.2f}" y="{height - 34:.2f}" font-size="14" font-family="Arial, sans-serif" fill="#486581">{subtitle}</text>',
            f'<text x="{(map_left + map_right) / 2:.2f}" y="{height - 2:.2f}" text-anchor="middle" font-size="12" font-family="Arial, sans-serif" fill="#243b53">Longitude</text>',
            *x_labels,
            *y_labels,
            "</svg>",
        ]
    )
    encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


def fallback_snapshot_data_url(example):
    points = [
        point
        for point in example.get("map_points", [])
        if is_valid_coordinate(point.get("lon"), point.get("lat"))
    ]
    if not points:
        return ""

    title = html_escape(
        f"{example.get('individual', '')} | {example.get('start_time_text', '')} to {example.get('end_time_text', '')}"
    )
    subtitle = html_escape(
        f"{example.get('set_name', 'train')} track • {example.get('window_fix_count', 0)} fixes • {example.get('suspicious_fix_count', 0)} suspicious"
    )
    return build_track_data_url(points, title, subtitle)


def snapshot_href_for_example(example, snapshot, screenshot_mode):
    if screenshot_mode != "auto":
        return ""
    if snapshot:
        artifact_name = str(snapshot.get("artifact_name", "")).strip()
        if artifact_name:
            return artifact_name
        data_url = str(snapshot.get("data_url", "")).strip()
        if data_url:
            return data_url
    return fallback_snapshot_data_url(example)


def fallback_examples_from_records(records, quality_fields):
    grouped = {}
    for record in sorted(records, key=lambda item: (item["individual"], item["time_ms"], item["fix_key"])):
        grouped.setdefault(record["individual"], []).append(record)
    examples = []
    for individual in sorted(grouped):
        for record in grouped[individual][:2]:
            examples.append(
                build_example_entry(
                    {
                        "snapshot_key": "",
                        "caption": f"{issue_type_for(record)} | {record['individual']} | {record['time_text']}",
                        "individual": record["individual"],
                        "set_name": record.get("set_name", "train"),
                        "start_time_ms": record["time_ms"],
                        "end_time_ms": record["time_ms"],
                        "start_time_text": record["time_text"],
                        "end_time_text": record["time_text"],
                        "window_fix_count": 1,
                    },
                    [record],
                    quality_fields,
                )
            )
    return examples


def build_issue_sections(matched_records, snapshot_windows, fieldnames, columns):
    quality_fields = extract_quality_fields(fieldnames, columns)
    record_by_fix_key = {record["fix_key"]: record for record in matched_records}
    records_by_issue_type = {}
    for record in matched_records:
        for issue_type in issue_types_for(record):
            records_by_issue_type.setdefault(issue_type, []).append(record)

    examples_by_issue_type = {}
    for window in snapshot_windows:
        typed_records = {}
        for fix_key in window.get("report_fix_keys", []):
            record = record_by_fix_key.get(fix_key)
            if not record:
                continue
            for issue_type in issue_types_for(record):
                typed_records.setdefault(issue_type, []).append(record)
        for issue_type, records in typed_records.items():
            examples_by_issue_type.setdefault(issue_type, []).append(build_example_entry(window, records, quality_fields))

    sections = []
    for issue_type in sorted(records_by_issue_type):
        records = sorted(records_by_issue_type[issue_type], key=lambda record: (record["individual"], record["time_ms"], record["fix_key"]))
        issue_ids = sorted({issue_id for record in records for issue_id in issue_ids_for(record)})
        issue_field = most_common_non_empty(
            [issue_field_for_type(record, issue_type) for record in records],
            "",
        )
        issue_threshold = most_common_non_empty(
            [issue_threshold_for_type(record, issue_type) for record in records],
            "",
        )
        issue_note = most_common_non_empty(
            [value for record in records for value in issue_detail_values_for_type(record, issue_type, "issue_note")],
            "Potential location error requiring owner review.",
        )
        owner_question = most_common_non_empty(
            [value for record in records for value in issue_detail_values_for_type(record, issue_type, "owner_question")],
            "Could you confirm whether these locations should be treated as outliers?",
        )
        examples = examples_by_issue_type.get(issue_type) or fallback_examples_from_records(records, quality_fields)
        sections.append(
            {
                "issue_type": issue_type,
                "records": records,
                "issue_ids": issue_ids,
                "issue_field": issue_field,
                "issue_threshold": issue_threshold,
                "issue_note": issue_note,
                "owner_question": owner_question,
                "status_counts": summarize_status_counts(records),
                "individual_rows": build_individual_summary_rows(records, issue_field),
                "examples": round_robin_examples(
                    sorted(examples, key=lambda example: (example["individual"], example["start_time_ms"], example["snapshot_key"])),
                    MAX_EXAMPLES_PER_ISSUE,
                ),
                "first_time_text": records[0]["time_text"],
                "last_time_text": records[-1]["time_text"],
                "quality_fields": quality_fields,
            }
        )
    return sections


def build_markdown_report(target_artifact, user, screenshot_mode, issue_sections, snapshots_by_key, selected_count):
    lines = [
        "# Movement Outlier Review Report",
        "",
        f"- Artifact: `{target_artifact}`",
        f"- Generated by: {user}",
        f"- Screenshot mode: {screenshot_mode}",
        f"- Selected fixes: {selected_count}",
        f"- Issue types shown: {len(issue_sections)}",
        f"- Generated at: {now_iso()}",
        "",
        "## Issue Type Overview",
        "",
        "| Issue Type | Suspicious Fixes | Individuals | Issue IDs | Example Tracklets |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for section in issue_sections:
        lines.append(
            f"| {section['issue_type']} | {len(section['records'])} | {len(section['individual_rows'])} | {len(section['issue_ids'])} | {len(section['examples'])} |"
        )
    lines.append("")

    for section in issue_sections:
        lines.extend(
            [
                f"## Issue Type: {section['issue_type']}",
                "",
                f"- Suspicious fixes: {len(section['records'])}",
                f"- Individuals affected: {len(section['individual_rows'])}",
                f"- Issue ids: {format_issue_ids(section['issue_ids'])}",
                f"- Flagged variable: {section['issue_field'] or 'not recorded'}",
                f"- Flag threshold: {section['issue_threshold'] or 'not recorded'}",
                f"- Status counts: {format_status_counts(section['status_counts'])}",
                f"- Time span: {section['first_time_text']} to {section['last_time_text']}",
                "",
                "### Description",
                section["issue_note"],
                "",
                "### Owner Question",
                section["owner_question"],
                "",
                "### Individual Summary",
                "",
                f"| Individual | Suspicious Fixes | Issue IDs | First Timestamp | Last Timestamp | {section['issue_field'] or 'Flagged variable'} summary | Max Step (m) | Max Speed (m/s) |",
                "| --- | ---: | ---: | --- | --- | --- | ---: | ---: |",
            ]
        )
        for row in section["individual_rows"]:
            lines.append(
                f"| {row['individual']} | {row['fix_count']} | {len(row['issue_ids'])} | {row['first_time_text']} | {row['last_time_text']} | {row['issue_field_summary']} | "
                f"{format_metric(row['max_step'], 2)} | {format_metric(row['max_speed'], 3)} |"
            )
        lines.extend(["", "### Example Tracklets", ""])
        for index, example in enumerate(section["examples"], start=1):
            lines.extend(
                [
                    f"#### Example {index}: {example['individual']} | {example['start_time_text']} to {example['end_time_text']}",
                    "",
                    f"- Track: {example['set_name']}",
                    f"- Window fixes on map: {example['window_fix_count']}",
                    f"- Suspicious fixes in this example: {example['suspicious_fix_count']}",
                    f"- Issue ids: {format_issue_ids(example['issue_ids'])}",
                    f"- Status counts: {format_status_counts(example['status_counts'])}",
                    f"- Max step length: {format_metric(example['max_step'], 2)} m",
                    f"- Max speed: {format_metric(example['max_speed'], 3)} m/s",
                    "",
                    "##### Snapshot",
                    "",
                ]
            )
            snapshot = snapshots_by_key.get(example["snapshot_key"])
            snapshot_href = snapshot_href_for_example(example, snapshot, screenshot_mode)
            if snapshot_href:
                caption = ""
                if snapshot:
                    caption = snapshot.get("caption", "").strip() or snapshot.get("artifact_name", "").strip()
                if not caption:
                    caption = example.get("caption", "").strip() or f"{example['individual']} track snapshot"
                lines.append(f"![{caption}]({snapshot_href})")
            else:
                lines.append("[No auto-rendered map snapshot included for this example.]")
            lines.extend(["", "##### Evidence", ""])
            if example["quality_lines"]:
                for line in example["quality_lines"]:
                    lines.append(f"- {line}")
            else:
                lines.append("- No additional quality fields were populated for this example.")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_html_report(target_artifact, user, screenshot_mode, issue_sections, snapshots_by_key, selected_count):
    parts = [
        "<!DOCTYPE html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        "<title>Movement Outlier Review Report</title>",
        "<style>",
        "body { font-family: Arial, sans-serif; line-height: 1.5; color: #1f2933; margin: 0; background: #f6f8fb; }",
        "main { max-width: 1120px; margin: 0 auto; padding: 32px 24px 48px; }",
        "header { margin-bottom: 28px; }",
        "h1, h2, h3, h4, h5 { color: #102a43; margin-bottom: 0.5rem; }",
        "h2 { margin-top: 2rem; padding-bottom: 0.35rem; border-bottom: 2px solid #d9e2ec; }",
        "section.issue { margin-bottom: 2rem; }",
        "section.example { background: #ffffff; border: 1px solid #d9e2ec; border-radius: 12px; padding: 20px; margin: 20px 0; box-shadow: 0 4px 16px rgba(15, 23, 42, 0.06); }",
        "ul.meta, ul.evidence { margin: 0.5rem 0 1rem 1.25rem; padding: 0; }",
        "figure { margin: 1rem 0; }",
        "figure img { max-width: 100%; height: auto; border: 1px solid #bcccdc; border-radius: 8px; background: #ffffff; }",
        "figcaption { color: #52606d; font-size: 0.95rem; margin-top: 0.4rem; }",
        "table { width: 100%; border-collapse: collapse; margin-top: 0.75rem; font-size: 0.95rem; background: #ffffff; }",
        "th, td { border: 1px solid #d9e2ec; padding: 0.55rem 0.65rem; text-align: left; vertical-align: top; }",
        "th { background: #eef2f7; color: #243b53; }",
        "td.numeric { text-align: right; font-variant-numeric: tabular-nums; }",
        "p.placeholder { font-style: italic; color: #52606d; }",
        "code { font-family: SFMono-Regular, Consolas, 'Liberation Mono', Menlo, monospace; font-size: 0.95em; }",
        "</style>",
        "</head>",
        "<body>",
        "<main>",
        "<header>",
        "<h1>Movement Outlier Review Report</h1>",
        '<ul class="meta">',
        f"<li><strong>Artifact:</strong> <code>{html_escape(target_artifact)}</code></li>",
        f"<li><strong>Generated by:</strong> {html_escape(user)}</li>",
        f"<li><strong>Screenshot mode:</strong> {html_escape(screenshot_mode)}</li>",
        f"<li><strong>Selected fixes:</strong> {selected_count}</li>",
        f"<li><strong>Issue types shown:</strong> {len(issue_sections)}</li>",
        f"<li><strong>Generated at:</strong> {html_escape(now_iso())}</li>",
        "</ul>",
        "<h2>Issue Type Overview</h2>",
        "<table>",
        "<thead><tr><th>Issue Type</th><th>Suspicious Fixes</th><th>Individuals</th><th>Issue IDs</th><th>Example Tracklets</th></tr></thead>",
        "<tbody>",
    ]
    for section in issue_sections:
        parts.append(
            "<tr>"
            f"<td>{html_escape(section['issue_type'])}</td>"
            f'<td class="numeric">{len(section["records"])}</td>'
            f'<td class="numeric">{len(section["individual_rows"])}</td>'
            f'<td class="numeric">{len(section["issue_ids"])}</td>'
            f'<td class="numeric">{len(section["examples"])}</td>'
            "</tr>"
        )
    parts.extend(["</tbody>", "</table>", "</header>"])

    for section in issue_sections:
        parts.extend(
            [
                '<section class="issue">',
                f"<h2>Issue Type: {html_escape(section['issue_type'])}</h2>",
                '<ul class="meta">',
                f'<li><strong>Suspicious fixes:</strong> {len(section["records"])}</li>',
                f'<li><strong>Individuals affected:</strong> {len(section["individual_rows"])}</li>',
                f'<li><strong>Issue ids:</strong> {html_escape(format_issue_ids(section["issue_ids"]))}</li>',
                f'<li><strong>Flagged variable:</strong> {html_escape(section["issue_field"] or "not recorded")}</li>',
                f'<li><strong>Flag threshold:</strong> {html_escape(section["issue_threshold"] or "not recorded")}</li>',
                f'<li><strong>Status counts:</strong> {html_escape(format_status_counts(section["status_counts"]))}</li>',
                f'<li><strong>Time span:</strong> {html_escape(section["first_time_text"])} to {html_escape(section["last_time_text"])}</li>',
                "</ul>",
                "<h3>Description</h3>",
                f"<p>{html_escape(section['issue_note'])}</p>",
                "<h3>Owner Question</h3>",
                f"<p>{html_escape(section['owner_question'])}</p>",
                "<h3>Individual Summary</h3>",
                "<table>",
                f"<thead><tr><th>Individual</th><th>Suspicious Fixes</th><th>Issue IDs</th><th>First Timestamp</th><th>Last Timestamp</th><th>{html_escape(section['issue_field'] or 'Flagged variable')} summary</th><th>Max Step (m)</th><th>Max Speed (m/s)</th></tr></thead>",
                "<tbody>",
            ]
        )
        for row in section["individual_rows"]:
            parts.append(
                "<tr>"
                f"<td>{html_escape(row['individual'])}</td>"
                f'<td class="numeric">{row["fix_count"]}</td>'
                f'<td class="numeric">{len(row["issue_ids"])}</td>'
                f"<td>{html_escape(row['first_time_text'])}</td>"
                f"<td>{html_escape(row['last_time_text'])}</td>"
                f"<td>{html_escape(row['issue_field_summary'])}</td>"
                f'<td class="numeric">{html_escape(format_metric(row["max_step"], 2))}</td>'
                f'<td class="numeric">{html_escape(format_metric(row["max_speed"], 3))}</td>'
                "</tr>"
            )
        parts.extend(["</tbody>", "</table>", "<h3>Example Tracklets</h3>"])
        for index, example in enumerate(section["examples"], start=1):
            snapshot = snapshots_by_key.get(example["snapshot_key"])
            snapshot_href = snapshot_href_for_example(example, snapshot, screenshot_mode)
            if snapshot_href:
                caption = ""
                if snapshot:
                    caption = snapshot.get("caption", "").strip() or snapshot.get("artifact_name", "").strip()
                if not caption:
                    caption = example.get("caption", "").strip() or f"{example['individual']} track snapshot"
                snapshot_markup = (
                    "<figure>"
                    f'<img src="{html_escape(snapshot_href)}" alt="{html_escape(caption)}">'
                    f"<figcaption>{html_escape(caption)}</figcaption>"
                    "</figure>"
                )
            else:
                snapshot_markup = '<p class="placeholder">No auto-rendered map snapshot included for this example.</p>'
            evidence_markup = (
                '<ul class="evidence">' + "".join(f"<li>{html_escape(line)}</li>" for line in example["quality_lines"]) + "</ul>"
                if example["quality_lines"]
                else "<p>No additional quality fields were populated for this example.</p>"
            )
            parts.extend(
                [
                    '<section class="example">',
                    f"<h4>Example {index}: {html_escape(example['individual'])} | {html_escape(example['start_time_text'])} to {html_escape(example['end_time_text'])}</h4>",
                    '<ul class="meta">',
                    f"<li><strong>Track:</strong> {html_escape(example['set_name'])}</li>",
                    f"<li><strong>Window fixes on map:</strong> {example['window_fix_count']}</li>",
                    f"<li><strong>Suspicious fixes in this example:</strong> {example['suspicious_fix_count']}</li>",
                    f"<li><strong>Issue ids:</strong> {html_escape(format_issue_ids(example['issue_ids']))}</li>",
                    f"<li><strong>Status counts:</strong> {html_escape(format_status_counts(example['status_counts']))}</li>",
                    f"<li><strong>Max step length:</strong> {html_escape(format_metric(example['max_step'], 2))} m</li>",
                    f"<li><strong>Max speed:</strong> {html_escape(format_metric(example['max_speed'], 3))} m/s</li>",
                    "</ul>",
                    "<h5>Snapshot</h5>",
                    snapshot_markup,
                    "<h5>Evidence</h5>",
                    evidence_markup,
                    "</section>",
                ]
            )
        parts.append("</section>")
    parts.extend(["</main>", "</body>", "</html>"])
    return "\n".join(parts).rstrip() + "\n"


def median_value(values):
    clean = sorted(float(value) for value in values if value is not None)
    if not clean:
        return None
    return quantile(clean, 0.5)


def format_count_label(value, singular, plural=None):
    count = int(value or 0)
    label = singular if count == 1 else (plural or singular + "s")
    return f"{count} {label}"


def format_temporal_resolution(seconds):
    if seconds is None:
        return "n/a"
    if seconds < 60:
        if abs(seconds - round(seconds)) < 1e-6:
            return format_count_label(int(round(seconds)), "second")
        return f"{seconds:.1f} seconds"
    minutes = seconds / 60.0
    if abs(minutes - round(minutes)) < 1e-6:
        return format_count_label(int(round(minutes)), "minute")
    return f"{minutes:.1f} minutes"


def format_monitoring_span(start_ms, end_ms):
    if start_ms is None or end_ms is None:
        return "n/a"
    start_text = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
    end_text = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
    return f"{start_text} to {end_text}"


def issue_breakdown_for_records(records):
    grouped = {}
    for record in records:
        for issue_type in issue_types_for(record):
            item = grouped.setdefault(
                issue_type,
                {
                    "issue_type": issue_type,
                    "fix_count": 0,
                    "issue_ids": set(),
                    "status_counts": {},
                },
            )
            item["fix_count"] += 1
            for issue_id in issue_ids_for(record):
                if issue_id:
                    item["issue_ids"].add(issue_id)
            status = status_for(record)
            item["status_counts"][status] = item["status_counts"].get(status, 0) + 1
    rows = []
    for item in grouped.values():
        rows.append(
            {
                "issue_type": item["issue_type"],
                "fix_count": item["fix_count"],
                "issue_ids": sorted(item["issue_ids"]),
                "status_counts": item["status_counts"],
            }
        )
    return sorted(rows, key=lambda item: (item["issue_type"], item["fix_count"]))


def build_issue_summary_lines(issue_breakdown):
    return [
        f"{item['issue_type']}: {format_count_label(item['fix_count'], 'reviewed fix')} ({format_status_counts(item['status_counts'])})"
        for item in issue_breakdown[:6]
    ]


def best_effort_value(record, column_name, fallback=""):
    if not column_name:
        return fallback
    value = str(record["raw"].get(column_name, "")).strip()
    return value or fallback


def build_individual_profile_sections(valid_records, fieldnames, columns, selected_individuals, target_artifact):
    selected_set = set(selected_individuals or [])
    if not selected_set:
        return []

    grouped = {}
    for record in sorted(valid_records, key=lambda item: (item["individual"], item["time_ms"], item["fix_key"])):
        individual = record["individual"]
        if individual not in selected_set:
            continue
        raw = record["raw"]
        item = grouped.setdefault(
            individual,
            {
                "individual": individual,
                "records": [],
                "study_names": [],
                "study_ids": [],
                "animal_ids": [],
                "species": [],
                "sources": [],
                "bursts": set(),
                "reviewed_records": [],
                "intervals_s": [],
                "start_ms": record["time_ms"],
                "end_ms": record["time_ms"],
                "start_text": record["time_text"],
                "end_text": record["time_text"],
            },
        )
        item["records"].append(record)
        item["study_names"].append(best_effort_value(record, columns.get("study_name")))
        item["study_ids"].append(best_effort_value(record, columns.get("study_id")))
        animal_id = best_effort_value(record, columns.get("animal_id"), individual)
        item["animal_ids"].append(animal_id)
        species = best_effort_value(record, columns.get("common_name")) or best_effort_value(record, columns.get("scientific_name"))
        item["species"].append(species or "Unknown species")
        item["sources"].append(best_effort_value(record, columns.get("source"), target_artifact))
        burst_value = best_effort_value(record, columns.get("burst"))
        if burst_value:
            item["bursts"].add(burst_value)
        if record["time_delta_s"] is not None:
            item["intervals_s"].append(float(record["time_delta_s"]))
        if normalize_review_status(record["review"].get("vc_outlier_status")):
            item["reviewed_records"].append(record)
        if record["time_ms"] < item["start_ms"]:
            item["start_ms"] = record["time_ms"]
            item["start_text"] = record["time_text"]
        if record["time_ms"] > item["end_ms"]:
            item["end_ms"] = record["time_ms"]
            item["end_text"] = record["time_text"]

    sections = []
    for individual in selected_individuals:
        item = grouped.get(individual)
        if not item:
            continue
        review_counts = summarize_status_counts(item["reviewed_records"])
        issue_breakdown = issue_breakdown_for_records(item["reviewed_records"])
        median_resolution_s = median_value(item["intervals_s"])
        study_name = most_common_non_empty(item["study_names"], "")
        study_id = most_common_non_empty(item["study_ids"], "")
        animal_id = most_common_non_empty(item["animal_ids"], individual)
        species = most_common_non_empty(item["species"], "Unknown species")
        source = most_common_non_empty(item["sources"], target_artifact)
        monitoring_text = format_monitoring_span(item["start_ms"], item["end_ms"])
        points = [
            {"lon": record["lon"], "lat": record["lat"]}
            for record in item["records"]
            if record.get("lon") is not None and record.get("lat") is not None
        ]
        map_title = html_escape(f"{individual} whole track")
        map_subtitle = html_escape(f"{species} • {monitoring_text} • {format_count_label(len(points), 'fix')}")
        sections.append(
            {
                "individual": individual,
                "snapshot_key": f"individual_profile::{individual}",
                "study_name": study_name,
                "study_id": study_id,
                "animal_id": animal_id,
                "species": species,
                "median_temporal_resolution_s": median_resolution_s,
                "median_temporal_resolution_text": format_temporal_resolution(median_resolution_s),
                "monitoring_start_ms": item["start_ms"],
                "monitoring_end_ms": item["end_ms"],
                "monitoring_text": monitoring_text,
                "source": source,
                "burst_count": len(item["bursts"]) if item["bursts"] else None,
                "row_count": len(item["records"]),
                "reviewed_fix_count": len(item["reviewed_records"]),
                "review_status_counts": review_counts,
                "issue_breakdown": issue_breakdown,
                "issue_summary_lines": build_issue_summary_lines(issue_breakdown),
                "map_data_url": build_track_data_url(points, map_title, map_subtitle),
            }
        )
    return sections


def profile_snapshot_href(section, snapshot):
    if snapshot:
        artifact_name = str(snapshot.get("artifact_name", "")).strip()
        if artifact_name:
            return artifact_name
        data_url = str(snapshot.get("data_url", "")).strip()
        if data_url:
            return data_url
    return section.get("map_data_url", "")


def build_individual_profile_markdown_section(section, snapshots_by_key):
    lines = [
        f"## Individual: {section['individual']}",
        "",
        f"- Study name: {section['study_name'] or 'n/a'}",
    ]
    if section["study_id"]:
        lines.append(f"- Study ID: {section['study_id']}")
    lines.extend(
        [
            f"- Animal ID: {section['animal_id']}",
            f"- Species: {section['species']}",
            f"- Median temporal resolution: {section['median_temporal_resolution_text']}",
            f"- Monitoring: {section['monitoring_text']}",
            f"- Source: {section['source']}",
        ]
    )
    if section["burst_count"] is not None:
        lines.append(f"- No. of bursts: {section['burst_count']}")
    lines.extend(
        [
            f"- Valid fixes: {section['row_count']}",
            "",
            "### Whole Track",
            "",
        ]
    )
    snapshot_href = profile_snapshot_href(section, snapshots_by_key.get(section.get("snapshot_key", "")))
    if snapshot_href:
        lines.append(f"![{section['individual']} whole track]({snapshot_href})")
    else:
        lines.append("[No valid coordinates were available for the whole-track map.]")
    if section["reviewed_fix_count"]:
        lines.extend(
            [
                "",
                "### Issue Summary",
                "",
                f"- Reviewed fixes: {section['reviewed_fix_count']}",
                f"- Status counts: {format_status_counts(section['review_status_counts'])}",
            ]
        )
        for line in section["issue_summary_lines"]:
            lines.append(f"- {line}")
    return "\n".join(lines).rstrip() + "\n"


def build_individual_profile_markdown_report(target_artifact, user, sections, snapshots_by_key=None):
    snapshots_by_key = snapshots_by_key or {}
    lines = [
        "# Movement Individual Profile Report",
        "",
        f"- Artifact: `{target_artifact}`",
        f"- Generated by: {user}",
        f"- Individuals included: {len(sections)}",
        f"- Generated at: {now_iso()}",
        "",
    ]
    for section in sections:
        lines.append(build_individual_profile_markdown_section(section, snapshots_by_key).rstrip())
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_individual_profile_html_section(section, snapshots_by_key):
    meta = [
        f"<li><strong>Study name:</strong> {html_escape(section['study_name'] or 'n/a')}</li>",
    ]
    if section["study_id"]:
        meta.append(f"<li><strong>Study ID:</strong> {html_escape(section['study_id'])}</li>")
    meta.extend(
        [
            f"<li><strong>Animal ID:</strong> {html_escape(section['animal_id'])}</li>",
            f"<li><strong>Species:</strong> {html_escape(section['species'])}</li>",
            f"<li><strong>Median temporal resolution:</strong> {html_escape(section['median_temporal_resolution_text'])}</li>",
            f"<li><strong>Monitoring:</strong> {html_escape(section['monitoring_text'])}</li>",
            f"<li><strong>Source:</strong> {html_escape(section['source'])}</li>",
        ]
    )
    if section["burst_count"] is not None:
        meta.append(f"<li><strong>No. of bursts:</strong> {section['burst_count']}</li>")
    meta.append(f"<li><strong>Valid fixes:</strong> {section['row_count']}</li>")

    issue_markup = ""
    if section["reviewed_fix_count"]:
        items = "".join(f"<li>{html_escape(line)}</li>" for line in section["issue_summary_lines"])
        issue_markup = (
            "<h3>Issue Summary</h3>"
            '<ul class="meta">'
            f"<li><strong>Reviewed fixes:</strong> {section['reviewed_fix_count']}</li>"
            f"<li><strong>Status counts:</strong> {html_escape(format_status_counts(section['review_status_counts']))}</li>"
            "</ul>"
            f'<ul class="evidence">{items}</ul>'
        )
    snapshot_href = profile_snapshot_href(section, snapshots_by_key.get(section.get("snapshot_key", "")))
    map_markup = (
        "<figure>"
        f'<img src="{html_escape(snapshot_href)}" alt="{html_escape(section["individual"])} whole track">'
        f"<figcaption>{html_escape(section['individual'])} whole track</figcaption>"
        "</figure>"
        if snapshot_href
        else '<p class="placeholder">No valid coordinates were available for the whole-track map.</p>'
    )
    return "\n".join(
        [
            '<section class="issue">',
            f"<h2>Individual: {html_escape(section['individual'])}</h2>",
            '<ul class="meta">',
            *meta,
            "</ul>",
            "<h3>Whole Track</h3>",
            map_markup,
            issue_markup,
            "</section>",
        ]
    )


def build_individual_profile_html_report(target_artifact, user, sections, snapshots_by_key=None):
    snapshots_by_key = snapshots_by_key or {}
    parts = [
        "<!DOCTYPE html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        "<title>Movement Individual Profile Report</title>",
        "<style>",
        "body { font-family: Arial, sans-serif; line-height: 1.5; color: #1f2933; margin: 0; background: #f6f8fb; }",
        "main { max-width: 1120px; margin: 0 auto; padding: 32px 24px 48px; }",
        "header { margin-bottom: 28px; }",
        "h1, h2, h3 { color: #102a43; margin-bottom: 0.5rem; }",
        "h2 { margin-top: 2rem; padding-bottom: 0.35rem; border-bottom: 2px solid #d9e2ec; }",
        "ul.meta, ul.evidence { margin: 0.5rem 0 1rem 1.25rem; padding: 0; }",
        "figure { margin: 1rem 0; }",
        "figure img { max-width: 100%; height: auto; border: 1px solid #bcccdc; border-radius: 8px; background: #ffffff; }",
        "figcaption { color: #52606d; font-size: 0.95rem; margin-top: 0.4rem; }",
        "p.placeholder { font-style: italic; color: #52606d; }",
        "section.issue { background: #ffffff; border: 1px solid #d9e2ec; border-radius: 12px; padding: 20px; margin: 20px 0; box-shadow: 0 4px 16px rgba(15, 23, 42, 0.06); }",
        "</style>",
        "</head>",
        "<body>",
        "<main>",
        "<header>",
        "<h1>Movement Individual Profile Report</h1>",
        '<ul class="meta">',
        f"<li><strong>Artifact:</strong> <code>{html_escape(target_artifact)}</code></li>",
        f"<li><strong>Generated by:</strong> {html_escape(user)}</li>",
        f"<li><strong>Individuals included:</strong> {len(sections)}</li>",
        f"<li><strong>Generated at:</strong> {html_escape(now_iso())}</li>",
        "</ul>",
        "</header>",
    ]
    for section in sections:
        parts.append(build_individual_profile_html_section(section, snapshots_by_key))
    parts.extend(["</main>", "</body>", "</html>"])
    return "\n".join(parts).rstrip() + "\n"


def build_individual_profile_index_markdown(sections, artifact_plan):
    lines = [
        "# Movement Individual Profile Report Index",
        "",
        f"- Individuals included: {len(sections)}",
        "",
        "| Individual | Markdown | HTML |",
        "| --- | --- | --- |",
    ]
    plan_by_individual = {item["individual"]: item for item in artifact_plan}
    for section in sections:
        item = plan_by_individual[section["individual"]]
        lines.append(
            f"| {section['individual']} | [{item['markdown_name']}]({item['markdown_name']}) | [{item['html_name']}]({item['html_name']}) |"
        )
    return "\n".join(lines).rstrip() + "\n"


def build_individual_profile_index_html(sections, artifact_plan):
    rows = []
    plan_by_individual = {item["individual"]: item for item in artifact_plan}
    for section in sections:
        item = plan_by_individual[section["individual"]]
        rows.append(
            "<tr>"
            f"<td>{html_escape(section['individual'])}</td>"
            f'<td><a href="{html_escape(item["markdown_name"])}">{html_escape(item["markdown_name"])}</a></td>'
            f'<td><a href="{html_escape(item["html_name"])}">{html_escape(item["html_name"])}</a></td>'
            "</tr>"
        )
    return "\n".join(
        [
            "<!DOCTYPE html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            "<title>Movement Individual Profile Report Index</title>",
            "<style>",
            "body { font-family: Arial, sans-serif; line-height: 1.5; color: #1f2933; margin: 0; background: #f6f8fb; }",
            "main { max-width: 920px; margin: 0 auto; padding: 32px 24px 48px; }",
            "table { width: 100%; border-collapse: collapse; background: #ffffff; }",
            "th, td { border: 1px solid #d9e2ec; padding: 0.55rem 0.65rem; text-align: left; vertical-align: top; }",
            "th { background: #eef2f7; color: #243b53; }",
            "</style>",
            "</head>",
            "<body>",
            "<main>",
            "<h1>Movement Individual Profile Report Index</h1>",
            "<table>",
            "<thead><tr><th>Individual</th><th>Markdown</th><th>HTML</th></tr></thead>",
            "<tbody>",
            *rows,
            "</tbody>",
            "</table>",
            "</main>",
            "</body>",
            "</html>",
        ]
    ).rstrip() + "\n"


def normalize_report_records(items):
    normalized = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        review_in = dict(item.get("review") or {})
        attributes_in = dict(item.get("attributes") or {})
        time_ms = try_float(item.get("time_ms"))
        lon = try_float(item.get("lon"))
        lat = try_float(item.get("lat"))
        if time_ms is None or lon is None or lat is None:
            continue
        issue_items = [
            clean_issue_payload(item, review_in.get("status"))
            for item in (review_in.get("issues") or [])
            if isinstance(item, dict)
        ]
        issue_items = [item for item in issue_items if item]
        review_status = normalize_review_status(review_in.get("status"))
        if not review_status and issue_items:
            issue_statuses = {item.get("status", "") for item in issue_items}
            if "suspected" in issue_statuses:
                review_status = "suspected"
            elif "confirmed" in issue_statuses:
                review_status = "confirmed"
        has_active_review = bool(review_status or issue_items)
        review = {
            "vc_outlier_status": review_status,
            "vc_issue_id": str(review_in.get("issue_id", "")).strip() if has_active_review else "",
            "vc_issue_type": str(review_in.get("issue_type", "")).strip() if has_active_review else "",
            "vc_issue_field": str(review_in.get("issue_field", "")).strip() if has_active_review else "",
            "issues": issue_items,
            "vc_issue_note": str(review_in.get("issue_note", "")).strip() if has_active_review else "",
            "vc_owner_question": str(review_in.get("owner_question", "")).strip() if has_active_review else "",
            "vc_review_user": str(review_in.get("review_user", "")).strip() if has_active_review else "",
            "vc_reviewed_at": str(review_in.get("reviewed_at", "")).strip() if has_active_review else "",
        }
        review["issues"] = [
            item
            for item in review["issues"]
            if item
        ] or parse_issue_refs(review)
        raw = {}
        for key, value in attributes_in.items():
            name = str(key).strip()
            if not name:
                continue
            raw[name] = "" if value is None else str(value)
        raw.update(review)
        normalized.append(
            {
                "fix_key": str(item.get("fix_key", "")).strip(),
                "individual": str(item.get("individual", "")).strip(),
                "set_name": str(item.get("set_name", "")).strip() or "train",
                "time_ms": int(time_ms),
                "time_text": str(item.get("time_text") or format_timestamp(time_ms)).strip(),
                "lon": float(lon),
                "lat": float(lat),
                "step_length_m": try_float(item.get("step_length_m")),
                "speed_mps": try_float(item.get("speed_mps")),
                "time_delta_s": try_float(item.get("time_delta_s")),
                "review": review,
                "raw": raw,
            }
        )
    return normalized


def normalize_snapshot_windows(items):
    normalized = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        snapshot_key = str(item.get("snapshot_key", "")).strip()
        if not snapshot_key:
            continue
        normalized.append(
            {
                "snapshot_key": snapshot_key,
                "caption": str(item.get("caption", "")).strip(),
                "individual": str(item.get("individual", "")).strip(),
                "set_name": str(item.get("set_name", "")).strip() or "train",
                "issue_type": str(item.get("issue_type", "")).strip() or "Unspecified issue",
                "issue_types": sorted({str(value).strip() for value in item.get("issue_types", []) if str(value).strip()}),
                "anchor_fix_keys": sorted({str(value).strip() for value in item.get("anchor_fix_keys", []) if str(value).strip()}),
                "report_fix_keys": sorted({str(value).strip() for value in item.get("report_fix_keys", []) if str(value).strip()}),
                "start_fix_key": str(item.get("start_fix_key", "")).strip(),
                "end_fix_key": str(item.get("end_fix_key", "")).strip(),
                "start_time_ms": int(try_float(item.get("start_time_ms")) or 0),
                "end_time_ms": int(try_float(item.get("end_time_ms")) or 0),
                "start_time_text": str(item.get("start_time_text", "")).strip(),
                "end_time_text": str(item.get("end_time_text", "")).strip(),
                "window_fix_count": int(try_float(item.get("window_fix_count")) or 0),
            }
        )
    return normalized


def main():
        spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
        summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
        spec = json.loads(spec_path.read_text())
        params = dict(spec["analysis"].get("parameters") or {})
        target_artifact = str(params.get("target_artifact") or "").strip()
        report_type = str(params.get("report_type") or "issue_first").strip().lower()
        output_mode = str(params.get("output_mode") or "combined").strip().lower()
        selected_fix_keys = sorted({str(item).strip() for item in params.get("fix_keys", []) if str(item).strip()})
        selected_issue_ids = sorted({str(item).strip() for item in params.get("issue_ids", []) if str(item).strip()})
        selected_individuals = sorted({str(item).strip() for item in params.get("individuals", []) if str(item).strip()})
        report_fixes = normalize_report_records(params.get("report_fixes") or [])
        snapshot_windows = normalize_snapshot_windows(params.get("snapshot_windows") or [])
        screenshot_mode = str(params.get("screenshot_mode") or "manual").strip().lower()
        snapshots = list(params.get("snapshots") or [])
        individual_report_artifacts = [
            {
                "individual": str(item.get("individual", "")).strip(),
                "markdown_name": str(item.get("markdown_name", "")).strip(),
                "html_name": str(item.get("html_name", "")).strip(),
            }
            for item in params.get("individual_report_artifacts", [])
            if isinstance(item, dict)
        ]
        user = str(params.get("user") or "").strip()
        if screenshot_mode not in {"manual", "auto"}:
            raise SystemExit("Invalid screenshot mode")
        if report_type not in {"issue_first", "individual_profile"}:
            raise SystemExit("Invalid report type")
        if output_mode not in {"combined", "separate"}:
            raise SystemExit("Invalid output mode")
        if not target_artifact:
            raise SystemExit("Missing target artifact")
        if report_type == "issue_first" and not selected_fix_keys and not selected_issue_ids and not report_fixes:
            raise SystemExit("Select at least one issue or fix before generating a report")
        if report_type == "individual_profile" and not selected_individuals:
            raise SystemExit("Select at least one individual before generating a report")

        output_by_name = {artifact["logical_name"]: artifact for artifact in spec.get("output_artifacts", [])}
        source = None
        for artifact in spec.get("input_artifacts", []):
            if artifact.get("logical_name") == target_artifact:
                source = artifact
                break
        if source is None:
            raise SystemExit("Target artifact was not provided as an input")

        snapshots_by_key = {}
        realized_snapshots = []
        for snapshot in snapshots:
            artifact_name = str(snapshot.get("artifact_name") or "").strip()
            if not artifact_name or artifact_name not in output_by_name:
                continue
            raw_bytes = decode_data_url(snapshot.get("data_url"))
            output_path = Path(output_by_name[artifact_name]["path"])
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(raw_bytes)
            snapshot_key = str(snapshot.get("snapshot_key") or "").strip()
            snapshots_by_key[snapshot_key] = snapshot
            realized_snapshots.append(artifact_name)

        if report_type == "issue_first":
            required_outputs = {
                "movement_outlier_report.md",
                "movement_outlier_report.html",
                "movement_outlier_fixes.csv",
            }
            if any(name not in output_by_name for name in required_outputs):
                raise SystemExit("Report outputs were not declared")

            if report_fixes:
                matched_records = report_fixes
                fieldnames = sorted({key for record in matched_records for key in record["raw"].keys()})
                columns = detect_columns(fieldnames) if fieldnames else {}
            else:
                fieldnames, columns, _, valid_records = load_rows_with_context(source["path"])
                matched_records = selected_contexts(
                    valid_records,
                    selected_fix_keys=selected_fix_keys,
                    selected_issue_ids=selected_issue_ids,
                )
            if not matched_records:
                raise SystemExit("None of the selected fixes or issues were found")
            matched_records.sort(key=lambda record: (record["individual"], record["time_ms"], record["fix_key"]))

            if not snapshot_windows:
                snapshot_windows = [
                    {
                        "snapshot_key": f"snapshot_{index + 1:02d}",
                        "caption": f"{issue_types_for(record)[0]} | {record['individual']} | {record['time_text']}",
                        "individual": record["individual"],
                        "set_name": record.get("set_name", "train"),
                        "issue_type": issue_types_for(record)[0],
                        "issue_types": issue_types_for(record),
                        "anchor_fix_keys": [record["fix_key"]],
                        "report_fix_keys": [record["fix_key"]],
                        "start_fix_key": record["fix_key"],
                        "end_fix_key": record["fix_key"],
                        "start_time_ms": record["time_ms"],
                        "end_time_ms": record["time_ms"],
                        "start_time_text": record["time_text"],
                        "end_time_text": record["time_text"],
                        "window_fix_count": 1,
                    }
                    for index, record in enumerate(matched_records)
                ]

            ordered_issue_sections = build_issue_sections(matched_records, snapshot_windows, fieldnames, columns)
            report_text = build_markdown_report(
                target_artifact,
                user,
                screenshot_mode,
                ordered_issue_sections,
                snapshots_by_key,
                len(matched_records),
            )
            report_path = Path(output_by_name["movement_outlier_report.md"]["path"])
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(report_text, encoding="utf-8")
            html_report_text = build_html_report(
                target_artifact,
                user,
                screenshot_mode,
                ordered_issue_sections,
                snapshots_by_key,
                len(matched_records),
            )
            html_report_path = Path(output_by_name["movement_outlier_report.html"]["path"])
            html_report_path.parent.mkdir(parents=True, exist_ok=True)
            html_report_path.write_text(html_report_text, encoding="utf-8")

            appendix_fields = [
                "issue_id",
                "issue_type",
                "fix_key",
                "individual",
                "timestamp",
                "longitude",
                "latitude",
                "step_length_m",
                "speed_mps",
                "time_delta_s",
                "status",
                "owner_question",
                "issue_note",
            ]
            quality_fields = extract_quality_fields(fieldnames, columns)
            appendix_fields.extend(quality_fields)
            appendix_path = Path(output_by_name["movement_outlier_fixes.csv"]["path"])
            appendix_path.parent.mkdir(parents=True, exist_ok=True)
            with appendix_path.open("w", newline="", encoding="utf-8") as output_handle:
                writer = csv.DictWriter(output_handle, fieldnames=appendix_fields)
                writer.writeheader()
                for record in matched_records:
                    row = {
                        "issue_id": record["review"].get("vc_issue_id", "").strip(),
                        "issue_type": record["review"].get("vc_issue_type", "").strip(),
                        "fix_key": record["fix_key"],
                        "individual": record["individual"],
                        "timestamp": record["time_text"],
                        "longitude": f"{record['lon']:.6f}",
                        "latitude": f"{record['lat']:.6f}",
                        "step_length_m": "" if record["step_length_m"] is None else f"{record['step_length_m']:.6f}",
                        "speed_mps": "" if record["speed_mps"] is None else f"{record['speed_mps']:.6f}",
                        "time_delta_s": "" if record["time_delta_s"] is None else f"{record['time_delta_s']:.6f}",
                        "status": record["review"].get("vc_outlier_status", "").strip(),
                        "owner_question": record["review"].get("vc_owner_question", "").strip(),
                        "issue_note": record["review"].get("vc_issue_note", "").strip(),
                    }
                    for name in quality_fields:
                        row[name] = str(record["raw"].get(name, "")).strip()
                    writer.writerow(row)

            write_json(summary_path, {
                "app": "movement",
                "action": "generate_report",
                "report_type": report_type,
                "target_artifact": target_artifact,
                "selected_fix_keys": selected_fix_keys,
                "selected_issue_ids": selected_issue_ids,
                "selected_report_fix_count": len(report_fixes),
                "matched_fix_count": len(matched_records),
                "matched_issue_types": [section["issue_type"] for section in ordered_issue_sections],
                "screenshot_mode": screenshot_mode,
                "snapshot_window_count": len(snapshot_windows),
                "realized_snapshots": realized_snapshots,
            })
            return

        fieldnames, columns, _, valid_records = load_rows_with_context(source["path"])
        sections = build_individual_profile_sections(
            valid_records,
            fieldnames,
            columns,
            selected_individuals,
            target_artifact,
        )
        if not sections:
            raise SystemExit("None of the selected individuals were found")

        effective_output_mode = output_mode if len(sections) > 1 else "combined"
        if effective_output_mode == "combined":
            required_outputs = {
                "movement_individual_reports.md",
                "movement_individual_reports.html",
            }
            if any(name not in output_by_name for name in required_outputs):
                raise SystemExit("Individual profile outputs were not declared")
            combined_markdown = build_individual_profile_markdown_report(target_artifact, user, sections, snapshots_by_key)
            combined_html = build_individual_profile_html_report(target_artifact, user, sections, snapshots_by_key)
            markdown_path = Path(output_by_name["movement_individual_reports.md"]["path"])
            html_path = Path(output_by_name["movement_individual_reports.html"]["path"])
            markdown_path.parent.mkdir(parents=True, exist_ok=True)
            html_path.parent.mkdir(parents=True, exist_ok=True)
            markdown_path.write_text(combined_markdown, encoding="utf-8")
            html_path.write_text(combined_html, encoding="utf-8")
        else:
            required_outputs = {
                "movement_individual_report_index.md",
                "movement_individual_report_index.html",
            }
            if any(name not in output_by_name for name in required_outputs):
                raise SystemExit("Individual profile index outputs were not declared")
            plan_by_individual = {
                item["individual"]: item
                for item in individual_report_artifacts
                if item["individual"] and item["markdown_name"] and item["html_name"]
            }
            if any(section["individual"] not in plan_by_individual for section in sections):
                raise SystemExit("Per-individual outputs were not declared")

            for section in sections:
                artifact_plan = plan_by_individual[section["individual"]]
                markdown_name = artifact_plan["markdown_name"]
                html_name = artifact_plan["html_name"]
                if markdown_name not in output_by_name or html_name not in output_by_name:
                    raise SystemExit("Per-individual outputs were not declared")
                markdown_text = build_individual_profile_markdown_report(target_artifact, user, [section], snapshots_by_key)
                html_text = build_individual_profile_html_report(target_artifact, user, [section], snapshots_by_key)
                markdown_path = Path(output_by_name[markdown_name]["path"])
                html_path = Path(output_by_name[html_name]["path"])
                markdown_path.parent.mkdir(parents=True, exist_ok=True)
                html_path.parent.mkdir(parents=True, exist_ok=True)
                markdown_path.write_text(markdown_text, encoding="utf-8")
                html_path.write_text(html_text, encoding="utf-8")

            ordered_artifacts = [plan_by_individual[section["individual"]] for section in sections]
            index_markdown = build_individual_profile_index_markdown(sections, ordered_artifacts)
            index_html = build_individual_profile_index_html(sections, ordered_artifacts)
            markdown_index_path = Path(output_by_name["movement_individual_report_index.md"]["path"])
            html_index_path = Path(output_by_name["movement_individual_report_index.html"]["path"])
            markdown_index_path.parent.mkdir(parents=True, exist_ok=True)
            html_index_path.parent.mkdir(parents=True, exist_ok=True)
            markdown_index_path.write_text(index_markdown, encoding="utf-8")
            html_index_path.write_text(index_html, encoding="utf-8")

        write_json(summary_path, {
            "app": "movement",
            "action": "generate_report",
            "report_type": report_type,
            "output_mode": effective_output_mode,
            "target_artifact": target_artifact,
            "individuals": [section["individual"] for section in sections],
            "individual_count": len(sections),
            "realized_snapshots": realized_snapshots,
        })


if __name__ == "__main__":
    main()
