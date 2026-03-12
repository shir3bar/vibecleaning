import csv
import random
from functools import lru_cache
from math import isfinite
from pathlib import Path


MAX_SERIES_POINTS = 5000
MAX_FIX_POINTS = 20000
MAX_STAT_SAMPLES = 5000


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
        "individual": find_column(normalized, [
            "individual",
            "individualid",
            "individuallocalidentifier",
            "animalid",
            "trackid",
            "taglocalidentifier",
            "id",
        ]),
        "time": find_column(normalized, [
            "timestamp",
            "time",
            "datetime",
            "eventtime",
            "transmissiontimestamp",
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


def is_valid_coordinate(lon: float, lat: float) -> bool:
    return isfinite(lon) and isfinite(lat) and -180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0


def _summary_cache_key(path: Path, include_fixes: bool) -> tuple[str, int, bool]:
    stat = path.stat()
    return (str(path.resolve()), stat.st_mtime_ns, include_fixes)


def build_trajectory_summary(path: Path, *, include_fixes: bool = False) -> dict:
    key = _summary_cache_key(path, include_fixes)
    return _build_trajectory_summary_cached(*key)


@lru_cache(maxsize=32)
def _build_trajectory_summary_cached(path_str: str, _mtime_ns: int, include_fixes: bool) -> dict:
    path = Path(path_str)
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        columns = detect_columns(fieldnames)

        if not columns["individual"] or not columns["time"] or not columns["lon"] or not columns["lat"]:
            raise ValueError("CSV is missing required columns for trajectory visualization")

        species_by_individual: dict[str, str] = {}
        row_counts: dict[str, int] = {}
        group_samples: dict[tuple[str, str], dict] = {}
        stat_samples: dict[str, dict[str, list[float] | int]] = {}
        fix_samples: list[dict] = []

        total_rows = 0
        min_lon = float("inf")
        max_lon = float("-inf")
        min_lat = float("inf")
        max_lat = float("-inf")
        min_time_ms = None
        max_time_ms = None

        for raw in reader:
            individual = str(raw.get(columns["individual"], "")).strip()
            if not individual:
                continue

            try:
                time_ms = int(__import__("datetime").datetime.fromisoformat(
                    str(raw.get(columns["time"], "")).replace("Z", "+00:00")
                ).timestamp() * 1000)
            except ValueError:
                try:
                    time_ms = int(__import__("datetime").datetime.strptime(
                        str(raw.get(columns["time"], "")).strip(), "%Y-%m-%d %H:%M:%S"
                    ).timestamp() * 1000)
                except ValueError:
                    continue

            try:
                lon = float(raw.get(columns["lon"], ""))
                lat = float(raw.get(columns["lat"], ""))
            except ValueError:
                continue
            if not is_valid_coordinate(lon, lat):
                continue

            set_name = str(raw.get(columns["set"], "")).strip().lower() if columns["set"] else "train"
            if set_name != "test":
                set_name = "train"

            common_name = str(raw.get(columns["common_name"], "")).strip() if columns["common_name"] else ""
            scientific_name = str(raw.get(columns["scientific_name"], "")).strip() if columns["scientific_name"] else ""
            if not species_by_individual.get(individual):
                species_by_individual[individual] = common_name or scientific_name or "Unknown species"

            total_rows += 1
            row_counts[individual] = row_counts.get(individual, 0) + 1

            min_lon = min(min_lon, lon)
            max_lon = max(max_lon, lon)
            min_lat = min(min_lat, lat)
            max_lat = max(max_lat, lat)
            min_time_ms = time_ms if min_time_ms is None else min(min_time_ms, time_ms)
            max_time_ms = time_ms if max_time_ms is None else max(max_time_ms, time_ms)

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

            indiv_stats = stat_samples.setdefault(individual, {"seen_fix": 0, "seen_step": 0, "fix": [], "step": []})
            previous = group["prev"]
            if previous and time_ms > previous[0]:
                delta_s = (time_ms - previous[0]) / 1000.0
                step_m = haversine_meters(previous[1], previous[2], lon, lat)
                indiv_stats["seen_fix"] += 1
                indiv_stats["seen_step"] += 1
                reservoir_append(indiv_stats["fix"], delta_s, indiv_stats["seen_fix"], MAX_STAT_SAMPLES)
                reservoir_append(indiv_stats["step"], step_m, indiv_stats["seen_step"], MAX_STAT_SAMPLES)
            group["prev"] = (time_ms, lon, lat)

            if include_fixes:
                reservoir_append(
                    fix_samples,
                    {
                        "individual": individual,
                        "set": set_name,
                        "time_ms": time_ms,
                        "lon": lon,
                        "lat": lat,
                    },
                    total_rows,
                    MAX_FIX_POINTS,
                )

    if total_rows == 0 or min_time_ms is None or max_time_ms is None:
        raise ValueError("CSV did not contain any valid trajectory rows")

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
        indiv_stats = stat_samples.get(individual, {"fix": [], "step": []})
        stats[individual] = {
            "row_count": int(row_counts.get(individual, 0)),
            "median_fix_s": median(indiv_stats.get("fix", [])),
            "median_step_m": median(indiv_stats.get("step", [])),
            "p95_step_m": quantile(indiv_stats.get("step", []), 0.95),
        }

    span = max(max_lon - min_lon, max_lat - min_lat)
    summary = {
        "total_rows": int(total_rows),
        "individuals": individuals,
        "species_by_individual": species_by_individual,
        "stats": stats,
        "coverage_by_individual": coverage_by_individual,
        "series_by_individual": series_by_individual,
        "initial_view": {
            "longitude": float((min_lon + max_lon) / 2),
            "latitude": float((min_lat + max_lat) / 2),
            "zoom": float(span_to_zoom(float(span))),
        },
        "min_time_ms": int(min_time_ms),
        "max_time_ms": int(max_time_ms),
    }
    if include_fixes:
        summary["fixes"] = fix_samples
    return summary
