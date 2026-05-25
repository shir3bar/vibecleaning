import random
from math import atan2, cos, radians, sin, sqrt


MAX_STAT_SAMPLES = 2000
STEP_FEATURE_FIELDS = ("step_length_m", "speed_mps", "time_delta_s")


def haversine_meters(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    return 6371000.0 * 2 * atan2(sqrt(a), sqrt(1 - a))


def _record_sort_key(record: dict) -> tuple[int, int, str]:
    return int(record["time_ms"]), int(record["row_index"]), str(record["fix_key"])


def _sorted_track_records(records_by_group: dict[tuple[str, str], list[dict]]):
    for group_key in sorted(records_by_group):
        yield group_key, sorted(records_by_group[group_key], key=_record_sort_key)


def _reservoir_append(sample: list, item, seen_count: int, limit: int):
    if limit <= 0:
        return
    if len(sample) < limit:
        sample.append(item)
        return
    slot = random.randrange(seen_count)
    if slot < limit:
        sample[slot] = item


def compute_track_movement(
    records_by_group: dict[tuple[str, str], list[dict]],
    *,
    max_stat_samples: int = MAX_STAT_SAMPLES,
) -> tuple[dict[str, dict[str, float | None]], dict[str, dict[str, list[float] | int]]]:
    """Compute canonical step features within each individual/track grouping."""
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
                _reservoir_append(indiv_stats["fix"], time_delta_s, indiv_stats["seen_fix"], max_stat_samples)
                _reservoir_append(indiv_stats["step"], step_length_m, indiv_stats["seen_step"], max_stat_samples)
                if speed_mps is not None:
                    indiv_stats["seen_speed"] += 1
                    _reservoir_append(indiv_stats["speed"], speed_mps, indiv_stats["seen_speed"], max_stat_samples)
            movement_by_fix_key[record["fix_key"]] = {
                "step_length_m": step_length_m,
                "speed_mps": speed_mps,
                "time_delta_s": time_delta_s,
            }
            previous = record

    return movement_by_fix_key, stat_samples
