import random
from math import atan2, cos, degrees, pi, radians, sin, sqrt


MAX_STAT_SAMPLES = 2000
STEP_FEATURE_FIELDS = (
    "step_length_m",
    "speed_mps",
    "time_delta_s",
    "turn_angle_deg",
)
EARTH_RADIUS_M = 6371000.0


def haversine_meters(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    bounded_a = min(1.0, max(0.0, a))
    return EARTH_RADIUS_M * 2 * atan2(sqrt(bounded_a), sqrt(1 - bounded_a))


def step_movement_metrics(
    previous_time_ms: int,
    previous_lon: float,
    previous_lat: float,
    current_time_ms: int,
    current_lon: float,
    current_lat: float,
) -> dict[str, float | None]:
    """Return canonical inbound movement metrics for two fixes."""
    if current_time_ms <= previous_time_ms:
        return {
            "step_length_m": None,
            "speed_mps": None,
            "time_delta_s": None,
        }
    time_delta_s = (current_time_ms - previous_time_ms) / 1000.0
    step_length_m = haversine_meters(
        previous_lon,
        previous_lat,
        current_lon,
        current_lat,
    )
    return {
        "step_length_m": step_length_m,
        "speed_mps": step_length_m / time_delta_s,
        "time_delta_s": time_delta_s,
    }


def initial_bearing_radians(
    lon1: float,
    lat1: float,
    lon2: float,
    lat2: float,
) -> float | None:
    """Return the initial bearing from the first fix to the second."""
    if lon1 == lon2 and lat1 == lat2:
        return None
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_lambda = radians(lon2 - lon1)
    y = sin(delta_lambda) * cos(phi2)
    x = cos(phi1) * sin(phi2) - sin(phi1) * cos(phi2) * cos(delta_lambda)
    if x == 0.0 and y == 0.0:
        return None
    return atan2(y, x)


def centered_turn_angle_degrees(
    previous: dict,
    center: dict,
    following: dict,
) -> float | None:
    """Return the signed direction change at the center of three fixes."""
    if not (
        center["time_ms"] > previous["time_ms"]
        and following["time_ms"] > center["time_ms"]
    ):
        return None
    inbound = initial_bearing_radians(
        previous["lon"],
        previous["lat"],
        center["lon"],
        center["lat"],
    )
    outbound = initial_bearing_radians(
        center["lon"],
        center["lat"],
        following["lon"],
        following["lat"],
    )
    if inbound is None or outbound is None:
        return None
    signed_turn = (outbound - inbound + pi) % (2.0 * pi) - pi
    return degrees(signed_turn)


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
            movement = (
                step_movement_metrics(
                    previous["time_ms"],
                    previous["lon"],
                    previous["lat"],
                    record["time_ms"],
                    record["lon"],
                    record["lat"],
                )
                if previous
                else {
                    "step_length_m": None,
                    "speed_mps": None,
                    "time_delta_s": None,
                    "turn_angle_deg": None,
                }
            )
            movement.setdefault("turn_angle_deg", None)
            step_length_m = movement["step_length_m"]
            speed_mps = movement["speed_mps"]
            time_delta_s = movement["time_delta_s"]
            if time_delta_s is not None:
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
                "turn_angle_deg": movement["turn_angle_deg"],
            }
            previous = record

        for index in range(1, len(sorted_records) - 1):
            center = sorted_records[index]
            movement_by_fix_key[center["fix_key"]]["turn_angle_deg"] = (
                centered_turn_angle_degrees(
                    sorted_records[index - 1],
                    center,
                    sorted_records[index + 1],
                )
            )

    return movement_by_fix_key, stat_samples
