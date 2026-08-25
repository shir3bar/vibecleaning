import random
from math import degrees, isfinite, pi, radians

from pyproj import Geod


MAX_STAT_SAMPLES = 2000
STEP_FEATURE_FIELDS = (
    "step_length_m",
    "speed_mps",
    "time_delta_s",
    "turn_angle_deg",
)
WGS84_GEOD = Geod(ellps="WGS84")


def _wgs84_inverse(
    lon1: float,
    lat1: float,
    lon2: float,
    lat2: float,
) -> tuple[float | None, float | None]:
    """Return forward azimuth in radians and geodesic distance in metres.

    PROJ's geodesic inverse is the Python equivalent of the WGS84 geodesic
    calculation used by ``lwgeom::st_geod_azimuth()`` and by ``sf`` when its
    ellipsoidal distance engine is active.
    """
    azimuth_degrees, _back_azimuth_degrees, distance_m = WGS84_GEOD.inv(
        lon1,
        lat1,
        lon2,
        lat2,
    )
    if not (isfinite(azimuth_degrees) and isfinite(distance_m)):
        return None, None
    bearing = None if distance_m == 0.0 else radians(azimuth_degrees)
    return bearing, float(distance_m)


def geodesic_distance_meters(
    lon1: float,
    lat1: float,
    lon2: float,
    lat2: float,
) -> float:
    """Return WGS84 geodesic distance, including across poles/dateline."""
    _bearing, distance_m = _wgs84_inverse(lon1, lat1, lon2, lat2)
    if distance_m is None:
        raise ValueError("WGS84 geodesic distance is undefined for these coordinates")
    return distance_m


def step_movement_metrics(
    previous_time_ms: int,
    previous_lon: float,
    previous_lat: float,
    current_time_ms: int,
    current_lon: float,
    current_lat: float,
) -> dict[str, float | None]:
    """Return move2-style metrics from the first fix to the next fix.

    ``mt_distance()``, ``mt_time_lags()`` and ``mt_speed()`` attach the value
    to the segment's starting fix.  A zero time lag retains distance and lag,
    but speed is undefined (move2 rejects zero-lag tracks when asked for
    speed).
    """
    time_delta_s = (current_time_ms - previous_time_ms) / 1000.0
    step_length_m = geodesic_distance_meters(
        previous_lon,
        previous_lat,
        current_lon,
        current_lat,
    )
    return {
        "step_length_m": step_length_m,
        "speed_mps": step_length_m / time_delta_s if time_delta_s > 0 else None,
        "time_delta_s": time_delta_s,
    }


def initial_bearing_radians(
    lon1: float,
    lat1: float,
    lon2: float,
    lat2: float,
) -> float | None:
    """Return the initial bearing from the first fix to the second."""
    bearing, _distance_m = _wgs84_inverse(lon1, lat1, lon2, lat2)
    return bearing


def centered_turn_angle_degrees(
    previous: dict,
    center: dict,
    following: dict,
) -> float | None:
    """Return the signed direction change at the center of three fixes."""
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
    # move2 0.5 normalizes the ambiguous exact reversal to +pi rather than -pi.
    if signed_turn == -pi:
        signed_turn = pi
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
        if not sorted_records:
            continue
        individual = sorted_records[0]["individual"]
        indiv_stats = stat_samples.setdefault(
            individual,
            {"seen_fix": 0, "seen_step": 0, "seen_speed": 0, "fix": [], "step": [], "speed": []},
        )
        for record in sorted_records:
            movement = {
                "step_length_m": None,
                "speed_mps": None,
                "time_delta_s": None,
                "turn_angle_deg": None,
            }
            movement_by_fix_key[record["fix_key"]] = movement

        # move2 aligns distance, lag and speed with the segment's starting fix.
        for index in range(len(sorted_records) - 1):
            record = sorted_records[index]
            following = sorted_records[index + 1]
            movement = movement_by_fix_key[record["fix_key"]]
            movement.update(
                step_movement_metrics(
                    record["time_ms"],
                    record["lon"],
                    record["lat"],
                    following["time_ms"],
                    following["lon"],
                    following["lat"],
                )
            )
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
