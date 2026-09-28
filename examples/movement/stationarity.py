"""Conservative stationarity candidates, shared by CSV and RDS review filters."""

from hashlib import sha256
from math import isfinite

from .movement_features import geodesic_distance_meters


ALGORITHM = "anchor-radius-v2"
SUPPORTED_ALGORITHMS = {"anchor-radius-v1", ALGORITHM}


def validate_stationarity_filter(value: dict) -> dict:
    algorithm = value.get("algorithm", ALGORITHM)
    if not isinstance(algorithm, str) or algorithm not in SUPPORTED_ALGORITHMS:
        raise ValueError("Unsupported stationarity algorithm")
    result = {"kind": "stationarity", "algorithm": algorithm}
    for key in ("radius_m", "minimum_duration_s", "maximum_gap_s"):
        raw = value.get(key)
        try:
            number = float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Stationarity {key} must be a positive number") from exc
        if isinstance(raw, bool) or not isfinite(number) or number <= 0:
            raise ValueError(f"Stationarity {key} must be a positive number")
        result[key] = number
    points = value.get("minimum_fixes", 3)
    if isinstance(points, bool) or not isinstance(points, int) or points < 3:
        raise ValueError("Stationarity requires at least 3 fixes")
    position = value.get("position", "ends")
    if position not in {"ends", "anywhere"}:
        raise ValueError("Stationarity position must be ends or anywhere")
    result.update(
        minimum_fixes=points,
        position=position,
        implementation_sha256=sha256(__loader__.get_source(__name__).encode("utf-8")).hexdigest(),
    )
    return result


def stationary_fix_keys(records: list[dict], spec: dict) -> list[str]:
    """Scan one individual/source/track set in chronological order.

    Each non-overlapping run has a fixed centre: its first fix. A fix outside
    that radius starts the next run. This bounds spatial extent even under
    slow drift; it is deliberately not an exhaustive search over all possible
    centres. Gaps, non-increasing times, tag/time-fragment changes and
    excluded/invalid fixes break runs. Source burst IDs do not override the
    chosen gap limit in v2; explicit v1 filters retain their original rules.
    Ends means the actual first/last record of this track, not the edges of
    internal gaps or bursts. No coordinates are changed.
    """
    if not records:
        return []
    matched = []
    run = []
    run_start = 0
    boundary_key = "burst" if spec.get("algorithm", ALGORITHM) == "anchor-radius-v1" else "segment"

    def finish(end: int) -> None:
        if len(run) < spec["minimum_fixes"]:
            return
        if run[-1]["time_ms"] - run[0]["time_ms"] < spec["minimum_duration_s"] * 1000:
            return
        if spec["position"] == "ends" and run_start != 0 and end != len(records):
            return
        matched.extend(item["fix_key"] for item in run)

    for index, record in enumerate(records):
        if record.get("excluded"):
            finish(index)
            run = []
            continue
        if run:
            gap_ms = record["time_ms"] - run[-1]["time_ms"]
            distance = geodesic_distance_meters(
                run[0]["lon"], run[0]["lat"], record["lon"], record["lat"]
            )
            if (
                gap_ms <= 0 or gap_ms > spec["maximum_gap_s"] * 1000
                or record.get(boundary_key) != run[-1].get(boundary_key)
                or distance is None or not isfinite(distance) or distance > spec["radius_m"]
            ):
                finish(index)
                run = []
        if not run:
            run_start = index
        run.append(record)
    finish(len(records))
    return matched
