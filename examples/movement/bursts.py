from math import isfinite
from statistics import median

from .movement_features import geodesic_distance_meters


DEFAULT_BURST_GAP_MODE = "manual"
DEFAULT_BURST_GAP_SECONDS = 3600.0
DEFAULT_BURST_GAP_QUANTILE = 0.999


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


def _record_sort_key(record: dict) -> tuple[int, int, str]:
    return int(record["time_ms"]), int(record["row_index"]), str(record["fix_key"])


def _sorted_track_records(records_by_group: dict[tuple[str, str], list[dict]]):
    for group_key in sorted(records_by_group):
        yield group_key, sorted(records_by_group[group_key], key=_record_sort_key)


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    index = (len(ordered) - 1) * q
    lower_index = int(index)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = index - lower_index
    return ordered[lower_index] + fraction * (ordered[upper_index] - ordered[lower_index])


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
        quantile_seconds = _quantile(gaps, quantile_value)
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


def burst_gap_metadata(burst_gap: dict) -> dict:
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


def build_auto_bursts(records: list[dict], *, burst_gap_seconds: float) -> list[dict]:
    """Build automatic per-track time-gap bursts used for movement review."""
    gap_seconds = normalize_burst_gap_seconds(burst_gap_seconds)
    grouped: dict[tuple[str, str], list[dict]] = {}
    for record in records:
        grouped.setdefault((record["individual"], record["set_name"]), []).append(record)
    bursts = []
    for (individual, set_name), group_records in grouped.items():
        sorted_records = sorted(group_records, key=_record_sort_key)
        burst_idx = -1
        current_rows = []
        previous_time_ms = None
        for record in sorted_records:
            starts_new = previous_time_ms is None or (
                (record["time_ms"] - previous_time_ms) / 1000.0
            ) > gap_seconds
            if starts_new:
                if current_rows:
                    bursts.append(
                        _finalize_auto_burst(
                            individual,
                            set_name,
                            burst_idx,
                            current_rows,
                            gap_seconds,
                        )
                    )
                burst_idx += 1
                current_rows = []
            current_rows.append(record)
            previous_time_ms = record["time_ms"]
        if current_rows:
            bursts.append(
                _finalize_auto_burst(individual, set_name, burst_idx, current_rows, gap_seconds)
            )
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
    path = [row["position"] for row in rows]
    step_lengths = [
        geodesic_distance_meters(*path[index - 1], *path[index])
        for index in range(1, len(path))
    ]
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
        "path": path,
        "path_length_m": float(sum(step_lengths)),
        "median_step_m": float(median(step_lengths)) if step_lengths else None,
    }
