from math import isfinite
from statistics import fmean, median, pstdev

from .movement_features import haversine_meters


STEP_LENGTH_FIELD = "step_length_m"
SPEED_FIELD = "speed_mps"
TIME_GAP_FIELD = "time_delta_s"
RAW_NUMERIC_PREFIXES = ("gps:", "height-above")


def _as_finite_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if isfinite(number) else None


def _fix_value(fix: dict, field: str) -> object:
    attributes = fix.get("attributes")
    if isinstance(attributes, dict) and field in attributes:
        return attributes[field]
    return fix.get(field)


def _numeric_values(fixes: list[dict], field: str) -> list[float]:
    values = []
    for fix in fixes:
        value = _as_finite_float(_fix_value(fix, field))
        if value is not None:
            values.append(value)
    return values


def _numeric_summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"mean": None, "median": None, "max": None, "sd": None}
    return {
        "mean": float(fmean(values)),
        "median": float(median(values)),
        "max": float(max(values)),
        "sd": float(pstdev(values)),
    }


def _is_raw_numeric_source_field(field: object) -> bool:
    normalized = str(field).lower()
    return normalized.startswith(RAW_NUMERIC_PREFIXES)


def _raw_numeric_source_fields(fixes: list[dict]) -> list[str]:
    candidates: set[str] = set()
    for fix in fixes:
        candidates.update(str(field) for field in fix if _is_raw_numeric_source_field(field))
        attributes = fix.get("attributes")
        if isinstance(attributes, dict):
            candidates.update(
                str(field) for field in attributes if _is_raw_numeric_source_field(field)
            )
    return sorted(field for field in candidates if _numeric_values(fixes, field))


def _burst_sort_key(burst: dict) -> tuple[str, str, int, int, str]:
    return (
        str(burst.get("individual", "")),
        str(burst.get("set_name", "")),
        int(burst.get("start_time_ms", 0)),
        int(burst.get("burst_idx", 0)),
        str(burst.get("burst_id", "")),
    )


def build_burst_feature_rows(fixes: list[dict], bursts: list[dict]) -> list[dict]:
    """Build deterministic movement feature rows from automatic burst memberships."""
    fixes_by_key: dict[str, dict] = {}
    for fix in fixes:
        fix_key = str(fix["fix_key"])
        if fix_key in fixes_by_key:
            raise ValueError(f"Duplicate fix_key in feature input: {fix_key}")
        fixes_by_key[fix_key] = fix

    raw_numeric_fields = _raw_numeric_source_fields(fixes)
    feature_rows = []
    for burst in sorted(bursts, key=_burst_sort_key):
        fix_keys = [str(fix_key) for fix_key in burst.get("fix_keys", [])]
        if not fix_keys:
            raise ValueError(f"Burst has no fix_keys: {burst.get('burst_id', '')}")
        missing_fix_keys = [fix_key for fix_key in fix_keys if fix_key not in fixes_by_key]
        if missing_fix_keys:
            raise ValueError(
                f"Burst {burst.get('burst_id', '')} references missing fixes: {missing_fix_keys}"
            )

        burst_fixes = [fixes_by_key[fix_key] for fix_key in fix_keys]
        transition_fixes = burst_fixes[1:]
        step_lengths = _numeric_values(transition_fixes, STEP_LENGTH_FIELD)
        speeds = _numeric_values(transition_fixes, SPEED_FIELD)
        time_gaps = _numeric_values(transition_fixes, TIME_GAP_FIELD)
        start_time_ms = int(burst.get("start_time_ms", burst_fixes[0]["time_ms"]))
        end_time_ms = int(burst.get("end_time_ms", burst_fixes[-1]["time_ms"]))
        path_length_m = float(sum(step_lengths))
        if len(burst_fixes) == 1:
            net_displacement_m = 0.0
        else:
            net_displacement_m = haversine_meters(
                float(burst_fixes[0]["lon"]),
                float(burst_fixes[0]["lat"]),
                float(burst_fixes[-1]["lon"]),
                float(burst_fixes[-1]["lat"]),
            )

        step_summary = _numeric_summary(step_lengths)
        speed_summary = _numeric_summary(speeds)
        row = {
            "burst_id": str(burst["burst_id"]),
            "individual": str(burst.get("individual", burst_fixes[0].get("individual", ""))),
            "start_time_ms": start_time_ms,
            "end_time_ms": end_time_ms,
            "n_fixes": len(burst_fixes),
            "fix_keys": fix_keys,
            "duration_s": float((end_time_ms - start_time_ms) / 1000.0),
            "path_length_m": path_length_m,
            "mean_step_length_m": step_summary["mean"],
            "sd_step_length_m": step_summary["sd"],
            "net_displacement_m": float(net_displacement_m),
            "straightness": (
                float(net_displacement_m / path_length_m) if path_length_m > 0.0 else None
            ),
            "mean_speed_mps": speed_summary["mean"],
            "median_speed_mps": speed_summary["median"],
            "max_speed_mps": speed_summary["max"],
            "sd_speed_mps": speed_summary["sd"],
            "max_time_gap_s": float(max(time_gaps)) if time_gaps else None,
        }
        if "set_name" in burst:
            row["set_name"] = str(burst["set_name"])
        elif "set" in burst_fixes[0]:
            row["set_name"] = str(burst_fixes[0]["set"])

        for field in raw_numeric_fields:
            summary = _numeric_summary(_numeric_values(burst_fixes, field))
            for statistic, value in summary.items():
                row[f"{field}__{statistic}"] = value
        feature_rows.append(row)
    return feature_rows
