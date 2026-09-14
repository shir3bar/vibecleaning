from math import isfinite
from statistics import fmean, median, pstdev

from .movement_features import burst_movement_summary


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


def _osm_distance_summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, **_numeric_summary(values)}
    return {"min": float(min(values)), **_numeric_summary(values)}


def _is_raw_numeric_source_field(field: object) -> bool:
    normalized = str(field).lower()
    return normalized.startswith(RAW_NUMERIC_PREFIXES)


def _is_osm_distance_source_field(field: object) -> bool:
    normalized = str(field).lower()
    return normalized.startswith("osm:") and normalized.endswith("_distance_m")


def _numeric_source_fields(fixes: list[dict], field_matcher) -> list[str]:
    candidates: set[str] = set()
    for fix in fixes:
        candidates.update(str(field) for field in fix if field_matcher(field))
        attributes = fix.get("attributes")
        if isinstance(attributes, dict):
            candidates.update(
                str(field) for field in attributes if field_matcher(field)
            )
    return sorted(field for field in candidates if _numeric_values(fixes, field))


def _raw_numeric_source_fields(fixes: list[dict]) -> list[str]:
    return _numeric_source_fields(fixes, _is_raw_numeric_source_field)


def _osm_distance_source_fields(fixes: list[dict]) -> list[str]:
    return _numeric_source_fields(fixes, _is_osm_distance_source_field)


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
    osm_distance_fields = _osm_distance_source_fields(fixes)
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

        burst_fixes = sorted(
            (fixes_by_key[key] for key in fix_keys if not fixes_by_key[key].get("analytically_excluded", False)),
            key=lambda fix: (int(fix["time_ms"]), str(fix["fix_key"])),
        )
        if not burst_fixes:
            continue
        fix_keys = [str(fix["fix_key"]) for fix in burst_fixes]
        row = {
            "burst_id": str(burst["burst_id"]),
            "individual": str(burst.get("individual", burst_fixes[0].get("individual", ""))),
            "fix_keys": fix_keys,
            **burst_movement_summary(
                [fix["time_ms"] for fix in burst_fixes],
                [fix["lon"] for fix in burst_fixes],
                [fix["lat"] for fix in burst_fixes],
            ),
        }
        if "set_name" in burst:
            row["set_name"] = str(burst["set_name"])
        elif "set" in burst_fixes[0]:
            row["set_name"] = str(burst_fixes[0]["set"])

        for field in raw_numeric_fields:
            summary = _numeric_summary(_numeric_values(burst_fixes, field))
            for statistic, value in summary.items():
                row[f"{field}__{statistic}"] = value
        for field in osm_distance_fields:
            summary = _osm_distance_summary(_numeric_values(burst_fixes, field))
            for statistic, value in summary.items():
                row[f"{field}__{statistic}"] = value
        feature_rows.append(row)
    return feature_rows
