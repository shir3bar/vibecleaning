import hashlib
import json
import math
from collections import defaultdict
from math import isfinite


DEFAULT_SEGMENT_GROUPING = {
    "enabled": False,
    "min_fixes": 2,
    "min_duration_s": 0.0,
    "max_gap_s": None,
    "preview_limit": 200,
}


def normalize_segment_grouping_config(raw_config: object) -> dict:
    config = dict(DEFAULT_SEGMENT_GROUPING)
    if not isinstance(raw_config, dict):
        return config
    config["enabled"] = bool(raw_config.get("enabled"))
    config["min_fixes"] = _positive_int(raw_config.get("min_fixes"), default=2)
    config["min_duration_s"] = _nonnegative_float(raw_config.get("min_duration_s"), default=0.0)
    config["max_gap_s"] = _optional_positive_float(raw_config.get("max_gap_s"))
    config["preview_limit"] = _positive_int(raw_config.get("preview_limit"), default=200)
    return config


def build_candidate_segments(
    *,
    query_definition: dict,
    query_digest_value: str,
    run_digest_value: str,
    track_fixes: list[dict],
    matched_fix_keys: set[str],
    evidence_by_fix_key: dict[str, dict],
    config: dict | None = None,
) -> dict:
    normalized = normalize_segment_grouping_config(config)
    if not normalized["enabled"]:
        return {
            "segment_count": 0,
            "returned_segment_count": 0,
            "candidate_segments": [],
            "segment_grouping": normalized,
            "warnings": [],
        }

    definition = dict((query_definition or {}).get("definition") or {})
    field = str(definition.get("field") or "").strip()
    op = str(definition.get("op") or "").strip()
    expected_value = definition.get("value")
    segments = []
    for _, fixes in sorted(_track_groups(track_fixes).items(), key=lambda item: item[0]):
        current_run: list[dict] = []
        previous_fix = None
        for fix in fixes:
            fix_key = str(fix.get("fix_key") or "")
            is_match = fix_key in matched_fix_keys
            if not is_match:
                _append_segment_if_valid(
                    segments,
                    current_run,
                    query_definition=query_definition,
                    query_digest_value=query_digest_value,
                    run_digest_value=run_digest_value,
                    evidence_by_fix_key=evidence_by_fix_key,
                    field=field,
                    op=op,
                    expected_value=expected_value,
                    config=normalized,
                )
                current_run = []
                previous_fix = fix
                continue
            if current_run and _exceeds_max_gap(previous_fix, fix, normalized["max_gap_s"]):
                _append_segment_if_valid(
                    segments,
                    current_run,
                    query_definition=query_definition,
                    query_digest_value=query_digest_value,
                    run_digest_value=run_digest_value,
                    evidence_by_fix_key=evidence_by_fix_key,
                    field=field,
                    op=op,
                    expected_value=expected_value,
                    config=normalized,
                )
                current_run = []
            current_run.append(fix)
            previous_fix = fix
        _append_segment_if_valid(
            segments,
            current_run,
            query_definition=query_definition,
            query_digest_value=query_digest_value,
            run_digest_value=run_digest_value,
            evidence_by_fix_key=evidence_by_fix_key,
            field=field,
            op=op,
            expected_value=expected_value,
            config=normalized,
        )

    sorted_segments = sorted(segments, key=_segment_sort_key(op))
    preview_limit = normalized["preview_limit"]
    returned_segments = sorted_segments[:preview_limit]
    warnings = []
    if len(sorted_segments) > len(returned_segments):
        warnings.append(
            f"Candidate segment preview was limited to {preview_limit} returned segments. "
            "Narrow the query scope or threshold for exhaustive review."
        )
    return {
        "segment_count": len(sorted_segments),
        "returned_segment_count": len(returned_segments),
        "candidate_segments": returned_segments,
        "segment_grouping": normalized,
        "warnings": warnings,
    }


def _track_groups(track_fixes: list[dict]) -> dict[tuple[str, str], list[dict]]:
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for fix in track_fixes:
        individual = str(fix.get("individual") or "").strip()
        set_name = str(fix.get("set_name") or fix.get("set") or "train").strip() or "train"
        fix_key = str(fix.get("fix_key") or "").strip()
        if not individual or not fix_key:
            continue
        groups[(individual, set_name)].append(fix)
    for key, fixes in list(groups.items()):
        groups[key] = sorted(fixes, key=_track_sort_key)
    return groups


def _track_sort_key(fix: dict):
    return (
        _safe_int(fix.get("time_ms")),
        _safe_int(fix.get("row_index")),
        str(fix.get("fix_key") or ""),
    )


def _append_segment_if_valid(
    segments: list[dict],
    run: list[dict],
    *,
    query_definition: dict,
    query_digest_value: str,
    run_digest_value: str,
    evidence_by_fix_key: dict[str, dict],
    field: str,
    op: str,
    expected_value: object,
    config: dict,
):
    if not run or len(run) < config["min_fixes"]:
        return
    start_time_ms = _safe_int(run[0].get("time_ms"))
    end_time_ms = _safe_int(run[-1].get("time_ms"))
    duration_s = max(0.0, (end_time_ms - start_time_ms) / 1000.0)
    if duration_s < config["min_duration_s"]:
        return
    segments.append(
        _segment_payload(
            run,
            query_definition=query_definition,
            query_digest_value=query_digest_value,
            run_digest_value=run_digest_value,
            evidence_by_fix_key=evidence_by_fix_key,
            field=field,
            op=op,
            expected_value=expected_value,
            duration_s=duration_s,
        )
    )


def _segment_payload(
    run: list[dict],
    *,
    query_definition: dict,
    query_digest_value: str,
    run_digest_value: str,
    evidence_by_fix_key: dict[str, dict],
    field: str,
    op: str,
    expected_value: object,
    duration_s: float,
) -> dict:
    fix_keys = [str(fix.get("fix_key") or "") for fix in run]
    values = [
        value
        for value in (_finite_number((evidence_by_fix_key.get(fix_key) or {}).get("value")) for fix_key in fix_keys)
        if value is not None
    ]
    representative_fix_key = _representative_fix_key(fix_keys, evidence_by_fix_key, op)
    start_fix_key = fix_keys[0]
    end_fix_key = fix_keys[-1]
    individual = str(run[0].get("individual") or "")
    set_name = str(run[0].get("set_name") or run[0].get("set") or "train")
    segment = {
        "segment_id": _segment_id(
            run_digest_value=run_digest_value,
            individual=individual,
            set_name=set_name,
            start_fix_key=start_fix_key,
            end_fix_key=end_fix_key,
        ),
        "kind": "segment",
        "source_query_id": str((query_definition or {}).get("query_id") or ""),
        "source_query_version": (query_definition or {}).get("version"),
        "query_digest": query_digest_value,
        "run_digest": run_digest_value,
        "individual": individual,
        "set_name": set_name,
        "start_time_ms": _safe_int(run[0].get("time_ms")),
        "end_time_ms": _safe_int(run[-1].get("time_ms")),
        "duration_s": duration_s,
        "n_fixes": len(run),
        "fix_keys": fix_keys,
        "start_fix_key": start_fix_key,
        "end_fix_key": end_fix_key,
        "representative_fix_key": representative_fix_key,
        "evidence_field": field,
        "op": op,
        "summary": _segment_summary(
            individual=individual,
            set_name=set_name,
            n_fixes=len(run),
            duration_s=duration_s,
            field=field,
            values=values,
        ),
    }
    if op in {"<", "<=", ">", ">="}:
        threshold = _finite_number(expected_value)
        if threshold is not None:
            segment["threshold"] = threshold
    else:
        segment["expected_value"] = "" if expected_value is None else str(expected_value)
    if values:
        sorted_values = sorted(values)
        segment["min_value"] = sorted_values[0]
        segment["median_value"] = _median(sorted_values)
        segment["max_value"] = sorted_values[-1]
    return segment


def _representative_fix_key(fix_keys: list[str], evidence_by_fix_key: dict[str, dict], op: str) -> str:
    scored = []
    for index, fix_key in enumerate(fix_keys):
        value = _finite_number((evidence_by_fix_key.get(fix_key) or {}).get("value"))
        if value is None:
            continue
        score = value if op in {"<", "<="} else -value
        scored.append((score, index, fix_key))
    if not scored:
        return fix_keys[0] if fix_keys else ""
    return sorted(scored)[0][2]


def _segment_sort_key(op: str):
    def key(segment: dict):
        start_time_ms = _safe_int(segment.get("start_time_ms"))
        segment_id = str(segment.get("segment_id") or "")
        n_fixes = _safe_int(segment.get("n_fixes"))
        duration_s = _safe_float(segment.get("duration_s"))
        min_value = _finite_number(segment.get("min_value"))
        max_value = _finite_number(segment.get("max_value"))
        if op in {"<", "<="} and min_value is not None:
            return (0, min_value, -n_fixes, -duration_s, start_time_ms, segment_id)
        if op in {">", ">="} and max_value is not None:
            return (0, -max_value, -n_fixes, -duration_s, start_time_ms, segment_id)
        return (1, -n_fixes, -duration_s, start_time_ms, segment_id)

    return key


def _exceeds_max_gap(previous_fix: dict | None, current_fix: dict, max_gap_s: float | None) -> bool:
    if max_gap_s is None or previous_fix is None:
        return False
    previous_time_ms = _safe_int(previous_fix.get("time_ms"))
    current_time_ms = _safe_int(current_fix.get("time_ms"))
    return (current_time_ms - previous_time_ms) / 1000.0 > max_gap_s


def _segment_id(
    *,
    run_digest_value: str,
    individual: str,
    set_name: str,
    start_fix_key: str,
    end_fix_key: str,
) -> str:
    payload = {
        "run_digest": run_digest_value,
        "individual": individual,
        "set_name": set_name,
        "start_fix_key": start_fix_key,
        "end_fix_key": end_fix_key,
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return f"cqs:{hashlib.sha256(serialized.encode('utf-8')).hexdigest()[:16]}"


def _segment_summary(
    *,
    individual: str,
    set_name: str,
    n_fixes: int,
    duration_s: float,
    field: str,
    values: list[float],
) -> str:
    duration_text = _format_duration(duration_s)
    if values:
        return (
            f"{individual} {set_name}: {n_fixes} consecutive fixes over {duration_text}; "
            f"min {field} {_format_number(min(values))}."
        )
    return f"{individual} {set_name}: {n_fixes} consecutive fixes over {duration_text}."


def _format_duration(duration_s: float) -> str:
    if duration_s < 60:
        return f"{duration_s:.0f} s"
    if duration_s < 3600:
        return f"{duration_s / 60.0:.1f} min"
    return f"{duration_s / 3600.0:.1f} h"


def _format_number(value: float) -> str:
    if abs(value) >= 100:
        return f"{value:.0f}"
    if abs(value) >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def _median(sorted_values: list[float]) -> float:
    if not sorted_values:
        return math.nan
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2.0


def _finite_number(raw_value: object) -> float | None:
    if isinstance(raw_value, bool):
        return None
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    return value if isfinite(value) else None


def _safe_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _positive_int(value: object, *, default: int) -> int:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, parsed)


def _nonnegative_float(value: object, *, default: float) -> float:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if not isfinite(parsed):
        return default
    return max(0.0, parsed)


def _optional_positive_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed <= 0:
        return None
    return parsed
