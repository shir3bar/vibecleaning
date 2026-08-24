"""Compact exact movement-map columns shared by CSV and RDS adapters."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np

from .rds_index import _pack_binary_columns


DERIVED_FIELD_KEYS = (
    "step_length_m",
    "speed_mps",
    "time_delta_s",
    "turn_angle_deg",
)


def _pack_strings(values: list[str]) -> tuple[np.ndarray, np.ndarray]:
    encoded = [str(value).encode("utf-8") for value in values]
    offsets = np.zeros(len(encoded) + 1, dtype=np.uint32)
    cursor = 0
    chunks: list[bytes] = []
    for index, value in enumerate(encoded, start=1):
        chunks.append(value)
        cursor += len(value)
        offsets[index] = cursor
    return offsets, np.frombuffer(b"".join(chunks), dtype=np.uint8).copy()


def _review_code(fix: dict) -> int:
    status = str((fix.get("review") or {}).get("status") or "").strip().lower()
    if status == "confirmed" or bool(fix.get("analytically_excluded")):
        return 2
    if status == "suspected":
        return 1
    return 0


def _attribute_columns(
    fixes: list[dict],
    color_fields: list[dict] | None = None,
) -> tuple[dict[str, np.ndarray], dict[str, dict]]:
    declared_kinds = {
        str(item.get("key") or ""): str(item.get("kind") or "")
        for item in (color_fields or [])
        if str(item.get("key") or "")
    }
    values_by_field: dict[str, list[Any]] = defaultdict(lambda: [None] * len(fixes))
    for index, fix in enumerate(fixes):
        for field, value in (fix.get("attributes") or {}).items():
            values_by_field[str(field)][index] = value

    arrays: dict[str, np.ndarray] = {}
    metadata: dict[str, dict] = {}
    for field_index, field in enumerate(sorted(values_by_field)):
        values = values_by_field[field]
        present = [value for value in values if value is not None and value != ""]
        array_name = f"color_{field_index}"
        declared_kind = declared_kinds.get(field)
        if declared_kind == "boolean" or (
            declared_kind is None
            and present
            and all(isinstance(value, (bool, np.bool_)) for value in present)
        ):
            target = np.full(len(values), 255, dtype=np.uint8)
            for index, value in enumerate(values):
                if value is not None and value != "":
                    target[index] = 1 if bool(value) else 0
            arrays[array_name] = target
            metadata[field] = {"array": array_name, "kind": "boolean"}
            continue
        if declared_kind == "numeric" or (
            declared_kind is None
            and present
            and all(
            isinstance(value, (int, float, np.integer, np.floating))
            and not isinstance(value, (bool, np.bool_))
            for value in present
            )
        ):
            target = np.full(len(values), np.nan, dtype=np.float64)
            for index, value in enumerate(values):
                if value is not None and value != "":
                    target[index] = float(value)
            arrays[array_name] = target
            metadata[field] = {"array": array_name, "kind": "numeric"}
            continue
        levels = sorted({str(value) for value in present})
        level_codes = {value: index + 1 for index, value in enumerate(levels)}
        dtype = np.uint16 if len(levels) <= 65534 else np.uint32
        target = np.zeros(len(values), dtype=dtype)
        for index, value in enumerate(values):
            if value is not None and value != "":
                target[index] = level_codes[str(value)]
        arrays[array_name] = target
        metadata[field] = {
            "array": array_name,
            "kind": "categorical",
            "levels": levels,
            "null_code": 0,
        }
    return arrays, metadata


def build_csv_binary_columns(
    payload: dict,
    *,
    source_signature: str,
    logical_name: str,
    all_individuals: list[str],
) -> bytes:
    fixes = list(payload.get("fixes") or [])
    fixes.sort(
        key=lambda fix: (
            str(fix.get("individual") or ""),
            str(fix.get("set") or "train"),
            int(fix.get("time_ms") or 0),
            str(fix.get("fix_key") or ""),
        )
    )
    individuals = [str(value) for value in all_individuals]
    individual_codes_by_name = {value: index for index, value in enumerate(individuals)}
    sets = sorted({str(fix.get("set") or "train") for fix in fixes}) or ["train"]
    set_codes_by_name = {value: index for index, value in enumerate(sets)}
    count = len(fixes)

    positions = np.empty((count, 2), dtype=np.float64)
    time_ms = np.empty(count, dtype=np.float64)
    individual_codes = np.empty(count, dtype=np.uint16 if len(individuals) <= 65535 else np.uint32)
    set_codes = np.empty(count, dtype=np.uint8 if len(sets) <= 255 else np.uint16)
    artifact_codes = np.zeros(count, dtype=np.uint16)
    source_rows = np.zeros(count, dtype=np.uint32)
    review_status = np.empty(count, dtype=np.uint8)
    fix_keys: list[str] = []
    source_flag_values = sorted({
        str(flag)
        for fix in fixes
        for flag in (fix.get("source_flags") or [])
        if str(flag)
    })
    source_flag_codes = {value: index for index, value in enumerate(source_flag_values[:32])}
    source_flags = np.zeros(count, dtype=np.uint32)

    burst_by_fix: dict[str, str] = {}
    for burst in payload.get("auto_bursts") or []:
        burst_id = str(burst.get("burst_id") or "")
        for fix_key in burst.get("fix_keys") or []:
            burst_by_fix[str(fix_key)] = burst_id
    burst_ids = sorted(set(burst_by_fix.values()))
    burst_codes_by_id = {value: index + 1 for index, value in enumerate(burst_ids)}
    burst_codes = np.zeros(count, dtype=np.uint32)

    for index, fix in enumerate(fixes):
        individual = str(fix.get("individual") or "")
        set_name = str(fix.get("set") or "train")
        fix_key = str(fix.get("fix_key") or "")
        positions[index] = (float(fix.get("lon") or 0), float(fix.get("lat") or 0))
        time_ms[index] = int(fix.get("time_ms") or 0)
        individual_codes[index] = individual_codes_by_name[individual]
        set_codes[index] = set_codes_by_name[set_name]
        review_status[index] = _review_code(fix)
        fix_keys.append(fix_key)
        if "#row:" in fix_key:
            try:
                source_rows[index] = int(fix_key.rsplit("#row:", 1)[1])
            except ValueError:
                source_rows[index] = 0
        burst_codes[index] = burst_codes_by_id.get(burst_by_fix.get(fix_key, ""), 0)
        mask = 0
        for flag in fix.get("source_flags") or []:
            code = source_flag_codes.get(str(flag))
            if code is not None:
                mask |= 1 << code
        source_flags[index] = mask

    key_offsets, key_bytes = _pack_strings(fix_keys)
    attribute_arrays, color_columns = _attribute_columns(
        fixes,
        list(payload.get("color_fields") or []),
    )
    color_stats: dict[str, dict[str, float]] = {}
    eligible = review_status != 2
    for field, column in color_columns.items():
        if column.get("kind") != "numeric":
            continue
        values = attribute_arrays[str(column["array"])]
        finite = values[np.isfinite(values) & eligible]
        if len(finite):
            color_stats[field] = {
                "observed_min": float(np.min(finite)),
                "observed_max": float(np.max(finite)),
                "q01": float(np.quantile(finite, 0.01)),
                "q99": float(np.quantile(finite, 0.99)),
            }

    line_sources = np.empty(count, dtype=np.uint32)
    line_targets = np.empty(count, dtype=np.uint32)
    line_count = 0
    previous_by_track: dict[tuple[int, int], int] = {}
    for point_index in range(count):
        if review_status[point_index] == 2:
            continue
        track = (int(individual_codes[point_index]), int(set_codes[point_index]))
        previous = previous_by_track.get(track)
        if previous is not None:
            line_sources[line_count] = previous
            line_targets[line_count] = point_index
            line_count += 1
        previous_by_track[track] = point_index
    line_sources = line_sources[:line_count]
    line_targets = line_targets[:line_count]

    point_ranges: dict[str, list[int]] = {}
    line_ranges: dict[str, list[int]] = {}
    for individual, code in individual_codes_by_name.items():
        indexes = np.flatnonzero(individual_codes == code)
        if len(indexes):
            point_ranges[individual] = [int(indexes[0]), int(indexes[-1]) + 1]
        matching_lines = np.flatnonzero(individual_codes[line_targets] == code)
        if len(matching_lines):
            line_ranges[individual] = [int(matching_lines[0]), int(matching_lines[-1]) + 1]

    arrays = {
        "positions": positions,
        "time_ms": time_ms,
        "individual_codes": individual_codes,
        "set_codes": set_codes,
        "artifact_codes": artifact_codes,
        "source_rows": source_rows,
        "fix_key_offsets": key_offsets,
        "fix_key_bytes": key_bytes,
        "burst_values": burst_codes,
        "source_flags": source_flags,
        "review_status": review_status,
        "line_source_indexes": line_sources,
        "line_target_indexes": line_targets,
        **attribute_arrays,
    }
    loaded_individuals = [
        individual for individual in individuals if individual in point_ranges
    ]
    return _pack_binary_columns(
        arrays,
        {
            "source_format": "csv",
            "row_count": count,
            "line_count": line_count,
            "source_signature": source_signature,
            "artifacts": [logical_name],
            "individuals": individuals,
            "loaded_individuals": loaded_individuals,
            "sets": sets,
            "individual_point_ranges": point_ranges,
            "individual_line_ranges": line_ranges,
            "burst_ids": burst_ids,
            "source_flag_values": source_flag_values[:32],
            "color_columns": color_columns,
            "color_stats": color_stats,
            "truncated": bool(payload.get("truncated")),
            "matching_fix_count": int(payload.get("matching_fix_count") or count),
        },
    )
