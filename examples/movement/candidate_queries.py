import hashlib
import csv
import heapq
import json
import math
from datetime import datetime, timezone
from functools import lru_cache
from math import isfinite
from pathlib import Path

from app.osm import EARTH_RADIUS_M, OSMFetchError, OSMValidationError, fetch_osm_features

from .candidate_segments import build_candidate_segments, normalize_segment_grouping_config
from .osm_context import (
    nearest_osm_feature as _nearest_osm_feature,
    project_lon_lat as _project_lon_lat,
)
from .summary import _make_fix_key, build_movement_fixes, detect_columns, parse_time_ms, try_float


SUPPORTED_OPS = {">", ">=", "<", "<=", "==", "!="}
SUPPORTED_EVALUATORS = {"fix_numeric_comparison", "fix_osm_proximity", "fix_string_comparison"}
EVALUATOR_IMPLEMENTATION_VERSION = "movement-candidate-query-v3"
PROVENANCE_NOTE = (
    "Evaluator implementation version and source digest are recorded for provenance; "
    "exact rerun reproducibility across future code changes is out of scope for this analysis."
)
FIELD_DISPLAY_METADATA = {
    "speed_mps": {
        "field_label": "Speed",
        "unit": "m/s",
        "display_unit": "km/h",
        "display_scale": 3.6,
    },
    "step_length_m": {
        "field_label": "Step length",
        "unit": "m",
        "display_unit": "m",
        "display_scale": 1.0,
    },
    "time_delta_s": {
        "field_label": "Time delta",
        "unit": "s",
        "display_unit": "s",
        "display_scale": 1.0,
    },
    "osm:nearest_road_distance_m": {
        "field_label": "Nearest road distance",
        "unit": "m",
        "display_unit": "m",
        "display_scale": 1.0,
    },
    "osm:nearest_railway_distance_m": {
        "field_label": "Nearest railway distance",
        "unit": "m",
        "display_unit": "m",
        "display_scale": 1.0,
    },
}
DEFAULT_OSM_TILE_SIZE_M = 4_000.0
DEFAULT_MAX_OSM_SUBSCOPES = 80


class CandidateQueryError(ValueError):
    pass


def query_digest(query_definition: dict) -> str:
    serialized = json.dumps(query_definition or {}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def run_digest(
    query_definition: dict,
    parameters: dict | None,
    *,
    evaluator_type: str,
    dataset_id: str,
    logical_name: str,
    resolved_fields: list[str] | None = None,
    execution_scope: dict | None = None,
) -> str:
    payload = {
        "query": query_definition or {},
        "evaluator_type": evaluator_type,
        "parameters": parameters or {},
        "resolved_fields": sorted(str(item) for item in (resolved_fields or [])),
        "input_dataset_id": dataset_id,
        "logical_name": logical_name,
        "execution_scope": execution_scope or {},
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def evaluator_provenance() -> dict:
    return {
        "evaluator_type": "movement_candidate_query",
        "implementation_version": EVALUATOR_IMPLEMENTATION_VERSION,
        "source_digest": _source_digest(),
    }


def run_candidate_query(
    artifact_path: Path,
    *,
    query_definition: dict,
    parameters: dict | None = None,
    dataset_id: str = "",
    logical_name: str = "",
    preview_limit: int | None = None,
    execution_scope: object = None,
    confirmed_fix_keys: set[str] | list[str] | tuple[str, ...] | None = None,
    confirmed_individual_tracks: set[tuple[str, str]] | list[tuple[str, str]] | tuple[tuple[str, str], ...] | None = None,
) -> dict:
    snapshot = dict(query_definition or {})
    evaluator = dict(snapshot.get("evaluator") or {})
    evaluator_type = str(evaluator.get("type") or "").strip()
    if evaluator_type == "fix_numeric_comparison":
        return run_fix_numeric_candidate_query(
            artifact_path,
            query_definition=snapshot,
            parameters=parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            preview_limit=preview_limit,
            execution_scope=execution_scope,
            confirmed_fix_keys=confirmed_fix_keys,
            confirmed_individual_tracks=confirmed_individual_tracks,
        )
    if evaluator_type == "fix_string_comparison":
        return run_fix_string_candidate_query(
            artifact_path,
            query_definition=snapshot,
            parameters=parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            preview_limit=preview_limit,
            execution_scope=execution_scope,
            confirmed_fix_keys=confirmed_fix_keys,
            confirmed_individual_tracks=confirmed_individual_tracks,
        )
    if evaluator_type == "fix_osm_proximity":
        return run_fix_osm_proximity_candidate_query(
            artifact_path,
            query_definition=snapshot,
            parameters=parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            preview_limit=preview_limit,
            execution_scope=execution_scope,
            confirmed_fix_keys=confirmed_fix_keys,
            confirmed_individual_tracks=confirmed_individual_tracks,
        )
    raise CandidateQueryError("Unsupported candidate query evaluator")


def run_fix_numeric_candidate_query(
    artifact_path: Path,
    *,
    query_definition: dict,
    parameters: dict | None = None,
    dataset_id: str = "",
    logical_name: str = "",
    preview_limit: int | None = None,
    execution_scope: object = None,
    confirmed_fix_keys: set[str] | list[str] | tuple[str, ...] | None = None,
    confirmed_individual_tracks: set[tuple[str, str]] | list[tuple[str, str]] | tuple[tuple[str, str], ...] | None = None,
) -> dict:
    parameters = dict(parameters or {})
    snapshot = dict(query_definition or {})
    digest = query_digest(snapshot)
    evaluator = dict(snapshot.get("evaluator") or {})
    evaluator_type = str(evaluator.get("type") or "").strip()
    definition = dict(snapshot.get("definition") or {})
    if evaluator_type != "fix_numeric_comparison":
        raise CandidateQueryError("Unsupported candidate query evaluator")

    field = str(definition.get("field") or evaluator.get("field") or "").strip()
    op = str(definition.get("op") or evaluator.get("op") or "").strip()
    if not field:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            unresolved_fields=["field"],
            warnings=["Candidate query is missing a numeric field."],
        )
    if op not in SUPPORTED_OPS:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            unresolved_fields=[field],
            warnings=[f"Unsupported numeric comparison operator: {op or 'missing'}"],
        )

    raw_value = definition.get("value", evaluator.get("value"))
    if raw_value is None:
        raw_value = _parameter_value(definition.get("parameter", evaluator.get("parameter")), parameters)
    elif isinstance(raw_value, str) and raw_value.startswith("$"):
        raw_value = _parameter_value(raw_value[1:], parameters)
    elif isinstance(raw_value, dict) and "parameter" in raw_value:
        raw_value = _parameter_value(raw_value.get("parameter"), parameters)
    threshold = _finite_number(raw_value)
    if threshold is None:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            unresolved_fields=[field],
            warnings=[f"Numeric comparison value for {field} is missing or invalid."],
        )
    if field.startswith("osm:"):
        return _run_raw_csv_attribute_candidate_query(
            artifact_path,
            query_definition=snapshot,
            parameters=parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            preview_limit=preview_limit,
            execution_scope=execution_scope,
            field=field,
            op=op,
            expected_value=threshold,
            value_kind="numeric",
            confirmed_fix_keys=confirmed_fix_keys,
            confirmed_individual_tracks=confirmed_individual_tracks,
        )

    preview_limit_value = _normalize_preview_limit(preview_limit)
    payload = build_movement_fixes(
        artifact_path,
        limit=None,
        confirmed_fix_keys=confirmed_fix_keys,
        confirmed_individual_tracks=confirmed_individual_tracks,
    )
    fixes = payload.get("fixes") if isinstance(payload.get("fixes"), list) else []
    fixes = [fix for fix in fixes if not fix.get("analytically_excluded")]
    scope_context = _resolve_execution_groups(fixes, execution_scope)
    required_fields = _required_fields(snapshot, field)
    evaluation_digest = run_digest(
        snapshot,
        parameters,
        evaluator_type=evaluator_type,
        dataset_id=dataset_id,
        logical_name=logical_name,
        resolved_fields=required_fields,
        execution_scope=scope_context["execution_scope"],
    )
    available_fields = _available_fields(fixes)
    unresolved_fields = [item for item in required_fields if item not in available_fields]
    if unresolved_fields:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            scope_context=scope_context,
            unresolved_fields=unresolved_fields,
            warnings=[
                f"Required field is not available in normalized movement fixes: {item}"
                for item in unresolved_fields
            ],
        )

    if scope_context["unresolved_scope_results"] and not scope_context["groups"]:
        return _scoped_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            query_digest_value=digest,
            run_digest_value=evaluation_digest,
            execution_scope=scope_context["execution_scope"],
            resolved_fields=required_fields,
            scope_results=scope_context["unresolved_scope_results"],
            candidates=[],
        )

    candidates = []
    scope_results = list(scope_context["unresolved_scope_results"])
    for group in scope_context["groups"]:
        scope_candidates = []
        matching_count = 0
        for fix in group["fixes"]:
            attributes = fix.get("attributes") if isinstance(fix.get("attributes"), dict) else {}
            value = _finite_number(attributes.get(field))
            if value is None or not _compare(value, op, threshold):
                continue
            matching_count += 1
            if preview_limit_value is not None and len(candidates) >= preview_limit_value:
                continue
            candidate = _candidate_from_fix(
                fix,
                field=field,
                op=op,
                threshold=threshold,
                value=value,
                scope_id=group["scope_id"],
                run_digest_value=evaluation_digest,
                dataset_id=dataset_id,
                logical_name=logical_name,
            )
            candidates.append(candidate)
            scope_candidates.append(candidate)
        warnings = []
        if preview_limit_value is not None and matching_count > len(scope_candidates):
            warnings.append(f"Candidate preview was limited to {preview_limit_value} returned candidates.")
        scope_results.append(
            _scope_result(
                group,
                run_status="success",
                candidate_count=matching_count,
                returned_count=len(scope_candidates),
                warnings=warnings,
            )
        )

    return _scoped_result(
        snapshot,
        parameters,
        dataset_id=dataset_id,
        logical_name=logical_name,
        query_digest_value=digest,
        run_digest_value=evaluation_digest,
        execution_scope=scope_context["execution_scope"],
        resolved_fields=required_fields,
        scope_results=scope_results,
        candidates=candidates,
    )


def run_fix_string_candidate_query(
    artifact_path: Path,
    *,
    query_definition: dict,
    parameters: dict | None = None,
    dataset_id: str = "",
    logical_name: str = "",
    preview_limit: int | None = None,
    execution_scope: object = None,
    confirmed_fix_keys: set[str] | list[str] | tuple[str, ...] | None = None,
    confirmed_individual_tracks: set[tuple[str, str]] | list[tuple[str, str]] | tuple[tuple[str, str], ...] | None = None,
) -> dict:
    parameters = dict(parameters or {})
    snapshot = dict(query_definition or {})
    digest = query_digest(snapshot)
    evaluator = dict(snapshot.get("evaluator") or {})
    evaluator_type = str(evaluator.get("type") or "").strip()
    definition = dict(snapshot.get("definition") or {})
    if evaluator_type != "fix_string_comparison":
        raise CandidateQueryError("Unsupported candidate query evaluator")

    field = str(definition.get("field") or evaluator.get("field") or "").strip()
    op = str(definition.get("op") or evaluator.get("op") or "").strip()
    if not field:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            unresolved_fields=["field"],
            warnings=["Candidate query is missing a string field."],
        )
    if op not in {"==", "!="}:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            unresolved_fields=[field],
            warnings=[f"Unsupported string comparison operator: {op or 'missing'}"],
        )

    raw_value = definition.get("value", evaluator.get("value"))
    if raw_value is None:
        raw_value = _parameter_value(definition.get("parameter", evaluator.get("parameter")), parameters)
    elif isinstance(raw_value, str) and raw_value.startswith("$"):
        raw_value = _parameter_value(raw_value[1:], parameters)
    elif isinstance(raw_value, dict) and "parameter" in raw_value:
        raw_value = _parameter_value(raw_value.get("parameter"), parameters)
    expected_value = _string_value(raw_value)
    if expected_value is None:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            unresolved_fields=[field],
            warnings=[f"String comparison value for {field} is missing."],
        )
    if field.startswith("osm:"):
        return _run_raw_csv_attribute_candidate_query(
            artifact_path,
            query_definition=snapshot,
            parameters=parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            preview_limit=preview_limit,
            execution_scope=execution_scope,
            field=field,
            op=op,
            expected_value=expected_value,
            value_kind="string",
            confirmed_fix_keys=confirmed_fix_keys,
            confirmed_individual_tracks=confirmed_individual_tracks,
        )

    preview_limit_value = _normalize_preview_limit(preview_limit)
    payload = build_movement_fixes(
        artifact_path,
        limit=None,
        confirmed_fix_keys=confirmed_fix_keys,
        confirmed_individual_tracks=confirmed_individual_tracks,
    )
    fixes = payload.get("fixes") if isinstance(payload.get("fixes"), list) else []
    fixes = [fix for fix in fixes if not fix.get("analytically_excluded")]
    scope_context = _resolve_execution_groups(fixes, execution_scope)
    required_fields = _required_fields(snapshot, field)
    evaluation_digest = run_digest(
        snapshot,
        parameters,
        evaluator_type=evaluator_type,
        dataset_id=dataset_id,
        logical_name=logical_name,
        resolved_fields=required_fields,
        execution_scope=scope_context["execution_scope"],
    )
    available_fields = _available_fields(fixes)
    unresolved_fields = [item for item in required_fields if item not in available_fields]
    if unresolved_fields:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            scope_context=scope_context,
            unresolved_fields=unresolved_fields,
            warnings=[
                f"Required field is not available in normalized movement fixes: {item}"
                for item in unresolved_fields
            ],
        )

    if scope_context["unresolved_scope_results"] and not scope_context["groups"]:
        return _scoped_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            query_digest_value=digest,
            run_digest_value=evaluation_digest,
            execution_scope=scope_context["execution_scope"],
            resolved_fields=required_fields,
            scope_results=scope_context["unresolved_scope_results"],
            candidates=[],
        )

    candidates = []
    scope_results = list(scope_context["unresolved_scope_results"])
    for group in scope_context["groups"]:
        scope_candidates = []
        matching_count = 0
        for fix in group["fixes"]:
            value = _fix_field_value(fix, field)
            if value is None or not _compare_string(value, op, expected_value):
                continue
            matching_count += 1
            if preview_limit_value is not None and len(candidates) >= preview_limit_value:
                continue
            candidate = _string_candidate_from_fix(
                fix,
                field=field,
                op=op,
                expected_value=expected_value,
                value=value,
                scope_id=group["scope_id"],
                run_digest_value=evaluation_digest,
                dataset_id=dataset_id,
                logical_name=logical_name,
            )
            candidates.append(candidate)
            scope_candidates.append(candidate)
        warnings = []
        if preview_limit_value is not None and matching_count > len(scope_candidates):
            warnings.append(f"Candidate preview was limited to {preview_limit_value} returned candidates.")
        scope_results.append(
            _scope_result(
                group,
                run_status="success",
                candidate_count=matching_count,
                returned_count=len(scope_candidates),
                warnings=warnings,
            )
        )

    return _scoped_result(
        snapshot,
        parameters,
        dataset_id=dataset_id,
        logical_name=logical_name,
        query_digest_value=digest,
        run_digest_value=evaluation_digest,
        execution_scope=scope_context["execution_scope"],
        resolved_fields=required_fields,
        scope_results=scope_results,
        candidates=candidates,
    )


def _run_raw_csv_attribute_candidate_query(
    artifact_path: Path,
    *,
    query_definition: dict,
    parameters: dict,
    dataset_id: str,
    logical_name: str,
    preview_limit: int | None,
    execution_scope: object,
    field: str,
    op: str,
    expected_value: object,
    value_kind: str,
    confirmed_fix_keys: set[str] | list[str] | tuple[str, ...] | None,
    confirmed_individual_tracks: set[tuple[str, str]] | list[tuple[str, str]] | tuple[tuple[str, str], ...] | None,
) -> dict:
    snapshot = dict(query_definition or {})
    digest = query_digest(snapshot)
    evaluator_type = str((snapshot.get("evaluator") or {}).get("type") or "").strip()
    preview_limit_value = _normalize_preview_limit(preview_limit)
    scope_context = _raw_csv_execution_scope_context(artifact_path, execution_scope)
    required_fields = _required_fields(snapshot, field)
    evaluation_digest = run_digest(
        snapshot,
        parameters,
        evaluator_type=evaluator_type,
        dataset_id=dataset_id,
        logical_name=logical_name,
        resolved_fields=required_fields,
        execution_scope=scope_context["execution_scope"],
    )

    if scope_context["unresolved_scope_results"] and not scope_context["groups"]:
        return _scoped_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            query_digest_value=digest,
            run_digest_value=evaluation_digest,
            execution_scope=scope_context["execution_scope"],
            resolved_fields=required_fields,
            scope_results=scope_context["unresolved_scope_results"],
            candidates=[],
        )

    with artifact_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        columns = detect_columns(fieldnames)
        if field not in fieldnames:
            return _unresolved_result(
                snapshot,
                parameters,
                dataset_id=dataset_id,
                logical_name=logical_name,
                execution_scope=execution_scope,
                scope_context=scope_context,
                unresolved_fields=[field],
                warnings=[f"Required field is not available in normalized movement fixes: {field}"],
            )
        missing_required = [
            label
            for label, column in {
                "individual": columns.get("individual"),
                "time": columns.get("time"),
                "lon": columns.get("lon"),
                "lat": columns.get("lat"),
            }.items()
            if not column
        ]
        if missing_required:
            return _unresolved_result(
                snapshot,
                parameters,
                dataset_id=dataset_id,
                logical_name=logical_name,
                execution_scope=execution_scope,
                scope_context=scope_context,
                unresolved_fields=missing_required,
                warnings=[f"CSV is missing required column for candidate preview: {item}" for item in missing_required],
            )

        groups_by_scope_id = {group["scope_id"]: group for group in scope_context["groups"]}
        scope_counts = {group["scope_id"]: 0 for group in scope_context["groups"]}
        collector = _CandidatePreviewCollector(limit=preview_limit_value, op=op, value_kind=value_kind)
        track_fixes = []
        matched_fix_keys = set()
        evidence_by_fix_key = {}
        confirmed_fix_key_set = {str(item) for item in (confirmed_fix_keys or [])}
        confirmed_individual_track_set = {
            (str(item[0]), str(item[1]))
            for item in (confirmed_individual_tracks or [])
            if isinstance(item, (list, tuple)) and len(item) == 2
        }
        for row_index, raw in enumerate(reader, start=1):
            row = _raw_csv_candidate_row(raw, columns, row_index)
            if row is None:
                continue
            status_value = str(raw.get("outlier_status") or "").strip().lower()
            if (
                status_value == "confirmed"
                or row["fix_key"] in confirmed_fix_key_set
                or (row["individual"], "") in confirmed_individual_track_set
                or (row["individual"], row.get("set_name", "train")) in confirmed_individual_track_set
            ):
                continue
            group = _raw_csv_group_for_row(row, scope_context, groups_by_scope_id)
            if group is None:
                continue
            track_fixes.append(row)
            if value_kind == "numeric":
                value = _finite_number(raw.get(field))
                if value is None or not _compare(value, op, float(expected_value)):
                    continue
                candidate = _candidate_from_fix(
                    row,
                    field=field,
                    op=op,
                    threshold=float(expected_value),
                    value=value,
                    scope_id=group["scope_id"],
                    run_digest_value=evaluation_digest,
                    dataset_id=dataset_id,
                    logical_name=logical_name,
                )
            else:
                value = _string_value(raw.get(field))
                if value is None or not _compare_string(value, op, str(expected_value)):
                    continue
                candidate = _string_candidate_from_fix(
                    row,
                    field=field,
                    op=op,
                    expected_value=str(expected_value),
                    value=value,
                    scope_id=group["scope_id"],
                    run_digest_value=evaluation_digest,
                    dataset_id=dataset_id,
                    logical_name=logical_name,
                )
            scope_counts[group["scope_id"]] += 1
            matched_fix_keys.add(str(row.get("fix_key") or ""))
            evidence_by_fix_key[str(row.get("fix_key") or "")] = dict(candidate.get("evidence") or {})
            collector.add(candidate)

    candidates = collector.candidates()
    returned_by_scope = {}
    for candidate in candidates:
        scope_id = str(candidate.get("scope_id") or "")
        returned_by_scope[scope_id] = returned_by_scope.get(scope_id, 0) + 1
    scope_results = list(scope_context["unresolved_scope_results"])
    for group in scope_context["groups"]:
        matching_count = scope_counts.get(group["scope_id"], 0)
        returned_count = returned_by_scope.get(group["scope_id"], 0)
        warnings = []
        if preview_limit_value is not None and matching_count > returned_count:
            warnings.append(
                f"Candidate preview was limited to {preview_limit_value} returned candidates. "
                "Narrow the query scope or threshold for exhaustive review."
            )
        scope_results.append(
            _scope_result(
                group,
                run_status="success",
                candidate_count=matching_count,
                returned_count=returned_count,
                warnings=warnings,
            )
        )

    result = _scoped_result(
        snapshot,
        parameters,
        dataset_id=dataset_id,
        logical_name=logical_name,
        query_digest_value=digest,
        run_digest_value=evaluation_digest,
        execution_scope=scope_context["execution_scope"],
        resolved_fields=required_fields,
        scope_results=scope_results,
        candidates=candidates,
    )
    return _attach_segment_grouping(
        result,
        query_definition=snapshot,
        query_digest_value=digest,
        run_digest_value=evaluation_digest,
        track_fixes=track_fixes,
        matched_fix_keys=matched_fix_keys,
        evidence_by_fix_key=evidence_by_fix_key,
    )


def run_fix_osm_proximity_candidate_query(
    artifact_path: Path,
    *,
    query_definition: dict,
    parameters: dict | None = None,
    dataset_id: str = "",
    logical_name: str = "",
    preview_limit: int | None = None,
    execution_scope: object = None,
    confirmed_fix_keys: set[str] | list[str] | tuple[str, ...] | None = None,
    confirmed_individual_tracks: set[tuple[str, str]] | list[tuple[str, str]] | tuple[tuple[str, str], ...] | None = None,
) -> dict:
    parameters = dict(parameters or {})
    snapshot = dict(query_definition or {})
    digest = query_digest(snapshot)
    evaluator = dict(snapshot.get("evaluator") or {})
    evaluator_type = str(evaluator.get("type") or "").strip()
    definition = dict(snapshot.get("definition") or {})
    if evaluator_type != "fix_osm_proximity":
        raise CandidateQueryError("Unsupported candidate query evaluator")

    distance_m = _resolve_osm_distance_m(definition, evaluator, parameters)
    if distance_m is None:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            unresolved_fields=["distance_m"],
            warnings=["OSM proximity distance_m is missing or invalid."],
        )
    osm_definition = definition.get("osm")
    if not isinstance(osm_definition, dict):
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            unresolved_fields=["osm"],
            warnings=["OSM proximity query is missing an osm definition."],
        )
    selectors = osm_definition.get("selectors")
    element_types = osm_definition.get("element_types")
    if not isinstance(selectors, list) or not selectors:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            unresolved_fields=["osm.selectors"],
            warnings=["OSM proximity query requires at least one selector."],
        )
    if not isinstance(element_types, list) or not element_types:
        return _unresolved_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            execution_scope=execution_scope,
            unresolved_fields=["osm.element_types"],
            warnings=["OSM proximity query requires explicit element_types."],
        )

    preview_limit_value = _normalize_preview_limit(preview_limit)
    payload = build_movement_fixes(
        artifact_path,
        limit=None,
        confirmed_fix_keys=confirmed_fix_keys,
        confirmed_individual_tracks=confirmed_individual_tracks,
    )
    fixes = payload.get("fixes") if isinstance(payload.get("fixes"), list) else []
    fixes = [fix for fix in fixes if not fix.get("analytically_excluded")]
    scope_context = _resolve_execution_groups(fixes, execution_scope)
    resolved_fields = ["lat", "lon"]
    evaluation_digest = run_digest(
        snapshot,
        parameters,
        evaluator_type=evaluator_type,
        dataset_id=dataset_id,
        logical_name=logical_name,
        resolved_fields=resolved_fields,
        execution_scope=scope_context["execution_scope"],
    )

    if scope_context["unresolved_scope_results"] and not scope_context["groups"]:
        return _scoped_result(
            snapshot,
            parameters,
            dataset_id=dataset_id,
            logical_name=logical_name,
            query_digest_value=digest,
            run_digest_value=evaluation_digest,
            execution_scope=scope_context["execution_scope"],
            resolved_fields=resolved_fields,
            scope_results=scope_context["unresolved_scope_results"],
            candidates=[],
        )

    candidates = []
    scope_results = list(scope_context["unresolved_scope_results"])
    allow_whole_study = bool(definition.get("allow_whole_study_osm") or evaluator.get("allow_whole_study_osm"))
    for group in scope_context["groups"]:
        if group["scope_type"] == "whole_study" and not allow_whole_study:
            scope_results.append(
                _scope_result(
                    group,
                    run_status="unresolved",
                    candidate_count=0,
                    returned_count=0,
                    warnings=[
                        "Whole-study OSM candidate queries require allow_whole_study_osm: true. "
                        "Choose Current individual or All individuals separately for road-proximity previews."
                    ],
                )
            )
            continue

        osm_attempts = []
        try:
            feature_collections, osm_attempts = _fetch_osm_collections_for_group(
                group,
                distance_m=distance_m,
                selectors=selectors,
                element_types=element_types,
                osm_definition=osm_definition,
            )
        except (OSMValidationError, OSMFetchError, ValueError) as exc:
            warning = f"OSM scope could not run for {group['scope_id']}: {exc}"
            failed_attempts = getattr(exc, "osm_attempts", osm_attempts)
            scope_results.append(
                _scope_result(
                    group,
                    run_status="unresolved",
                    candidate_count=0,
                    returned_count=0,
                    warnings=[warning],
                    osm=_aggregate_osm_metadata(
                        [],
                        attempts=failed_attempts,
                        distance_m=distance_m,
                        selectors=selectors,
                        element_types=element_types,
                    )
                    if failed_attempts
                    else None,
                )
            )
            continue

        features = _merged_osm_features(feature_collections)
        osm_metadata = _aggregate_osm_metadata(
            feature_collections,
            attempts=osm_attempts,
            distance_m=distance_m,
            selectors=selectors,
            element_types=element_types,
        )
        scope_candidates = []
        matching_count = 0
        for fix in group["fixes"]:
            lon = _finite_number(fix.get("lon"))
            lat = _finite_number(fix.get("lat"))
            if lon is None or lat is None:
                continue
            nearest = _nearest_osm_feature(lon, lat, features)
            if nearest is None or nearest["distance_m"] > distance_m:
                continue
            matching_count += 1
            if preview_limit_value is not None and len(candidates) >= preview_limit_value:
                continue
            candidate = _osm_candidate_from_fix(
                fix,
                nearest=nearest,
                threshold_m=distance_m,
                selectors=selectors,
                element_types=element_types,
                scope_id=group["scope_id"],
                run_digest_value=evaluation_digest,
                dataset_id=dataset_id,
                logical_name=logical_name,
            )
            candidates.append(candidate)
            scope_candidates.append(candidate)

        warnings = _unique_strings(attempt.get("warning") for attempt in osm_attempts)
        if preview_limit_value is not None and matching_count > len(scope_candidates):
            warnings.append(f"Candidate preview was limited to {preview_limit_value} returned candidates.")
        scope_results.append(
            _scope_result(
                group,
                run_status="partial" if any(attempt.get("run_status") != "success" for attempt in osm_attempts) else "success",
                candidate_count=matching_count,
                returned_count=len(scope_candidates),
                warnings=warnings,
                osm=osm_metadata,
            )
        )

    return _scoped_result(
        snapshot,
        parameters,
        dataset_id=dataset_id,
        logical_name=logical_name,
        query_digest_value=digest,
        run_digest_value=evaluation_digest,
        execution_scope=scope_context["execution_scope"],
        resolved_fields=resolved_fields,
        scope_results=scope_results,
        candidates=candidates,
    )


def unresolved_candidate_query_result(
    query_definition: dict,
    parameters: dict | None = None,
    *,
    dataset_id: str = "",
    logical_name: str = "",
    execution_scope: object = None,
    unresolved_fields: list[str] | None = None,
    warnings: list[str] | None = None,
) -> dict:
    return _unresolved_result(
        query_definition,
        dict(parameters or {}),
        dataset_id=dataset_id,
        logical_name=logical_name,
        execution_scope=execution_scope,
        unresolved_fields=list(unresolved_fields or []),
        warnings=list(warnings or []),
    )


def _unresolved_result(
    query_definition: dict,
    parameters: dict,
    *,
    dataset_id: str,
    logical_name: str,
    execution_scope: object = None,
    scope_context: dict | None = None,
    unresolved_fields: list[str],
    warnings: list[str],
) -> dict:
    context = scope_context or _default_execution_scope_context(execution_scope)
    scope_results = list(context["unresolved_scope_results"])
    if not scope_results:
        for group in context["groups"]:
            scope_results.append(
                _scope_result(
                    group,
                    run_status="unresolved",
                    candidate_count=0,
                    returned_count=0,
                    unresolved_fields=unresolved_fields,
                    warnings=warnings,
                )
            )
    if not scope_results:
        scope_results.append(
            {
                "scope_id": "unresolved",
                "scope_type": "unresolved",
                "individual": "",
                "run_status": "unresolved",
                "candidate_count": 0,
                "returned_count": 0,
                "unresolved_fields": list(unresolved_fields),
                "warnings": list(warnings),
            }
        )
    else:
        scope_results = [
            {
                **scope_result,
                "unresolved_fields": _unique_strings(
                    [*(scope_result.get("unresolved_fields") or []), *unresolved_fields]
                ),
                "warnings": _unique_strings([*(scope_result.get("warnings") or []), *warnings]),
            }
            for scope_result in scope_results
        ]
    return _scoped_result(
        query_definition,
        parameters,
        dataset_id=dataset_id,
        logical_name=logical_name,
        query_digest_value=query_digest(query_definition or {}),
        run_digest_value=run_digest(
            query_definition or {},
            parameters,
            evaluator_type=str((query_definition or {}).get("evaluator", {}).get("type") or ""),
            dataset_id=dataset_id,
            logical_name=logical_name,
            resolved_fields=[],
            execution_scope=context["execution_scope"],
        ),
        execution_scope=context["execution_scope"],
        resolved_fields=[],
        scope_results=scope_results,
        candidates=[],
    )


def _scoped_result(
    query_definition: dict,
    parameters: dict,
    *,
    dataset_id: str,
    logical_name: str,
    query_digest_value: str,
    run_digest_value: str,
    execution_scope: dict,
    resolved_fields: list[str],
    scope_results: list[dict],
    candidates: list[dict],
) -> dict:
    warnings = _unique_strings(
        warning
        for scope_result in scope_results
        for warning in scope_result.get("warnings", [])
    )
    unresolved_fields = _unique_strings(
        field
        for scope_result in scope_results
        for field in scope_result.get("unresolved_fields", [])
    )
    return {
        "run_status": _aggregate_run_status(scope_results),
        "query": dict(query_definition or {}),
        "query_digest": query_digest_value,
        "run_digest": run_digest_value,
        "evaluator_provenance": evaluator_provenance(),
        "provenance_note": PROVENANCE_NOTE,
        "parameters": dict(parameters or {}),
        "input_dataset_id": dataset_id,
        "logical_name": logical_name,
        "target_artifact": logical_name,
        "execution_scope": execution_scope,
        "scope_results": scope_results,
        "resolved_fields": resolved_fields if not unresolved_fields else [],
        "unresolved_fields": unresolved_fields,
        "candidate_count": int(sum(_safe_int(item.get("candidate_count")) for item in scope_results)),
        "returned_count": int(len(candidates)),
        "candidates": candidates,
        "warnings": warnings,
    }


def _attach_segment_grouping(
    result: dict,
    *,
    query_definition: dict,
    query_digest_value: str,
    run_digest_value: str,
    track_fixes: list[dict],
    matched_fix_keys: set[str],
    evidence_by_fix_key: dict[str, dict],
) -> dict:
    config = normalize_segment_grouping_config((query_definition or {}).get("segment_grouping"))
    if not config["enabled"]:
        return result
    segment_result = build_candidate_segments(
        query_definition=query_definition,
        query_digest_value=query_digest_value,
        run_digest_value=run_digest_value,
        track_fixes=track_fixes,
        matched_fix_keys=matched_fix_keys,
        evidence_by_fix_key=evidence_by_fix_key,
        config=config,
    )
    result["segment_count"] = segment_result["segment_count"]
    result["returned_segment_count"] = segment_result["returned_segment_count"]
    result["candidate_segments"] = segment_result["candidate_segments"]
    result["segment_grouping"] = segment_result["segment_grouping"]
    result["warnings"] = _unique_strings([*result.get("warnings", []), *segment_result.get("warnings", [])])
    return result


def _aggregate_run_status(scope_results: list[dict]) -> str:
    has_runnable = any(item.get("run_status") in {"success", "partial"} for item in scope_results)
    has_unresolved = any(item.get("run_status") in {"unresolved", "partial"} for item in scope_results)
    if has_runnable and has_unresolved:
        return "partial"
    if has_runnable:
        return "success"
    return "unresolved"


def _safe_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _unique_strings(values) -> list[str]:
    seen = set()
    result = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        result.append(text)
        seen.add(text)
    return result


def _default_execution_scope_context(execution_scope: object) -> dict:
    requested = _requested_execution_scope(execution_scope)
    scope_type = str(requested.get("type") or "").strip()
    if scope_type == "current_individual":
        scope_type = "individual"
    unresolved_scope_results = []
    if scope_type == "individual" and not str(requested.get("individual") or "").strip():
        unresolved_scope_results.append(
            _unresolved_scope_result(
                scope_id="individual:",
                scope_type="individual",
                individual="",
                warnings=["Individual execution scope is missing an individual id."],
            )
        )
    elif scope_type not in {"whole_study", "individual", "all_individuals_per_individual"}:
        unresolved_scope_results.append(
            _unresolved_scope_result(
                scope_id="unresolved",
                scope_type="unresolved",
                individual="",
                warnings=[_invalid_execution_scope_warning(scope_type)],
            )
        )
        scope_type = "unresolved"
    return {
        "execution_scope": {
            "requested": requested,
            "resolved": {
                "type": scope_type,
                "scope_count": 0,
                "scope_ids": [],
                "individuals": [],
            },
        },
        "groups": [],
        "unresolved_scope_results": unresolved_scope_results,
    }


def _requested_execution_scope(raw_scope: object) -> dict:
    if raw_scope is None:
        return {"type": "whole_study"}
    if isinstance(raw_scope, dict):
        requested = dict(raw_scope)
        requested["type"] = str(requested.get("type") or "").strip()
        if "individual" in requested:
            requested["individual"] = str(requested.get("individual") or "").strip()
        return requested
    return {"type": str(raw_scope or "").strip()}


def _resolve_execution_groups(fixes: list[dict], raw_scope: object) -> dict:
    requested = _requested_execution_scope(raw_scope)
    requested_type = str(requested.get("type") or "").strip()
    scope_type = "individual" if requested_type == "current_individual" else requested_type
    groups = []
    unresolved_scope_results = []
    fixes_by_individual = _fixes_by_individual(fixes)
    individuals = sorted(fixes_by_individual)

    if scope_type == "whole_study":
        groups.append(
            {
                "scope_id": "whole_study",
                "scope_type": "whole_study",
                "individual": "",
                "fixes": fixes,
            }
        )
    elif scope_type == "individual":
        individual = str(requested.get("individual") or "").strip()
        scope_id = f"individual:{individual}" if individual else "individual:"
        if not individual:
            unresolved_scope_results.append(
                _unresolved_scope_result(
                    scope_id=scope_id,
                    scope_type="individual",
                    individual=individual,
                    warnings=["Individual execution scope is missing an individual id."],
                )
            )
        elif individual not in fixes_by_individual:
            unresolved_scope_results.append(
                _unresolved_scope_result(
                    scope_id=scope_id,
                    scope_type="individual",
                    individual=individual,
                    warnings=[f"Unknown individual for execution scope: {individual}"],
                )
            )
        else:
            groups.append(
                {
                    "scope_id": scope_id,
                    "scope_type": "individual",
                    "individual": individual,
                    "fixes": fixes_by_individual[individual],
                }
            )
    elif scope_type == "all_individuals_per_individual":
        for individual in individuals:
            groups.append(
                {
                    "scope_id": f"individual:{individual}",
                    "scope_type": "individual",
                    "individual": individual,
                    "fixes": fixes_by_individual[individual],
                }
            )
    else:
        unresolved_scope_results.append(
            _unresolved_scope_result(
                scope_id="unresolved",
                scope_type="unresolved",
                individual="",
                warnings=[_invalid_execution_scope_warning(requested_type)],
            )
        )
        return {
            "execution_scope": {
                "requested": requested,
                "resolved": {
                    "type": "unresolved",
                    "scope_count": 0,
                    "scope_ids": [],
                    "individuals": [],
                },
            },
            "groups": groups,
            "unresolved_scope_results": unresolved_scope_results,
        }

    resolved = {
        "type": scope_type,
        "scope_count": len(groups),
        "scope_ids": [group["scope_id"] for group in groups],
        "individuals": [group["individual"] for group in groups if group["individual"]],
    }
    if scope_type == "individual":
        resolved["individual"] = str(requested.get("individual") or "").strip()
    return {
        "execution_scope": {
            "requested": requested,
            "resolved": resolved,
        },
        "groups": groups,
        "unresolved_scope_results": unresolved_scope_results,
    }


def _fixes_by_individual(fixes: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for fix in fixes:
        individual = str(fix.get("individual") or "").strip()
        if not individual:
            continue
        grouped.setdefault(individual, []).append(fix)
    return grouped


def _raw_csv_execution_scope_context(artifact_path: Path, raw_scope: object) -> dict:
    requested = _requested_execution_scope(raw_scope)
    requested_type = str(requested.get("type") or "").strip()
    scope_type = "individual" if requested_type == "current_individual" else requested_type
    groups = []
    unresolved_scope_results = []
    individuals = _raw_csv_individuals(artifact_path)

    if scope_type == "whole_study":
        groups.append(
            {
                "scope_id": "whole_study",
                "scope_type": "whole_study",
                "individual": "",
            }
        )
    elif scope_type == "individual":
        individual = str(requested.get("individual") or "").strip()
        scope_id = f"individual:{individual}" if individual else "individual:"
        if not individual:
            unresolved_scope_results.append(
                _unresolved_scope_result(
                    scope_id=scope_id,
                    scope_type="individual",
                    individual=individual,
                    warnings=["Individual execution scope is missing an individual id."],
                )
            )
        elif individual not in individuals:
            unresolved_scope_results.append(
                _unresolved_scope_result(
                    scope_id=scope_id,
                    scope_type="individual",
                    individual=individual,
                    warnings=[f"Unknown individual for execution scope: {individual}"],
                )
            )
        else:
            groups.append(
                {
                    "scope_id": scope_id,
                    "scope_type": "individual",
                    "individual": individual,
                }
            )
    elif scope_type == "all_individuals_per_individual":
        for individual in sorted(individuals):
            groups.append(
                {
                    "scope_id": f"individual:{individual}",
                    "scope_type": "individual",
                    "individual": individual,
                }
            )
    else:
        unresolved_scope_results.append(
            _unresolved_scope_result(
                scope_id="unresolved",
                scope_type="unresolved",
                individual="",
                warnings=[_invalid_execution_scope_warning(requested_type)],
            )
        )
        scope_type = "unresolved"

    resolved = {
        "type": scope_type,
        "scope_count": len(groups),
        "scope_ids": [group["scope_id"] for group in groups],
        "individuals": [group["individual"] for group in groups if group["individual"]],
    }
    if scope_type == "individual":
        resolved["individual"] = str(requested.get("individual") or "").strip()
    return {
        "execution_scope": {
            "requested": requested,
            "resolved": resolved,
        },
        "groups": groups,
        "unresolved_scope_results": unresolved_scope_results,
    }


def _raw_csv_individuals(artifact_path: Path) -> set[str]:
    individuals = set()
    with artifact_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = detect_columns(list(reader.fieldnames or []))
        individual_column = columns.get("individual")
        if not individual_column:
            return individuals
        for raw in reader:
            individual = str(raw.get(individual_column, "")).strip()
            if individual:
                individuals.add(individual)
    return individuals


def _raw_csv_group_for_row(row: dict, scope_context: dict, groups_by_scope_id: dict[str, dict]) -> dict | None:
    resolved_type = str(scope_context.get("execution_scope", {}).get("resolved", {}).get("type") or "")
    if resolved_type == "whole_study":
        return groups_by_scope_id.get("whole_study")
    if resolved_type in {"individual", "all_individuals_per_individual"}:
        return groups_by_scope_id.get(f"individual:{row.get('individual')}")
    return None


def _invalid_execution_scope_warning(scope_type: str) -> str:
    return (
        "Execution scope type is missing."
        if not str(scope_type or "").strip()
        else f"Unsupported execution scope type: {scope_type}"
    )


def _scope_result(
    group: dict,
    *,
    run_status: str,
    candidate_count: int,
    returned_count: int,
    unresolved_fields: list[str] | None = None,
    warnings: list[str] | None = None,
    osm: dict | None = None,
) -> dict:
    result = {
        "scope_id": group["scope_id"],
        "scope_type": group["scope_type"],
        "individual": group["individual"],
        "run_status": run_status,
        "candidate_count": int(candidate_count),
        "returned_count": int(returned_count),
        "unresolved_fields": list(unresolved_fields or []),
        "warnings": list(warnings or []),
    }
    if osm is not None:
        result["osm"] = osm
    return result


def _unresolved_scope_result(*, scope_id: str, scope_type: str, individual: str, warnings: list[str]) -> dict:
    return {
        "scope_id": scope_id,
        "scope_type": scope_type,
        "individual": individual,
        "run_status": "unresolved",
        "candidate_count": 0,
        "returned_count": 0,
        "unresolved_fields": [],
        "warnings": warnings,
    }


def _parameter_value(raw_name: object, parameters: dict) -> object:
    name = str(raw_name or "").strip()
    if not name:
        return None
    return parameters.get(name)


def _finite_number(raw_value: object) -> float | None:
    if isinstance(raw_value, bool):
        return None
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    return value if isfinite(value) else None


def _string_value(raw_value: object) -> str | None:
    if raw_value is None:
        return None
    return str(raw_value)


def _fix_field_value(fix: dict, field: str) -> str | None:
    attributes = fix.get("attributes") if isinstance(fix.get("attributes"), dict) else {}
    if field in attributes:
        return _string_value(attributes.get(field))
    if field in fix:
        return _string_value(fix.get(field))
    return None


def _raw_csv_candidate_row(raw: dict, columns: dict[str, str | None], row_index: int) -> dict | None:
    individual_column = columns.get("individual")
    time_column = columns.get("time")
    lon_column = columns.get("lon")
    lat_column = columns.get("lat")
    if not individual_column or not time_column or not lon_column or not lat_column:
        return None
    individual = str(raw.get(individual_column, "")).strip()
    time_ms = parse_time_ms(raw.get(time_column))
    lon = try_float(raw.get(lon_column))
    lat = try_float(raw.get(lat_column))
    if not individual or time_ms is None or lon is None or lat is None:
        return None
    set_column = columns.get("set")
    set_name = str(raw.get(set_column, "")).strip().lower() if set_column else "train"
    if set_name != "test":
        set_name = "train"
    fix_id_column = columns.get("fix_id")
    fix_id = str(raw.get(fix_id_column, "")).strip() if fix_id_column else ""
    return {
        "fix_key": _make_fix_key(row_index, fix_id, individual, time_ms),
        "individual": individual,
        "set": set_name,
        "time_ms": int(time_ms),
        "row_index": int(row_index),
        "lon": float(lon),
        "lat": float(lat),
        "attributes": _raw_csv_osm_attributes(raw),
    }


def _raw_csv_osm_attributes(raw: dict) -> dict:
    attributes = {}
    for key, raw_value in raw.items():
        if not str(key or "").startswith("osm:"):
            continue
        text = str(raw_value or "").strip()
        if not text:
            continue
        numeric = _finite_number(text)
        attributes[str(key)] = numeric if numeric is not None else text
    return attributes


def _resolve_osm_distance_m(definition: dict, evaluator: dict, parameters: dict) -> float | None:
    raw_value = definition.get("distance_m", evaluator.get("distance_m"))
    if raw_value is None:
        raw_value = _parameter_value(definition.get("distance_parameter", evaluator.get("distance_parameter")), parameters)
    elif isinstance(raw_value, str) and raw_value.startswith("$"):
        raw_value = _parameter_value(raw_value[1:], parameters)
    elif isinstance(raw_value, dict) and "parameter" in raw_value:
        raw_value = _parameter_value(raw_value.get("parameter"), parameters)
    distance_m = _finite_number(raw_value)
    if distance_m is None or distance_m <= 0:
        return None
    return distance_m


def _osm_bbox_scope_for_fixes(fixes: list[dict], distance_m: float) -> dict:
    coordinates = []
    for fix in fixes:
        lon = _finite_number(fix.get("lon"))
        lat = _finite_number(fix.get("lat"))
        if lon is None or lat is None:
            continue
        coordinates.append((lon, lat))
    if not coordinates:
        raise ValueError("No valid fix coordinates are available for OSM scope.")
    west = min(lon for lon, _ in coordinates)
    east = max(lon for lon, _ in coordinates)
    south = min(lat for _, lat in coordinates)
    north = max(lat for _, lat in coordinates)
    mid_lat = max(min((south + north) / 2.0, 89.0), -89.0)
    lat_buffer = math.degrees(distance_m / EARTH_RADIUS_M)
    cos_lat = max(math.cos(math.radians(mid_lat)), 0.01)
    lon_buffer = math.degrees(distance_m / (EARTH_RADIUS_M * cos_lat))
    return {
        "type": "bbox",
        "west": max(-180.0, west - lon_buffer),
        "south": max(-90.0, south - lat_buffer),
        "east": min(180.0, east + lon_buffer),
        "north": min(90.0, north + lat_buffer),
    }


def _fetch_osm_collections_for_group(
    group: dict,
    *,
    distance_m: float,
    selectors: list[dict],
    element_types: list[str],
    osm_definition: dict,
) -> tuple[list[dict], list[dict]]:
    scopes = _osm_fetch_scopes_for_fixes(
        group["fixes"],
        distance_m,
        max_subscopes=_normalize_osm_max_subscopes(osm_definition.get("max_subscopes")),
    )
    feature_collections = []
    attempts = []
    for index, scope in enumerate(scopes, start=1):
        query = {
            "scope": scope,
            "selectors": selectors,
            "element_types": element_types,
        }
        if "max_features" in osm_definition:
            query["max_features"] = osm_definition["max_features"]
        if "timeout_s" in osm_definition:
            query["timeout_s"] = osm_definition["timeout_s"]
        attempt = {
            "scope": scope,
            "query": query,
            "run_status": "pending",
            "subscope_index": index,
            "subscope_count": len(scopes),
        }
        try:
            feature_collection = fetch_osm_features(query)
        except (OSMValidationError, OSMFetchError, ValueError) as exc:
            warning = f"OSM subscope {index}/{len(scopes)} could not run: {exc}"
            attempt["run_status"] = "unresolved"
            attempt["warning"] = warning
            attempt["error"] = str(exc)
            attempts.append(attempt)
            continue
        attempt["run_status"] = "success"
        attempt["metadata"] = feature_collection.get("metadata") if isinstance(feature_collection, dict) else {}
        attempts.append(attempt)
        feature_collections.append(feature_collection)
    if not feature_collections:
        failure_count = len([attempt for attempt in attempts if attempt.get("run_status") != "success"])
        exc = OSMFetchError(f"all {failure_count or len(scopes)} OSM subscopes failed")
        setattr(exc, "osm_attempts", list(attempts))
        raise exc
    return feature_collections, attempts


def _osm_fetch_scopes_for_fixes(fixes: list[dict], distance_m: float, *, max_subscopes: int) -> list[dict]:
    coordinates = []
    for fix in fixes:
        lon = _finite_number(fix.get("lon"))
        lat = _finite_number(fix.get("lat"))
        if lon is None or lat is None:
            continue
        coordinates.append((fix, lon, lat))
    if not coordinates:
        raise ValueError("No valid fix coordinates are available for OSM scope.")

    reference_lat = sum(lat for _, _, lat in coordinates) / len(coordinates)
    tile_size_m = _osm_tile_size_m(distance_m)
    tiles: dict[tuple[int, int], list[dict]] = {}
    for fix, lon, lat in coordinates:
        x, y = _project_lon_lat(lon, lat, reference_lat)
        key = (math.floor(x / tile_size_m), math.floor(y / tile_size_m))
        tiles.setdefault(key, []).append(fix)
    if len(tiles) > max_subscopes:
        raise ValueError(
            f"OSM scope would require {len(tiles)} spatial subscopes; "
            f"max_subscopes is {max_subscopes}."
        )
    return [
        _osm_bbox_scope_for_fixes(tile_fixes, distance_m)
        for _, tile_fixes in sorted(tiles.items(), key=lambda item: item[0])
    ]


def _osm_tile_size_m(distance_m: float) -> float:
    return max(50.0, min(DEFAULT_OSM_TILE_SIZE_M, 4_900.0 - (2.0 * distance_m)))


def _normalize_osm_max_subscopes(raw_value: object) -> int:
    if raw_value in (None, ""):
        return DEFAULT_MAX_OSM_SUBSCOPES
    if isinstance(raw_value, bool):
        return DEFAULT_MAX_OSM_SUBSCOPES
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return DEFAULT_MAX_OSM_SUBSCOPES
    return max(1, min(500, value))


def _candidate_osm_metadata(raw_metadata: object, osm_scope: dict) -> dict:
    metadata = raw_metadata if isinstance(raw_metadata, dict) else {}
    return {
        "scope": metadata.get("scope") if isinstance(metadata.get("scope"), dict) else osm_scope,
        "scope_signature": str(metadata.get("scope_signature") or ""),
        "query_signature": str(metadata.get("query_signature") or ""),
        "feature_count": _safe_int(metadata.get("feature_count")),
        "omitted_feature_count": _safe_int(metadata.get("omitted_feature_count")),
        "unsupported_relation_count": _safe_int(metadata.get("unsupported_relation_count")),
        "unsupported_element_count": _safe_int(metadata.get("unsupported_element_count")),
        "unsupported_geometry_count": _safe_int(metadata.get("unsupported_geometry_count")),
        "truncated_feature_count": _safe_int(metadata.get("truncated_feature_count")),
        "fetched_at": str(metadata.get("fetched_at") or ""),
        "warnings": list(metadata.get("warnings") or []) if isinstance(metadata.get("warnings"), list) else [],
    }


def _aggregate_osm_metadata(
    feature_collections: list[dict],
    *,
    attempts: list[dict] | None = None,
    distance_m: float,
    selectors: list[dict],
    element_types: list[str],
) -> dict:
    if attempts is not None:
        subscopes = [
            _osm_metadata_from_attempt(
                attempt,
                distance_m=distance_m,
                selectors=selectors,
                element_types=element_types,
            )
            for attempt in attempts
            if isinstance(attempt, dict)
        ]
    else:
        subscopes = [
            _candidate_osm_metadata(collection.get("metadata"), collection.get("metadata", {}).get("scope", {}))
            for collection in feature_collections
            if isinstance(collection, dict)
        ]
    if len(subscopes) == 1:
        result = dict(subscopes[0])
        result["selectors"] = selectors
        result["element_types"] = element_types
        result["distance_m"] = distance_m
        result["buffer_m"] = distance_m
        result["subscope_count"] = 1
        result["subscopes"] = subscopes
        return result
    fetched_at_values = [item.get("fetched_at") for item in subscopes if item.get("fetched_at")]
    warnings = _unique_strings(warning for item in subscopes for warning in item.get("warnings", []))
    return {
        "scope": {"type": "tiled_bbox", "subscope_count": len(subscopes)},
        "selectors": selectors,
        "element_types": element_types,
        "distance_m": distance_m,
        "buffer_m": distance_m,
        "scope_signature": _payload_signature([item.get("scope") for item in subscopes]),
        "query_signature": _payload_signature(
            {
                "scopes": [item.get("scope") for item in subscopes],
                "selectors": selectors,
                "element_types": element_types,
            }
        ),
        "feature_count": len(_merged_osm_features(feature_collections)),
        "omitted_feature_count": sum(_safe_int(item.get("omitted_feature_count")) for item in subscopes),
        "unsupported_relation_count": sum(_safe_int(item.get("unsupported_relation_count")) for item in subscopes),
        "unsupported_element_count": sum(_safe_int(item.get("unsupported_element_count")) for item in subscopes),
        "unsupported_geometry_count": sum(_safe_int(item.get("unsupported_geometry_count")) for item in subscopes),
        "truncated_feature_count": sum(_safe_int(item.get("truncated_feature_count")) for item in subscopes),
        "fetched_at": max(fetched_at_values) if fetched_at_values else "",
        "warnings": warnings,
        "subscope_count": len(subscopes),
        "subscopes": subscopes,
    }


def _osm_metadata_from_attempt(
    attempt: dict,
    *,
    distance_m: float,
    selectors: list[dict],
    element_types: list[str],
) -> dict:
    if attempt.get("run_status") == "success":
        metadata = _candidate_osm_metadata(attempt.get("metadata"), attempt.get("scope", {}))
        metadata["run_status"] = "success"
        metadata["subscope_index"] = attempt.get("subscope_index")
        metadata["subscope_count"] = attempt.get("subscope_count")
        return metadata
    warning = str(attempt.get("warning") or attempt.get("error") or "OSM subscope could not run.")
    metadata = _attempted_osm_metadata(
        osm_scope=attempt.get("scope") if isinstance(attempt.get("scope"), dict) else None,
        osm_query=attempt.get("query") if isinstance(attempt.get("query"), dict) else None,
        distance_m=distance_m,
        selectors=selectors,
        element_types=element_types,
        warning=warning,
    )
    metadata["run_status"] = "unresolved"
    metadata["subscope_index"] = attempt.get("subscope_index")
    metadata["subscope_count"] = attempt.get("subscope_count")
    return metadata


def _merged_osm_features(feature_collections: list[dict]) -> list[dict]:
    features = []
    seen = set()
    for collection in feature_collections:
        raw_features = collection.get("features") if isinstance(collection.get("features"), list) else []
        for feature in raw_features:
            if not isinstance(feature, dict):
                continue
            key = str(feature.get("id") or "")
            if not key:
                key = _payload_signature(feature)
            if key in seen:
                continue
            seen.add(key)
            features.append(feature)
    return features


def _attempted_osm_metadata(
    *,
    osm_scope: dict | None,
    osm_query: dict | None,
    distance_m: float,
    selectors: list[dict],
    element_types: list[str],
    warning: str,
) -> dict:
    query = osm_query if isinstance(osm_query, dict) else {}
    scope = query.get("scope") if isinstance(query.get("scope"), dict) else osm_scope
    attempted_selectors = query.get("selectors") if isinstance(query.get("selectors"), list) else selectors
    attempted_element_types = query.get("element_types") if isinstance(query.get("element_types"), list) else element_types
    return {
        "scope": scope,
        "selectors": attempted_selectors,
        "element_types": attempted_element_types,
        "distance_m": distance_m,
        "buffer_m": distance_m,
        "scope_signature": _payload_signature(scope) if isinstance(scope, dict) else None,
        "query_signature": _payload_signature(
            {
                "scope": scope,
                "selectors": attempted_selectors,
                "element_types": attempted_element_types,
            }
        )
        if isinstance(scope, dict)
        else None,
        "feature_count": 0,
        "omitted_feature_count": 0,
        "unsupported_relation_count": 0,
        "unsupported_element_count": 0,
        "unsupported_geometry_count": 0,
        "truncated_feature_count": 0,
        "fetched_at": None,
        "warnings": [warning],
        "error": warning,
    }


def _payload_signature(payload: object) -> str:
    serialized = json.dumps(payload or {}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


class _CandidatePreviewCollector:
    def __init__(self, *, limit: int | None, op: str, value_kind: str):
        self.limit = limit
        self.op = op
        self.value_kind = value_kind
        self._items = []
        self._sequence = 0

    def add(self, candidate: dict):
        self._sequence += 1
        if self.limit is None:
            self._items.append(candidate)
            return
        if self.limit <= 0:
            return
        if self.value_kind != "numeric" or self.op not in {"<", "<=", ">", ">="}:
            if len(self._items) < self.limit:
                self._items.append(candidate)
            return
        priority = self._heap_priority(candidate, self._sequence)
        item = (priority, self._sequence, candidate)
        if len(self._items) < self.limit:
            heapq.heappush(self._items, item)
            return
        if priority > self._items[0][0]:
            heapq.heapreplace(self._items, item)

    def candidates(self) -> list[dict]:
        if self.value_kind != "numeric" or self.op not in {"<", "<=", ">", ">="}:
            return list(self._items)
        if self.limit is None:
            return sorted(self._items, key=_candidate_output_sort_key(self.op, self.value_kind))
        return sorted(
            [item[2] for item in self._items],
            key=_candidate_output_sort_key(self.op, self.value_kind),
        )

    def _heap_priority(self, candidate: dict, sequence: int) -> float:
        value = _finite_number((candidate.get("evidence") or {}).get("value"))
        if value is None:
            value = math.inf if self.op in {"<", "<="} else -math.inf
        if self.op in {"<", "<="}:
            return -float(value)
        return float(value)


def _candidate_output_sort_key(op: str, value_kind: str):
    def key(candidate: dict):
        evidence = candidate.get("evidence") if isinstance(candidate.get("evidence"), dict) else {}
        value = _finite_number(evidence.get("value"))
        time_ms = _safe_int(candidate.get("time_ms"))
        fix_key = str(candidate.get("fix_key") or "")
        if value_kind == "numeric" and value is not None:
            if op in {"<", "<="}:
                return (value, time_ms, fix_key)
            if op in {">", ">="}:
                return (-value, time_ms, fix_key)
        return (time_ms, fix_key)

    return key


def _normalize_preview_limit(raw_value: object) -> int | None:
    if raw_value in (None, ""):
        return None
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return None
    return max(1, value)


def _required_fields(query_definition: dict, field: str) -> list[str]:
    raw_fields = query_definition.get("required_fields")
    fields = []
    if isinstance(raw_fields, list):
        fields.extend(str(item or "").strip() for item in raw_fields)
    fields.append(field)
    return sorted({item for item in fields if item})


def _available_fields(fixes: list[dict]) -> set[str]:
    fields = {"fix_key", "individual", "time_ms", "time", "lon", "lat", "set"}
    for fix in fixes:
        if isinstance(fix, dict):
            fields.update(str(key) for key in fix.keys())
            attributes = fix.get("attributes")
            if isinstance(attributes, dict):
                fields.update(str(key) for key in attributes.keys())
    return fields


def _compare(left: float, op: str, right: float) -> bool:
    if op == ">":
        return left > right
    if op == ">=":
        return left >= right
    if op == "<":
        return left < right
    if op == "<=":
        return left <= right
    if op == "==":
        return left == right
    if op == "!=":
        return left != right
    return False


def _compare_string(left: str, op: str, right: str) -> bool:
    if op == "==":
        return left == right
    if op == "!=":
        return left != right
    return False


def _candidate_from_fix(
    fix: dict,
    *,
    field: str,
    op: str,
    threshold: float,
    value: float,
    scope_id: str,
    run_digest_value: str,
    dataset_id: str,
    logical_name: str,
) -> dict:
    time_ms = fix.get("time_ms")
    fix_key = fix.get("fix_key")
    display_metadata = _evidence_display_metadata(field, value=value, threshold=threshold)
    return {
        "candidate_id": _candidate_id(
            run_digest_value=run_digest_value,
            dataset_id=dataset_id,
            logical_name=logical_name,
            scope_id=scope_id,
            fix_key=fix_key,
        ),
        "scope_id": scope_id,
        "kind": "fix",
        "fix_key": fix_key,
        "individual": fix.get("individual"),
        "set": fix.get("set", "train"),
        "time_ms": time_ms,
        "time": _format_time(time_ms),
        "lon": fix.get("lon"),
        "lat": fix.get("lat"),
        "attributes": dict(fix.get("attributes") or {}) if isinstance(fix.get("attributes"), dict) else {},
        "review": dict(fix.get("review") or {}) if isinstance(fix.get("review"), dict) else {},
        "segments": list(fix.get("segments") or []) if isinstance(fix.get("segments"), list) else [],
        "evidence": {
            "field": field,
            "op": op,
            "threshold": threshold,
            "value": value,
            **display_metadata,
        },
    }


def _string_candidate_from_fix(
    fix: dict,
    *,
    field: str,
    op: str,
    expected_value: str,
    value: str,
    scope_id: str,
    run_digest_value: str,
    dataset_id: str,
    logical_name: str,
) -> dict:
    time_ms = fix.get("time_ms")
    fix_key = fix.get("fix_key")
    return {
        "candidate_id": _candidate_id(
            run_digest_value=run_digest_value,
            dataset_id=dataset_id,
            logical_name=logical_name,
            scope_id=scope_id,
            fix_key=fix_key,
        ),
        "scope_id": scope_id,
        "kind": "fix",
        "fix_key": fix_key,
        "individual": fix.get("individual"),
        "set": fix.get("set", "train"),
        "time_ms": time_ms,
        "time": _format_time(time_ms),
        "lon": fix.get("lon"),
        "lat": fix.get("lat"),
        "attributes": dict(fix.get("attributes") or {}) if isinstance(fix.get("attributes"), dict) else {},
        "review": dict(fix.get("review") or {}) if isinstance(fix.get("review"), dict) else {},
        "segments": list(fix.get("segments") or []) if isinstance(fix.get("segments"), list) else [],
        "evidence": {
            "field": field,
            "op": op,
            "expected_value": expected_value,
            "value": value,
            "field_label": field,
            "value_display": value,
            "threshold_display": expected_value,
        },
    }


def _osm_candidate_from_fix(
    fix: dict,
    *,
    nearest: dict,
    threshold_m: float,
    selectors: list[dict],
    element_types: list[str],
    scope_id: str,
    run_digest_value: str,
    dataset_id: str,
    logical_name: str,
) -> dict:
    time_ms = fix.get("time_ms")
    fix_key = fix.get("fix_key")
    feature = nearest["feature"]
    properties = nearest.get("properties") if isinstance(nearest.get("properties"), dict) else {}
    tags = properties.get("tags") if isinstance(properties.get("tags"), dict) else {}
    distance_m = float(nearest["distance_m"])
    return {
        "candidate_id": _candidate_id(
            run_digest_value=run_digest_value,
            dataset_id=dataset_id,
            logical_name=logical_name,
            scope_id=scope_id,
            fix_key=fix_key,
        ),
        "scope_id": scope_id,
        "kind": "fix",
        "fix_key": fix_key,
        "individual": fix.get("individual"),
        "set": fix.get("set", "train"),
        "time_ms": time_ms,
        "time": _format_time(time_ms),
        "lon": fix.get("lon"),
        "lat": fix.get("lat"),
        "attributes": dict(fix.get("attributes") or {}) if isinstance(fix.get("attributes"), dict) else {},
        "review": dict(fix.get("review") or {}) if isinstance(fix.get("review"), dict) else {},
        "segments": list(fix.get("segments") or []) if isinstance(fix.get("segments"), list) else [],
        "evidence": {
            "field": "osm_proximity",
            "distance_m": distance_m,
            "threshold_m": threshold_m,
            "osm_feature_id": feature.get("id") or _osm_feature_id(properties),
            "osm_feature_type": properties.get("osm_type", ""),
            "osm_feature_name": properties.get("name", ""),
            "osm_tags": tags,
            "value_display": _format_display_value(distance_m, "m"),
            "threshold_display": _format_display_value(threshold_m, "m"),
            "selectors": selectors,
            "element_types": element_types,
        },
    }


def _osm_feature_id(properties: dict) -> str:
    osm_type = str(properties.get("osm_type") or "")
    osm_id = properties.get("osm_id")
    return f"{osm_type}/{osm_id}" if osm_type and osm_id not in (None, "") else ""


def _candidate_id(
    *,
    run_digest_value: str,
    dataset_id: str,
    logical_name: str,
    scope_id: str,
    fix_key: object,
) -> str:
    serialized = json.dumps(
        {
            "dataset_id": dataset_id,
            "fix_key": str(fix_key or ""),
            "logical_name": logical_name,
            "run_digest": run_digest_value,
            "scope_id": scope_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"cq:{hashlib.sha256(serialized.encode('utf-8')).hexdigest()[:16]}"


def _format_time(raw_time_ms: object) -> str:
    try:
        time_ms = int(raw_time_ms)
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _evidence_display_metadata(field: str, *, value: float, threshold: float) -> dict:
    metadata = FIELD_DISPLAY_METADATA.get(field)
    if not metadata:
        return {"field_label": field}
    display_unit = metadata["display_unit"]
    scale = float(metadata.get("display_scale") or 1.0)
    return {
        "field_label": metadata["field_label"],
        "unit": metadata["unit"],
        "display_unit": display_unit,
        "value_display": _format_display_value(value * scale, display_unit),
        "threshold_display": _format_display_value(threshold * scale, display_unit),
    }


def _format_display_value(value: float, unit: str) -> str:
    text = f"{value:.6g}"
    return f"{text} {unit}" if unit else text


@lru_cache(maxsize=1)
def _source_digest() -> str:
    try:
        content = Path(__file__).read_bytes()
    except OSError:
        content = EVALUATOR_IMPLEMENTATION_VERSION.encode("utf-8")
    return hashlib.sha256(content).hexdigest()[:16]
