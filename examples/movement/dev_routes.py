"""Development-playground routes excluded from the shipped slim application."""

from collections.abc import Callable
from math import isfinite
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.auth import Actor, apply_actor, request_actor
from app.edit_locks import (
    EditConflictError,
    EditLockedError,
    create_guarded_step,
    require_editable_dataset,
)
from app.execution import create_analysis
from app.query_library import get_query
from app.reviews import (
    ReviewConflictError,
    ReviewForbiddenError,
    ReviewLockedError,
    ReviewStateError,
    active_review,
    authorize_analysis,
    authorize_persistent_change,
    load_review_state,
)
from app.state import (
    ProjectStateError,
    dataset_summary,
    get_dataset_artifact,
    list_history,
    load_dataset,
    load_project_state,
)
from app.web import json_error, parse_json_body, validate_path_part

from .script_bundle import build_self_contained_script


StudyDirResolver = Callable[[str, str], Path]

MOVEMENT_REVIEW_MODULES = (
    "examples.movement.bursts",
    "examples.movement.movement_features",
    "examples.movement.summary",
    "examples.movement.review_annotations",
)
MOVEMENT_CANDIDATE_QUERY_MODULES = (
    *MOVEMENT_REVIEW_MODULES,
    "app.osm",
    "examples.movement.candidate_segments",
    "examples.movement.osm_context",
    "examples.movement.candidate_queries",
)
MOVEMENT_FEATURE_SPACE_MODULES = (
    *MOVEMENT_REVIEW_MODULES,
    "examples.movement.burst_features",
    "examples.movement.burst_feature_matrix",
    "examples.movement.burst_feature_space",
)
MOVEMENT_OSM_ENRICHMENT_MODULES = (
    "examples.movement.bursts",
    "examples.movement.movement_features",
    "examples.movement.summary",
    "app.osm",
    "examples.movement.osm_context",
    "examples.movement.osm_extracts",
    "examples.movement.osm_enrichment",
)

CANDIDATE_QUERY_ANALYSIS_TEMPLATE_PATH = Path(__file__).with_name(
    "candidate_query_analysis_template.py"
)
CANDIDATE_QUERY_ANALYSIS_SCRIPT = build_self_contained_script(
    CANDIDATE_QUERY_ANALYSIS_TEMPLATE_PATH,
    MOVEMENT_CANDIDATE_QUERY_MODULES,
)
BURST_FEATURE_SPACE_ANALYSIS_TEMPLATE_PATH = Path(__file__).with_name(
    "burst_feature_space_analysis_template.py"
)
BURST_FEATURE_SPACE_ANALYSIS_SCRIPT = build_self_contained_script(
    BURST_FEATURE_SPACE_ANALYSIS_TEMPLATE_PATH,
    MOVEMENT_FEATURE_SPACE_MODULES,
)
OSM_ENRICHMENT_TEMPLATE_PATH = Path(__file__).with_name("osm_enrichment_template.py")
OSM_ENRICHMENT_SCRIPT = build_self_contained_script(
    OSM_ENRICHMENT_TEMPLATE_PATH,
    MOVEMENT_OSM_ENRICHMENT_MODULES,
)


def _parse_optional_int(raw_value: object, *, label: str) -> int | None:
    if raw_value in (None, ""):
        return None
    if isinstance(raw_value, bool):
        raise ValueError(f"Invalid {label}")
    try:
        return int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {label}") from exc


def _parse_optional_limit(raw_value: object) -> int | None:
    if raw_value in (None, ""):
        return None
    value = _parse_optional_int(raw_value, label="limit")
    if value is None or value <= 0:
        raise ValueError("Invalid limit")
    return value


def _parse_burst_gap_seconds(raw_value: object, *, default: float) -> float:
    if raw_value in (None, ""):
        return default
    try:
        value = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid burst_gap_seconds") from exc
    if not isfinite(value) or value <= 0:
        raise ValueError("Invalid burst_gap_seconds")
    return value


def _parse_burst_gap_mode(raw_value: object, *, default: str) -> str:
    if raw_value in (None, ""):
        return default
    value = str(raw_value).strip().lower()
    if value not in {"manual", "quantile"}:
        raise ValueError("Invalid burst_gap_mode")
    return value


def _parse_burst_gap_quantile(raw_value: object, *, default: float) -> float:
    if raw_value in (None, ""):
        return default
    try:
        value = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid burst_gap_quantile") from exc
    if not isfinite(value) or value <= 0.0 or value > 1.0:
        raise ValueError("Invalid burst_gap_quantile")
    return value


def _parse_anomaly_feature_set(raw_value: object) -> str:
    value = str(raw_value or "movement_only").strip()
    if value in {"movement_only", "movement_plus_context"}:
        return value
    raise ValueError("Invalid feature_set")


def _parse_osm_search_radius_m(raw_value: object) -> float:
    if raw_value in (None, ""):
        raise ValueError("search_radius_m is required")
    if isinstance(raw_value, bool):
        raise ValueError("Invalid search_radius_m")
    try:
        value = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid search_radius_m") from exc
    if not isfinite(value) or value <= 0.0:
        raise ValueError("Invalid search_radius_m")
    return value


def _validate_query_definition(value: object) -> dict:
    if not isinstance(value, dict):
        raise ValueError("Query definition is required")
    evaluator = value.get("evaluator")
    definition = value.get("definition")
    if not isinstance(evaluator, dict) or not isinstance(definition, dict):
        raise ValueError("Query definition must include evaluator and definition")
    if evaluator.get("type") not in {
        "fix_numeric_comparison",
        "fix_osm_proximity",
        "fix_string_comparison",
    }:
        raise ValueError("Unsupported candidate query evaluator")
    return dict(value)


def _validate_query_parameters(value: object) -> dict:
    if value in (None, ""):
        return {}
    if not isinstance(value, dict):
        raise ValueError("Invalid query parameters")
    return dict(value)


def _movement_analysis_input_names(dataset: dict, logical_name: str) -> list[str]:
    names = [logical_name]
    if any(
        artifact.get("logical_name") == "movement_review_annotations.json"
        for artifact in dataset.get("artifacts", [])
    ):
        names.append("movement_review_annotations.json")
    return names


def _reusable_osm_enrichment_response(
    study_dir: Path,
    *,
    parent_dataset_id: str,
    logical_name: str,
    search_radius_m: float,
) -> dict | None:
    history = list_history(study_dir)
    for step in reversed(history["steps"]):
        parameters = dict(step.get("parameters") or {})
        summary = dict(step.get("summary") or {})
        if step.get("parent_dataset_id") != parent_dataset_id:
            continue
        if parameters.get("action") != "enrich_osm_context":
            continue
        if parameters.get("target_artifact") != logical_name:
            continue
        if parameters.get("search_radius_m") != search_radius_m:
            continue
        if (
            summary.get("run_status") != "completed"
            or summary.get("source_type") != "local_extract"
        ):
            continue
        if "movement_osm_context.csv" not in step.get("output_artifacts", []):
            continue
        try:
            output_dataset = load_dataset(study_dir, step["output_dataset_id"])
            get_dataset_artifact(
                study_dir,
                output_dataset["dataset_id"],
                "movement_osm_context.csv",
            )
        except (KeyError, ProjectStateError):
            continue
        return {
            "step": step,
            "dataset": dataset_summary(output_dataset),
            "history": history,
            "reused": True,
        }
    return None


def register_movement_dev_routes(
    app: FastAPI,
    *,
    data_root: Path,
    configured_study_dir: StudyDirResolver,
    default_burst_gap_mode: str,
    default_burst_gap_seconds: float,
    default_burst_gap_quantile: float,
    annotate_scope_script: str,
    publish_event: Callable[[str, str, Path, str, Actor | None], None] | None = None,
):
    """Register candidate, feature-space, and OSM-enrichment playground routes."""

    authentication_enabled = getattr(app.state, "auth_manager", None) is not None

    def prepare_analysis(request: Request, study_dir: Path, payload: dict) -> dict:
        actor = request_actor(request) if authentication_enabled else None
        if actor is None:
            return payload
        review = authorize_analysis(study_dir, actor)
        return apply_actor(payload, actor, review_id=str((review or {}).get("review_id") or ""))

    def review_error(exc: Exception) -> JSONResponse:
        if isinstance(exc, ReviewForbiddenError):
            return json_error(str(exc), 403)
        if isinstance(exc, ReviewConflictError):
            return json_error(str(exc), 409)
        if isinstance(exc, ReviewLockedError):
            return JSONResponse({"error": str(exc), "code": exc.code}, status_code=423)
        return json_error(str(exc), 400)

    def prepare_annotation_step(
        request: Request,
        study_dir: Path,
        body: dict,
        payload: dict,
    ) -> tuple[dict, Callable[[], None] | None, Actor | None]:
        actor = request_actor(request) if authentication_enabled else None
        if actor is None:
            return payload, None, None
        expected_revision = body.get("expected_review_revision")
        review = authorize_persistent_change(
            study_dir,
            actor,
            expected_review_revision=expected_revision,
            review_effect="annotation_only",
        )
        updated = apply_actor(
            payload,
            actor,
            review_id=str((review or {}).get("review_id") or ""),
        )
        parameters = dict(updated.get("parameters") or {})
        parameters["workflow"] = {
            "review_effect": "annotation_only",
            "review_impact": {"scope": "none", "actor": actor.as_dict()},
        }
        updated["parameters"] = parameters

        def preflight() -> None:
            authorize_persistent_change(
                study_dir,
                actor,
                expected_review_revision=expected_revision,
                review_effect="annotation_only",
            )

        return updated, preflight, actor

    @app.post(
        "/api/apps/movement/family/{family_name}/study/{study_name}/"
        "actions/run-candidate-query"
    )
    async def post_movement_run_candidate_query(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        analysis_result = None
        try:
            study_dir = configured_study_dir(family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            dataset = load_dataset(study_dir, dataset_id)
            head_at_analysis_start = str(load_project_state(study_dir)["current_dataset_id"])
            query_parameters = _validate_query_parameters(
                body.get("query_parameters", body.get("parameters"))
            )
            query_id = str(body.get("query_id") or "").strip()
            if query_id:
                query_version = _parse_optional_int(
                    body.get("query_version", body.get("version")),
                    label="query_version",
                )
                query_definition = _validate_query_definition(
                    get_query(data_root, query_id, version=query_version)
                )
            else:
                query_definition = _validate_query_definition(
                    body.get("query_definition")
                )
            query_label = (
                str(query_definition.get("name") or "").strip()
                or str(query_definition.get("query_id") or "").strip()
                or "inline candidate query"
            )
            actor = request_actor(request) if authentication_enabled else None
            user = actor.display_name if actor is not None else body.get("user")
            if actor is not None:
                authorize_persistent_change(
                    study_dir,
                    actor,
                    expected_review_revision=body.get("expected_review_revision"),
                    review_effect="annotation_only",
                )
            payload = prepare_analysis(
                request,
                study_dir,
                {
                        "user": user,
                        "title": f"Run candidate query {query_label} on {logical_name}",
                        "kind": "python",
                        "script": CANDIDATE_QUERY_ANALYSIS_SCRIPT,
                        "dataset_id": dataset_id,
                        "input_artifacts": _movement_analysis_input_names(
                            dataset,
                            logical_name,
                        ),
                        "output_artifacts": ["candidate_query_results.json"],
                        "parameters": {
                            "app": "movement",
                            "action": "run_candidate_query",
                            "target_artifact": logical_name,
                            "dataset_id": dataset_id,
                            "query_id": query_definition.get("query_id", ""),
                            "query_version": query_definition.get("version"),
                            "query_definition": query_definition,
                            "query_parameters": query_parameters,
                            "execution_scope": body.get("execution_scope"),
                            "preview_limit": _parse_optional_limit(
                                body.get("preview_limit")
                            ),
                            "user": user,
                        },
                    },
            )
            analysis_result = create_analysis(study_dir, payload)
            summary = dict(analysis_result.get("summary") or {})
            candidate_count = int(summary.get("candidate_count") or 0)
            if summary.get("run_status") != "success" or candidate_count <= 0:
                return JSONResponse(analysis_result)
            row_ranges = list(summary.get("match_row_ranges") or [])
            if not row_ranges:
                raise ProjectStateError("Candidate analysis did not persist its complete match ranges")
            input_artifacts = _movement_analysis_input_names(dataset, logical_name)
            analysis_id = str((analysis_result.get("analysis") or {}).get("analysis_id") or "")
            description = str(query_definition.get("description") or "").strip()
            step_payload = {
                "user": user,
                "title": f"Flag {candidate_count} candidate fix(es) from {query_label}",
                "kind": "python",
                "script": annotate_scope_script,
                "parameters": {
                    "app": "movement",
                    "action": "annotate_scope",
                    "target_artifact": logical_name,
                    "dataset_id": dataset_id,
                    "scope": {
                        "kind": "fix",
                        "row_ranges": row_ranges,
                        "fix_count": candidate_count,
                    },
                    "status": "suspected",
                    "origin": "algorithm",
                    "issue_type": query_label,
                    "issue_field": "",
                    "issue_threshold": "",
                    "comment": description or f"Matched candidate query {query_label}.",
                    "owner_question": "Should these filter matches be treated as outliers?",
                    "source_analysis_id": analysis_id,
                    "user": user,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": input_artifacts,
                "output_artifacts": ["movement_review_annotations.json"],
                "set_as_head": True,
            }
            step_payload, preflight, actor = prepare_annotation_step(
                request,
                study_dir,
                body,
                step_payload,
            )
            step_result = create_guarded_step(
                study_dir,
                step_payload,
                selected_dataset_id=dataset_id,
                expected_current_dataset_id=str(
                    body.get("expected_current_dataset_id")
                    or head_at_analysis_start
                ),
                preflight=preflight,
            )
            if publish_event is not None:
                publish_event(family_name, study_name, study_dir, "dataset_head_changed", actor)
            return JSONResponse({**analysis_result, **step_result})
        except EditLockedError as exc:
            payload = {"error": str(exc), "code": "edit_locked", "edit_profile": exc.profile}
            if analysis_result:
                payload["analysis"] = analysis_result.get("analysis")
                payload["summary"] = analysis_result.get("summary")
            return JSONResponse(payload, status_code=423)
        except EditConflictError as exc:
            payload = {"error": str(exc), "code": "edit_conflict"}
            if analysis_result:
                payload["analysis"] = analysis_result.get("analysis")
                payload["summary"] = analysis_result.get("summary")
            return JSONResponse(payload, status_code=409)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return review_error(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post(
        "/api/apps/movement/family/{family_name}/study/{study_name}/"
        "actions/run-burst-feature-space"
    )
    async def post_movement_run_burst_feature_space(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = configured_study_dir(family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            dataset = load_dataset(study_dir, dataset_id)
            feature_set = _parse_anomaly_feature_set(body.get("feature_set"))
            feature_set_label = (
                "movement + OSM context"
                if feature_set == "movement_plus_context"
                else "movement only"
            )
            actor = request_actor(request) if authentication_enabled else None
            user = actor.display_name if actor is not None else body.get("user")
            payload = prepare_analysis(
                request,
                study_dir,
                {
                        "user": user,
                        "title": (
                            "Project automatic movement bursts "
                            f"({feature_set_label}) for {logical_name}"
                        ),
                        "kind": "python",
                        "script": BURST_FEATURE_SPACE_ANALYSIS_SCRIPT,
                        "dataset_id": dataset_id,
                        "input_artifacts": _movement_analysis_input_names(
                            dataset,
                            logical_name,
                        ),
                        "output_artifacts": ["burst_feature_space.json"],
                        "parameters": {
                            "app": "movement",
                            "action": "run_burst_feature_space",
                            "target_artifact": logical_name,
                            "dataset_id": dataset_id,
                            "burst_gap_mode": _parse_burst_gap_mode(
                                body.get("burst_gap_mode"),
                                default=default_burst_gap_mode,
                            ),
                            "burst_gap_seconds": _parse_burst_gap_seconds(
                                body.get("burst_gap_seconds"),
                                default=default_burst_gap_seconds,
                            ),
                            "burst_gap_quantile": _parse_burst_gap_quantile(
                                body.get("burst_gap_quantile"),
                                default=default_burst_gap_quantile,
                            ),
                            "feature_set": feature_set,
                            "user": user,
                        },
                    },
            )
            return JSONResponse(create_analysis(study_dir, payload))
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return review_error(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post(
        "/api/apps/movement/family/{family_name}/study/{study_name}/"
        "actions/enrich-osm-context"
    )
    async def post_movement_enrich_osm_context(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = configured_study_dir(family_name, study_name)
            actor = request_actor(request) if authentication_enabled else None
            if actor is not None and actor.role != "editor":
                raise ReviewForbiddenError("Only editors can run persistent enrichment")
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            search_radius_m = _parse_osm_search_radius_m(body.get("search_radius_m"))
            confirmed_large_download = body.get("confirmed_large_download", False)
            if not isinstance(confirmed_large_download, bool):
                raise ValueError("Invalid confirmed_large_download")
            reusable = _reusable_osm_enrichment_response(
                study_dir,
                parent_dataset_id=dataset_id,
                logical_name=logical_name,
                search_radius_m=search_radius_m,
            )
            if reusable is not None:
                return JSONResponse(reusable)
            require_editable_dataset(study_dir, dataset_id)
            user = actor.display_name if actor is not None else body.get("user")
            payload = {
                "user": user,
                "title": f"Add OSM road and railway context to {logical_name}",
                "kind": "python",
                "script": OSM_ENRICHMENT_SCRIPT,
                "parameters": {
                    "app": "movement",
                    "action": "enrich_osm_context",
                    "target_artifact": logical_name,
                    "search_radius_m": search_radius_m,
                    "confirmed_large_download": confirmed_large_download,
                    "data_root": str(data_root.resolve()),
                    "user": user,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": [logical_name],
                "output_artifacts": ["movement_osm_context.csv"],
                "set_as_head": True,
            }
            preflight = None
            if actor is not None:
                review = authorize_persistent_change(
                    study_dir,
                    actor,
                    expected_review_revision=body.get("expected_review_revision"),
                    review_effect="preserves_individual_scope",
                )
                payload = apply_actor(
                    payload,
                    actor,
                    review_id=str((review or {}).get("review_id") or ""),
                )
                parameters = dict(payload.get("parameters") or {})
                workflow = dict(parameters.get("workflow") or {})
                workflow.update(
                    {
                        "review_effect": "preserves_individual_scope",
                        "review_impact": {"scope": "none", "actor": actor.as_dict()},
                    }
                )
                parameters["workflow"] = workflow
                payload["parameters"] = parameters

                def preflight() -> None:
                    authorize_persistent_change(
                        study_dir,
                        actor,
                        expected_review_revision=body.get("expected_review_revision"),
                        review_effect="preserves_individual_scope",
                    )

            result = create_guarded_step(
                study_dir,
                payload,
                selected_dataset_id=dataset_id,
                expected_current_dataset_id=str(
                    body.get("expected_current_dataset_id")
                    or load_project_state(study_dir)["current_dataset_id"]
                ),
                preflight=preflight,
            )
            if publish_event is not None:
                publish_event(family_name, study_name, study_dir, "dataset_head_changed", actor)
            return JSONResponse(result)
        except EditLockedError as exc:
            return JSONResponse(
                {
                    "error": str(exc),
                    "code": "edit_locked",
                    "edit_profile": exc.profile,
                },
                status_code=423,
            )
        except EditConflictError as exc:
            return JSONResponse(
                {
                    "error": str(exc),
                    "code": "edit_conflict",
                },
                status_code=409,
            )
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return review_error(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)
