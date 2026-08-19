import base64
import binascii
import asyncio
from collections.abc import Callable
import json
from math import isfinite
from pathlib import Path
from threading import Lock
from time import monotonic

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool

from app.auth import Actor, actor_payload, apply_actor, request_actor
from app.edit_locks import (
    EditConflictError,
    EditLockedError,
    build_edit_lock_profile,
    create_guarded_step,
    resume_from_dataset,
    undo_guarded,
)
from app.execution import create_analysis
from app.events import StudyEventBroker
from app.reviews import (
    ReviewConflictError,
    ReviewForbiddenError,
    ReviewLockedError,
    ReviewStateError,
    active_review,
    assign_review,
    authorize_analysis,
    authorize_persistent_change,
    can_read_study,
    carryover_second_opinions,
    cancel_review,
    complete_review,
    finish_editor_control,
    load_review_state,
    review_coverage,
    review_profile,
    start_editor_control,
    valid_review_decisions,
)
from app.state import (
    ProjectStateError,
    get_dataset_artifact,
    graph_payload,
    list_history,
    load_dataset,
    load_json,
    load_project_state,
    make_id,
    media_type_for_path,
    now_iso,
    normalize_user,
    project_paths,
    project_state_payload,
)
from app.web import get_project_dir, json_error, parse_json_body, validate_path_part

from .analysis_history import build_movement_analysis_history
from .catalog import get_study_dir, list_families, list_studies
from .review_annotations import (
    apply_review_annotation_counts,
    apply_review_annotations,
    compress_fix_keys,
    confirmed_exclusion_scopes,
    load_review_annotations,
    row_tokens_for_scope,
)
from .script_bundle import build_self_contained_script
from .summary import (
    DEFAULT_BURST_GAP_MODE,
    DEFAULT_BURST_GAP_QUANTILE,
    DEFAULT_BURST_GAP_SECONDS,
    DEFAULT_FIX_LIMIT,
    build_movement_fixes,
    build_movement_overview,
    build_movement_summary,
    list_movement_individuals,
)


ArtifactFilter = Callable[[dict], bool]
MAX_REPORT_SNAPSHOTS = 100
MAX_REPORT_SNAPSHOT_BYTES = 20 * 1024 * 1024
MAX_BACKGROUND_ANALYSIS_JOBS = 100


def _edit_locked_response(exc: EditLockedError) -> JSONResponse:
    return JSONResponse(
        {
            "error": str(exc),
            "code": "edit_locked",
            "edit_profile": exc.profile,
        },
        status_code=423,
    )


def _edit_conflict_response(exc: EditConflictError) -> JSONResponse:
    return JSONResponse(
        {
            "error": str(exc),
            "code": "edit_conflict",
        },
        status_code=409,
    )


def _review_error_response(exc: Exception) -> JSONResponse:
    if isinstance(exc, ReviewForbiddenError):
        return json_error(str(exc), 403)
    if isinstance(exc, ReviewConflictError):
        return JSONResponse({"error": str(exc), "code": "review_conflict"}, status_code=409)
    if isinstance(exc, ReviewLockedError):
        return JSONResponse(
            {"error": str(exc), "code": exc.code},
            status_code=423,
        )
    return json_error(str(exc), 400)


def _build_initial_study_payload(
    study_dir: Path,
    artifact_filter: ArtifactFilter | None = None,
) -> dict:
    state = project_state_payload(study_dir)
    graph = graph_payload(study_dir)
    dataset_id = state["current_dataset"]["dataset_id"]
    dataset = load_dataset(study_dir, dataset_id)
    artifacts = [
        artifact
        for artifact in dataset.get("artifacts") or []
        if artifact_filter is None or artifact_filter(artifact)
    ]
    if not artifacts:
        raise ProjectStateError("Selected dataset has no artifacts")
    logical_name = str(artifacts[0].get("logical_name") or "").strip()
    if not logical_name:
        raise ProjectStateError("Selected dataset has no artifacts")
    return {
        "state": state,
        "graph": graph,
        "dataset": dataset,
        "dataset_id": dataset_id,
        "logical_name": logical_name,
    }


def _apply_dataset_review_annotations(
    study_dir: Path,
    *,
    dataset_id: str,
    logical_name: str,
    payload: dict,
) -> dict:
    annotations = _load_dataset_review_annotations(study_dir, dataset_id=dataset_id)
    return apply_review_annotations(payload, annotations, source_artifact=logical_name)


def _load_dataset_review_annotations(study_dir: Path, *, dataset_id: str) -> list[dict]:
    try:
        _, sidecar_path = get_dataset_artifact(
            study_dir,
            dataset_id,
            "movement_review_annotations.json",
        )
    except ProjectStateError:
        return []
    return load_review_annotations(sidecar_path)


def _review_annotation_candidates(
    annotations: list[dict],
    *,
    logical_name: str,
) -> tuple[set[str], set[str]]:
    fix_keys: set[str] = set()
    individuals: set[str] = set()
    for annotation in annotations:
        if not annotation.get("status"):
            continue
        source_artifact = str(annotation.get("source_artifact") or "")
        if source_artifact and source_artifact != logical_name:
            continue
        scope = annotation.get("scope") or {}
        fix_keys.update(str(item) for item in scope.get("fix_keys") or [] if str(item))
        fix_keys.update(row_tokens_for_scope(scope))
        if str(scope.get("kind") or "") == "individual":
            individual = str(scope.get("individual") or "").strip()
            if individual:
                individuals.add(individual)
    return fix_keys, individuals


def _filter_review_status_payload(payload: dict, *, review_status: str, limit: int | None) -> dict:
    payload = dict(payload)
    fixes = list(payload.get("fixes") or [])
    if review_status == "reviewed":
        matches = [fix for fix in fixes if bool(fix.get("review"))]
    else:
        matches = [
            fix
            for fix in fixes
            if str((fix.get("review") or {}).get("status") or "").strip().lower() == review_status
        ]
    returned = matches if limit is None else matches[:limit]
    payload["fixes"] = returned
    payload["segments"] = []
    payload["auto_bursts"] = []
    payload["matching_fix_count"] = len(matches)
    payload["returned_fix_count"] = len(returned)
    payload["truncated"] = len(returned) < len(matches)
    detail_scope = dict(payload.get("detail_scope") or {})
    detail_scope["review_status"] = review_status
    detail_scope["limit"] = limit
    payload["detail_scope"] = detail_scope
    return payload


def _movement_analysis_input_names(dataset: dict, logical_name: str) -> list[str]:
    names = [logical_name]
    if any(
        artifact.get("logical_name") == "movement_review_annotations.json"
        for artifact in dataset.get("artifacts", [])
    ):
        names.append("movement_review_annotations.json")
    return names


MOVEMENT_SUMMARY_MODULES = (
    "examples.movement.bursts",
    "examples.movement.movement_features",
    "examples.movement.summary",
)
MOVEMENT_REVIEW_MODULES = (
    *MOVEMENT_SUMMARY_MODULES,
    "examples.movement.review_annotations",
)
MOVEMENT_ANOMALY_MODULES = (
    *MOVEMENT_REVIEW_MODULES,
    "examples.movement.burst_features",
    "examples.movement.burst_feature_matrix",
    "examples.movement.anomaly_ranking",
)
REPORT_ANALYSIS_TEMPLATE_PATH = Path(__file__).with_name("report_analysis_template.py")
GENERATE_REPORT_SCRIPT = build_self_contained_script(
    REPORT_ANALYSIS_TEMPLATE_PATH,
    MOVEMENT_REVIEW_MODULES,
)
ANOMALY_ANALYSIS_TEMPLATE_PATH = Path(__file__).with_name("anomaly_analysis_template.py")
BURST_ANOMALY_ANALYSIS_SCRIPT = build_self_contained_script(
    ANOMALY_ANALYSIS_TEMPLATE_PATH,
    MOVEMENT_ANOMALY_MODULES,
)

EXPORT_REVIEWED_CSV_TEMPLATE_PATH = Path(__file__).with_name("export_reviewed_csv_analysis_template.py")
EXPORT_REVIEWED_CSV_SCRIPT = build_self_contained_script(
    EXPORT_REVIEWED_CSV_TEMPLATE_PATH,
    MOVEMENT_REVIEW_MODULES,
)

ANNOTATE_SCOPE_TEMPLATE_PATH = Path(__file__).with_name("annotate_scope_step_template.py")
ANNOTATE_SCOPE_SCRIPT = build_self_contained_script(
    ANNOTATE_SCOPE_TEMPLATE_PATH,
    MOVEMENT_REVIEW_MODULES,
)

CONFIRM_ISSUES_TEMPLATE_PATH = Path(__file__).with_name("confirm_issues_step_template.py")
CONFIRM_ISSUES_SCRIPT = build_self_contained_script(
    CONFIRM_ISSUES_TEMPLATE_PATH,
    MOVEMENT_REVIEW_MODULES,
)

DISMISS_ISSUES_TEMPLATE_PATH = Path(__file__).with_name("dismiss_issues_step_template.py")
DISMISS_ISSUES_SCRIPT = build_self_contained_script(
    DISMISS_ISSUES_TEMPLATE_PATH,
    MOVEMENT_REVIEW_MODULES,
)

REVIEW_INDIVIDUALS_TEMPLATE_PATH = Path(__file__).with_name(
    "review_individuals_step_template.py"
)
REVIEW_INDIVIDUALS_SCRIPT = build_self_contained_script(
    REVIEW_INDIVIDUALS_TEMPLATE_PATH,
    MOVEMENT_REVIEW_MODULES,
)


def _validate_fix_keys(value: object, *, allow_empty: bool = False) -> list[str]:
    if value is None and allow_empty:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid fix list")
    cleaned = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("Invalid fix list")
        key = " ".join(item.strip().split())
        if not key:
            raise ValueError("Invalid fix list")
        cleaned.append(key)
    unique = sorted(set(cleaned))
    if not unique and not allow_empty:
        raise ValueError("Select at least one fix")
    return unique


def _validate_confirmations(value: object) -> list[dict]:
    if not isinstance(value, list) or not value:
        raise ValueError("Select at least one suspected issue")
    confirmations = []
    seen = set()
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("Invalid confirmation list")
        parent_annotation_id = _validate_required_text(
            item.get("parent_annotation_id"),
            label="Suspected issue id",
            max_length=240,
        )
        fix_keys = _validate_fix_keys(item.get("fix_keys"))
        row_ranges = compress_fix_keys(fix_keys)
        key = (parent_annotation_id, tuple(tuple(item) for item in row_ranges))
        if key in seen:
            continue
        seen.add(key)
        confirmations.append({
            "parent_annotation_id": parent_annotation_id,
            "row_ranges": row_ranges,
            "fix_count": len(fix_keys),
        })
    return confirmations


def _validate_dismissals(value: object) -> list[dict]:
    try:
        return _validate_confirmations(value)
    except ValueError as exc:
        message = str(exc).replace("confirmation", "dismissal").replace("Confirm", "Dismiss")
        raise ValueError(message) from exc


def _validate_individual_review_decisions(value: object) -> list[dict]:
    if not isinstance(value, list) or not value:
        raise ValueError("Review at least one individual")
    if len(value) > 25:
        raise ValueError("A review page supports at most 25 individual decisions")
    decisions = []
    seen = set()
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("Invalid individual review decisions")
        individual = _normalize_individual_name(item.get("individual"))
        if individual in seen:
            raise ValueError(f"Duplicate review decision for {individual}")
        seen.add(individual)
        raw_decision = str(item.get("review_decision") or "").strip().lower()
        review_ok = item.get("review_ok")
        if not raw_decision and isinstance(review_ok, bool):
            raw_decision = "ok" if review_ok else "not_ok"
        if raw_decision not in {"ok", "not_ok", "second_opinion"}:
            raise ValueError(
                "Individual review decisions require ok, not_ok, or second_opinion"
            )
        decisions.append(
            {
                "individual": individual,
                "review_decision": raw_decision,
                "review_ok": raw_decision == "ok",
                "comment": _validate_optional_text(
                    item.get("comment"),
                    label="Review comment",
                    max_length=1200,
                ),
            }
        )
    return decisions


def _validate_fix_key(value: object, *, label: str) -> str:
    return _validate_required_text(value, label=label, max_length=240)


def _validate_issue_ids(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid issue list")
    cleaned = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("Invalid issue list")
        issue_id = " ".join(item.strip().split())
        if not issue_id:
            raise ValueError("Invalid issue list")
        cleaned.append(issue_id)
    return sorted(set(cleaned))


def _validate_status(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Status is required")
    status = value.strip().lower()
    if status not in {"suspected", "confirmed"}:
        raise ValueError("Status must be suspected or confirmed")
    return status


def _validate_filter_scope(value: object) -> dict:
    if not isinstance(value, dict):
        raise ValueError("Filter definition is required")
    field_key = _validate_required_text(
        value.get("field_key"),
        label="Filter field",
        max_length=240,
    )
    field_kind = str(value.get("field_kind") or "").strip().lower()
    if field_kind not in {"numeric", "boolean", "categorical"}:
        raise ValueError("Invalid filter field kind")
    result: dict[str, object] = {
        "field_key": field_key,
        "field_kind": field_kind,
    }
    if field_kind == "numeric":
        raw_threshold = value.get("threshold_value")
        if isinstance(raw_threshold, bool):
            raise ValueError("Filter threshold must be numeric")
        try:
            threshold = float(raw_threshold)
        except (TypeError, ValueError) as exc:
            raise ValueError("Filter threshold must be numeric") from exc
        if not isfinite(threshold):
            raise ValueError("Filter threshold must be finite")
        operator = str(value.get("operator") or "gt").strip().lower()
        if operator not in {"gt", "lt"}:
            raise ValueError("Invalid numeric filter operator")
        result.update({"operator": operator, "threshold_value": threshold})
    else:
        raw_levels = value.get("selected_levels")
        if not isinstance(raw_levels, list):
            raise ValueError("Filter levels must be a list")
        levels = list(
            dict.fromkeys(
                _validate_required_text(item, label="Filter level", max_length=240)
                for item in raw_levels
            )
        )
        if not levels or len(levels) > 100:
            raise ValueError("Choose between 1 and 100 filter levels")
        result["selected_levels"] = levels
    return result


def _validate_required_text(value: object, *, label: str, max_length: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} is required")
    normalized = " ".join(value.strip().split())
    if not normalized:
        raise ValueError(f"{label} is required")
    if len(normalized) > max_length:
        raise ValueError(f"{label} is too long")
    return normalized


def _validate_optional_text(value: object, *, label: str, max_length: int) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"Invalid {label.lower()}")
    normalized = " ".join(value.strip().split())
    if len(normalized) > max_length:
        raise ValueError(f"{label} is too long")
    return normalized


def _validate_screenshot_mode(value: object) -> str:
    if value is None:
        return "manual"
    if not isinstance(value, str):
        raise ValueError("Invalid screenshot mode")
    mode = value.strip().lower()
    if mode not in {"manual", "auto"}:
        raise ValueError("Invalid screenshot mode")
    return mode


def _validate_snapshots(value: object) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid snapshots payload")
    if len(value) > MAX_REPORT_SNAPSHOTS:
        raise ValueError(f"Reports support at most {MAX_REPORT_SNAPSHOTS} snapshots")
    snapshots = []
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            raise ValueError("Invalid snapshots payload")
        data_url = item.get("data_url")
        if not isinstance(data_url, str) or not data_url.strip():
            raise ValueError("Each snapshot must include image data")
        header, separator, encoded = data_url.strip().partition(",")
        if separator != "," or header.lower() != "data:image/png;base64":
            raise ValueError("Snapshots must be base64-encoded PNG images")
        try:
            content = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("Snapshot image data is invalid") from exc
        if not content.startswith(b"\x89PNG\r\n\x1a\n"):
            raise ValueError("Snapshot image data is not a PNG")
        if len(content) > MAX_REPORT_SNAPSHOT_BYTES:
            raise ValueError("Snapshot image is too large")
        caption = _validate_optional_text(item.get("caption"), label="Snapshot caption", max_length=240)
        snapshot_key = _validate_required_text(item.get("snapshot_key"), label="Snapshot key", max_length=120)
        snapshots.append(
            {
                "artifact_name": f"movement_snapshot_{index:02d}.png",
                "caption": caption,
                "content": content,
                "content_type": "image/png",
                "snapshot_key": snapshot_key,
            }
        )
    return snapshots


def _report_snapshot_inputs(snapshots: list[dict]) -> tuple[list[dict], list[dict]]:
    parameters = []
    attachments = []
    for snapshot in snapshots:
        artifact_name = snapshot["artifact_name"]
        parameters.append(
            {
                "artifact_name": artifact_name,
                "attachment_name": artifact_name,
                "caption": snapshot["caption"],
                "snapshot_key": snapshot["snapshot_key"],
            }
        )
        attachments.append(
            {
                "logical_name": artifact_name,
                "content": snapshot["content"],
                "content_type": snapshot["content_type"],
            }
        )
    return parameters, attachments


def _validate_snapshot_windows(value: object) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid snapshot windows payload")
    cleaned = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("Invalid snapshot windows payload")
        cleaned.append(
            {
                "snapshot_key": _validate_required_text(item.get("snapshot_key"), label="Snapshot key", max_length=120),
                "snapshot_kind": _validate_optional_text(item.get("snapshot_kind"), label="Snapshot kind", max_length=40),
                "burst_id": _validate_optional_text(item.get("burst_id"), label="Burst ID", max_length=240),
                "caption": _validate_optional_text(item.get("caption"), label="Snapshot caption", max_length=240),
                "individual": _validate_optional_text(item.get("individual"), label="Individual", max_length=200),
                "set_name": _validate_optional_text(item.get("set_name"), label="Track", max_length=40),
                "issue_type": _validate_optional_text(item.get("issue_type"), label="Issue type", max_length=120),
                "issue_types": _validate_issue_ids(item.get("issue_types")),
                "anchor_row_ranges": compress_fix_keys(
                    _validate_fix_keys(item.get("anchor_fix_keys"), allow_empty=True)
                ),
                "report_row_ranges": compress_fix_keys(
                    _validate_fix_keys(item.get("report_fix_keys"), allow_empty=True)
                ),
                "start_fix_key": _validate_optional_text(item.get("start_fix_key"), label="Start fix key", max_length=240),
                "end_fix_key": _validate_optional_text(item.get("end_fix_key"), label="End fix key", max_length=240),
                "start_time_ms": item.get("start_time_ms"),
                "end_time_ms": item.get("end_time_ms"),
                "start_time_text": _validate_optional_text(item.get("start_time_text"), label="Start time", max_length=120),
                "end_time_text": _validate_optional_text(item.get("end_time_text"), label="End time", max_length=120),
                "window_fix_count": item.get("window_fix_count"),
            }
        )
    return cleaned


def _validate_report_type(value: object) -> str:
    if value is None:
        return "issue_first"
    if not isinstance(value, str):
        raise ValueError("Invalid report type")
    report_type = value.strip().lower()
    if report_type not in {"issue_first", "individual_profile"}:
        raise ValueError("Invalid report type")
    return report_type


def _validate_output_mode(value: object) -> str:
    if value is None:
        return "separate"
    if not isinstance(value, str):
        raise ValueError("Invalid output mode")
    output_mode = value.strip().lower()
    if output_mode not in {"combined", "separate"}:
        raise ValueError("Invalid output mode")
    return output_mode


def _normalize_individual_name(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Invalid individual")
    normalized = " ".join(value.strip().split())
    if not normalized or any(ord(char) < 32 for char in normalized):
        raise ValueError("Invalid individual")
    return normalized


def _validate_report_individuals(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid individuals")
    cleaned = []
    seen = set()
    for item in value:
        individual = _normalize_individual_name(item)
        if individual in seen:
            continue
        cleaned.append(individual)
        seen.add(individual)
    return sorted(cleaned)


def _slugify_individual_name(value: str) -> str:
    chars = []
    last_sep = False
    for char in str(value or "").strip().lower():
        if char.isalnum():
            chars.append(char)
            last_sep = False
            continue
        if not last_sep:
            chars.append("_")
            last_sep = True
    slug = "".join(chars).strip("_")
    return slug or "individual"


def _build_individual_report_artifacts(individuals: list[str]) -> list[dict]:
    artifacts = []
    used = {}
    for index, individual in enumerate(individuals, start=1):
        slug = _slugify_individual_name(individual)
        occurrence = used.get(slug, 0) + 1
        used[slug] = occurrence
        suffix = slug if occurrence == 1 else f"{slug}_{occurrence}"
        stem = f"movement_individual_report_{index:02d}_{suffix}"
        artifacts.append(
            {
                "individual": individual,
                "markdown_name": f"{stem}.md",
                "html_name": f"{stem}.html",
            }
        )
    return artifacts


def _reviewed_csv_artifact_name(logical_name: str) -> str:
    name = Path(logical_name).name
    stem = name[:-4] if name.lower().endswith(".csv") else Path(name).stem
    return f"{stem or 'movement'}_reviewed.csv"


def register_movement_routes(
    app: FastAPI,
    *,
    data_root: Path,
    allowed_families: set[str] | None = None,
    artifact_filter: ArtifactFilter | None = None,
    include_dev_routes: bool = True,
    overview_fix_limit: int | None = None,
    overview_series_points: int | None = None,
    background_anomaly_ranking: bool = False,
):
    data_root = data_root.resolve()
    configured_families = set(allowed_families or [])
    configured_overview_fix_limit = (
        None if overview_fix_limit is None else max(0, int(overview_fix_limit))
    )
    configured_overview_series_points = (
        None if overview_series_points is None else max(1, int(overview_series_points))
    )
    analysis_jobs: dict[str, dict] = {}
    analysis_jobs_lock = Lock()
    event_broker = StudyEventBroker()
    authentication_enabled = getattr(app.state, "auth_manager", None) is not None

    def prune_analysis_jobs() -> None:
        with analysis_jobs_lock:
            if len(analysis_jobs) < MAX_BACKGROUND_ANALYSIS_JOBS:
                return
            completed = sorted(
                (
                    job
                    for job in analysis_jobs.values()
                    if job.get("status") in {"completed", "failed"}
                ),
                key=lambda job: float(job.get("_updated_monotonic") or 0),
            )
            for job in completed[
                : max(1, len(analysis_jobs) - MAX_BACKGROUND_ANALYSIS_JOBS + 1)
            ]:
                analysis_jobs.pop(str(job.get("job_id") or ""), None)

    def run_analysis_job(job_id: str, study_dir: Path, payload: dict) -> None:
        with analysis_jobs_lock:
            job = analysis_jobs.get(job_id)
            if job is None:
                return
            job["status"] = "running"
            job["started_at"] = now_iso()
            job["_updated_monotonic"] = monotonic()
        try:
            result = create_analysis(study_dir, payload)
        except Exception as exc:
            with analysis_jobs_lock:
                job = analysis_jobs.get(job_id)
                if job is not None:
                    job["status"] = "failed"
                    job["error"] = str(exc) or "Analysis failed"
                    job["finished_at"] = now_iso()
                    job["_updated_monotonic"] = monotonic()
            return
        with analysis_jobs_lock:
            job = analysis_jobs.get(job_id)
            if job is not None:
                job["status"] = "completed"
                job["result"] = result
                job["finished_at"] = now_iso()
                job["_updated_monotonic"] = monotonic()

    def require_configured_family(family_name: str) -> str:
        family = validate_path_part(family_name, label="family")
        if configured_families and family not in configured_families:
            raise ValueError("Unknown movement family")
        return family

    def configured_study_dir(family_name: str, study_name: str) -> Path:
        return get_study_dir(data_root, require_configured_family(family_name), study_name)

    def configured_artifact_filter(artifact: dict) -> bool:
        logical_name = str(artifact.get("logical_name") or "")
        is_csv = logical_name.lower().endswith(".csv")
        return is_csv and (artifact_filter is None or artifact_filter(artifact))

    def current_actor(request: Request) -> Actor | None:
        return request_actor(request) if authentication_enabled else None

    def effective_user(request: Request, body: dict) -> str:
        actor = current_actor(request)
        return actor.display_name if actor is not None else normalize_user(body.get("user"))

    def require_read(request: Request, study_dir: Path) -> Actor | None:
        actor = current_actor(request)
        if actor is not None and not can_read_study(study_dir, actor):
            # A reviewer must not be able to enumerate unassigned study names.
            raise ReviewForbiddenError("Unknown study")
        return actor

    def study_event_key(family_name: str, study_name: str) -> str:
        return f"{family_name}/{study_name}"

    def state_event_payload(study_dir: Path, *, reason: str, actor: Actor | None) -> dict:
        state = load_review_state(study_dir)
        return {
            "event": "study_state_changed",
            "reason": reason,
            "review_revision": int(state.get("revision") or 0),
            "current_dataset_id": str(load_project_state(study_dir)["current_dataset_id"]),
            "actor": actor_payload(actor),
        }

    def publish_state_event(
        family_name: str,
        study_name: str,
        study_dir: Path,
        *,
        reason: str,
        actor: Actor | None,
    ) -> None:
        event_broker.publish(
            study_event_key(family_name, study_name),
            state_event_payload(study_dir, reason=reason, actor=actor),
        )

    def active_annotations(study_dir: Path) -> list[dict]:
        dataset_id = str(load_project_state(study_dir)["current_dataset_id"])
        return _load_dataset_review_annotations(study_dir, dataset_id=dataset_id)

    def admin_review_summary(
        family_name: str,
        study_name: str,
        *,
        include_individuals: bool,
    ) -> dict:
        study_dir = configured_study_dir(family_name, study_name)
        state = load_review_state(study_dir)
        review = active_review(state)
        if review is None:
            review = next(
                (
                    item
                    for item in reversed(state.get("reviews") or [])
                    if item.get("status") == "completed" and item.get("final_dataset_id")
                ),
                None,
            )
        current_dataset_id = str(load_project_state(study_dir)["current_dataset_id"])
        if review is None:
            return {
                "family": family_name,
                "study": study_name,
                "current_dataset_id": current_dataset_id,
                "review": None,
                "counts": {
                    "required": 0,
                    "reviewed": 0,
                    "undecided": 0,
                    "ok": 0,
                    "not_ok": 0,
                    "second_opinion": 0,
                },
                **({"individuals": []} if include_individuals else {}),
            }
        dataset_id = (
            current_dataset_id
            if review.get("status") == "active"
            else str(review.get("final_dataset_id") or current_dataset_id)
        )
        annotations = _load_dataset_review_annotations(study_dir, dataset_id=dataset_id)
        valid = valid_review_decisions(
            study_dir,
            review,
            annotations,
            current_dataset_id=dataset_id,
        )
        coverage = review_coverage(
            study_dir,
            review,
            annotations,
            current_dataset_id=dataset_id,
        )
        decision_counts = {"ok": 0, "not_ok": 0, "second_opinion": 0}
        for annotation in valid.values():
            decision = str(annotation.get("review_decision") or "")
            if decision in decision_counts:
                decision_counts[decision] += 1
        result = {
            "family": family_name,
            "study": study_name,
            "current_dataset_id": current_dataset_id,
            "review": {
                "review_id": str(review.get("review_id") or ""),
                "status": str(review.get("status") or ""),
                "reviewer": dict(review.get("reviewer") or {}),
                "assigned_at": str(review.get("assigned_at") or ""),
                "completed_at": str(review.get("completed_at") or ""),
            },
            "counts": {
                "required": int(coverage["required_count"]),
                "reviewed": int(coverage["reviewed_count"]),
                "undecided": int(coverage["remaining_count"]),
                **decision_counts,
            },
        }
        if include_individuals:
            required = sorted(set(valid) | set(coverage["remaining_individuals"]))
            result["individuals"] = [
                {
                    "individual": individual,
                    "review_decision": str(
                        (valid.get(individual) or {}).get("review_decision") or ""
                    ),
                    "reviewed_at": str(
                        (valid.get(individual) or {}).get("created_at")
                        or (valid.get(individual) or {}).get("reviewed_at")
                        or ""
                    ),
                }
                for individual in required
            ]
        return result

    def display_annotations(
        study_dir: Path,
        dataset_id: str,
        annotations: list[dict],
    ) -> list[dict]:
        review = active_review(load_review_state(study_dir))
        if review is None:
            return annotations
        try:
            valid = valid_review_decisions(
                study_dir,
                review,
                annotations,
                current_dataset_id=dataset_id,
            )
        except ReviewStateError:
            valid = {}
        allowed = {
            str(item.get("annotation_id") or "") for item in valid.values()
        }
        return [
            item
            for item in annotations
            if item.get("annotation_kind") != "individual_review"
            or str(item.get("annotation_id") or "") in allowed
        ]

    def prepare_analysis_payload(
        request: Request,
        study_dir: Path,
        payload: dict,
    ) -> dict:
        actor = current_actor(request)
        if actor is None:
            return payload
        review = authorize_analysis(study_dir, actor)
        updated = apply_actor(
            payload,
            actor,
            review_id=str((review or {}).get("review_id") or ""),
        )
        if review:
            review_annotations = active_annotations(study_dir)
            valid = valid_review_decisions(study_dir, review, review_annotations)
            parameters = dict(updated.get("parameters") or {})
            parameters["valid_individual_review_annotation_ids"] = sorted(
                str(item.get("annotation_id") or "") for item in valid.values()
            )
            parameters["second_opinion_individuals"] = sorted(
                {
                    individual
                    for individual, item in valid.items()
                    if item.get("review_decision") == "second_opinion"
                }
                | set(carryover_second_opinions(study_dir, review, review_annotations))
            )
            parameters["individual_review_decisions"] = {
                individual: str(item.get("review_decision") or "")
                for individual, item in sorted(valid.items())
            }
            updated["parameters"] = parameters
        return updated

    def prepare_step_payload(
        request: Request,
        study_dir: Path,
        body: dict,
        payload: dict,
        *,
        review_effect: str,
        review_impact: dict | None = None,
    ) -> tuple[dict, Callable[[], None] | None, Actor | None]:
        actor = current_actor(request)
        if actor is None:
            return payload, None, None
        expected_revision = body.get("expected_review_revision")
        review = authorize_persistent_change(
            study_dir,
            actor,
            expected_review_revision=expected_revision,
            review_effect=review_effect,
        )
        updated = apply_actor(
            payload,
            actor,
            review_id=str((review or {}).get("review_id") or ""),
        )
        parameters = dict(updated.get("parameters") or {})
        workflow = dict(parameters.get("workflow") or {})
        impact = dict(review_impact or {"scope": "none"})
        impact["actor"] = actor.as_dict()
        workflow.update(
            {
                "review_effect": review_effect,
                "review_impact": impact,
            }
        )
        parameters["workflow"] = workflow
        updated["parameters"] = parameters

        def preflight() -> None:
            authorize_persistent_change(
                study_dir,
                actor,
                expected_review_revision=expected_revision,
                review_effect=review_effect,
            )

        return updated, preflight, actor

    def combined_edit_profile(
        study_dir: Path,
        dataset_id: str,
        actor: Actor | None,
    ) -> dict:
        if actor is None:
            return build_edit_lock_profile(study_dir, dataset_id)
        workflow_profile = review_profile(
            study_dir,
            actor,
            active_annotations(study_dir),
        )
        blockers = []
        if not workflow_profile["capabilities"]["can_review"]:
            control = workflow_profile.get("editor_control")
            review = workflow_profile.get("review")
            if control:
                blockers.append(
                    {
                        "code": "editor_control",
                        "scope": "study",
                        "message": f"Editor control is held by {control.get('owner_display_name') or 'an editor'}.",
                        "owner": control.get("owner_display_name"),
                        "acquired_at": control.get("started_at"),
                        "expires_at": None,
                    }
                )
            elif actor.role == "editor" and review:
                blockers.append(
                    {
                        "code": "editor_control_required",
                        "scope": "study",
                        "message": "Take editor control before changing this active review.",
                        "owner": None,
                        "acquired_at": None,
                        "expires_at": None,
                    }
                )
            else:
                blockers.append(
                    {
                        "code": "assignment_required",
                        "scope": "study",
                        "message": "This study is not an active assignment for this reviewer.",
                        "owner": None,
                        "acquired_at": None,
                        "expires_at": None,
                    }
                )
        profile = build_edit_lock_profile(
            study_dir,
            dataset_id,
            additional_blockers=blockers,
        )
        profile.update(workflow_profile)
        if actor.role == "reviewer":
            try:
                require_history_change(
                    study_dir,
                    actor,
                    expected_review_revision=workflow_profile["review_revision"],
                )
            except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError):
                profile["capabilities"]["can_undo"] = False
            resume = dict(profile.get("resume") or {})
            if resume.get("allowed"):
                try:
                    require_history_change(
                        study_dir,
                        actor,
                        expected_review_revision=workflow_profile["review_revision"],
                        selected_dataset_id=dataset_id,
                    )
                except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
                    resume["allowed"] = False
                    resume["blocker_message"] = str(exc)
                profile["resume"] = resume
        # Core lineage blockers remain authoritative alongside workflow capabilities.
        profile["editable"] = bool(
            not profile.get("blockers")
            and workflow_profile["capabilities"]["can_review"]
        )
        return profile

    def require_history_change(
        study_dir: Path,
        actor: Actor,
        *,
        expected_review_revision: object,
        selected_dataset_id: str | None = None,
    ) -> None:
        review = authorize_persistent_change(
            study_dir,
            actor,
            expected_review_revision=expected_review_revision,
            review_effect="annotation_only",
        )
        if actor.role == "editor":
            return
        if review is None:
            raise ReviewForbiddenError("The reviewer has no active assignment")
        current_id = str(load_project_state(study_dir)["current_dataset_id"])
        baseline_id = str(review.get("baseline_dataset_id") or "")
        lineage = []
        cursor = current_id
        while cursor:
            lineage.append(cursor)
            if cursor == baseline_id:
                break
            cursor = str(load_dataset(study_dir, cursor).get("parent_dataset_id") or "")
        if not lineage or lineage[-1] != baseline_id:
            raise ReviewForbiddenError("Current history is outside the assigned review")
        lineage.reverse()
        target_id = selected_dataset_id or str(
            load_dataset(study_dir, current_id).get("parent_dataset_id") or ""
        )
        if target_id not in lineage or target_id == current_id and selected_dataset_id is None:
            raise ReviewForbiddenError("Reviewers cannot move before the assignment baseline")
        if selected_dataset_id is None and current_id == baseline_id:
            raise ReviewForbiddenError("Reviewers cannot undo before the assignment baseline")
        target_index = lineage.index(target_id)
        forward_ids = set(lineage[target_index + 1 :])
        steps = [
            step
            for step in list_history(study_dir)["steps"]
            if str(step.get("output_dataset_id") or "") in forward_ids
        ]
        for step in steps:
            actor_snapshot = step.get("actor") or {}
            workflow = (step.get("parameters") or {}).get("workflow") or {}
            if (
                actor_snapshot.get("user_id") != actor.user_id
                or workflow.get("review_id") != review.get("review_id")
                or workflow.get("review_effect") != "annotation_only"
            ):
                raise ReviewForbiddenError(
                    "An editor must change history containing editor, update, or earlier-review steps"
                )

    def parse_optional_int(raw_value: object, *, label: str) -> int | None:
        if raw_value in (None, ""):
            return None
        if isinstance(raw_value, bool):
            raise ValueError(f"Invalid {label}")
        try:
            return int(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid {label}") from exc

    def parse_optional_limit(raw_value: object) -> int | None:
        if raw_value in (None, ""):
            return None
        value = parse_optional_int(raw_value, label="limit")
        if value is None or value <= 0:
            raise ValueError("Invalid limit")
        return value

    def parse_burst_gap_seconds(raw_value: object) -> float:
        if raw_value in (None, ""):
            return DEFAULT_BURST_GAP_SECONDS
        try:
            value = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid burst_gap_seconds") from exc
        if not isfinite(value) or value <= 0:
            raise ValueError("Invalid burst_gap_seconds")
        return value

    def parse_optional_burst_gap_effective_seconds(raw_value: object) -> float | None:
        if raw_value in (None, ""):
            return None
        return parse_burst_gap_seconds(raw_value)

    def parse_burst_gap_mode(raw_value: object) -> str:
        if raw_value in (None, ""):
            return DEFAULT_BURST_GAP_MODE
        value = str(raw_value).strip().lower()
        if value not in {"manual", "quantile"}:
            raise ValueError("Invalid burst_gap_mode")
        return value

    def parse_burst_gap_quantile(raw_value: object) -> float:
        if raw_value in (None, ""):
            return DEFAULT_BURST_GAP_QUANTILE
        try:
            value = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid burst_gap_quantile") from exc
        if not isfinite(value) or value <= 0.0 or value > 1.0:
            raise ValueError("Invalid burst_gap_quantile")
        return value

    def parse_anomaly_feature_set(raw_value: object) -> str:
        value = str(raw_value or "movement_only").strip()
        if value == "movement_only":
            return value
        if value == "movement_plus_context" and include_dev_routes:
            return value
        raise ValueError("Invalid feature_set")

    def parse_optional_individual(raw_value: object) -> str:
        if raw_value in (None, ""):
            return ""
        return _normalize_individual_name(raw_value)

    def parse_optional_individuals(raw_values: list[str] | tuple[str, ...]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_value in raw_values:
            value = parse_optional_individual(raw_value)
            if not value or value in seen:
                continue
            normalized.append(value)
            seen.add(value)
        return normalized

    @app.get("/api/apps/movement/families")
    async def get_movement_families(request: Request):
        families = list_families(data_root)
        if configured_families:
            families = [item for item in families if item.get("name") in configured_families]
        actor = current_actor(request)
        if actor is not None and actor.role == "reviewer":
            visible = []
            for family in families:
                try:
                    studies = list_studies(data_root, str(family["name"]))
                except (ValueError, ProjectStateError):
                    continue
                count = sum(
                    1
                    for study in studies
                    if can_read_study(
                        configured_study_dir(str(family["name"]), str(study["name"])),
                        actor,
                    )
                )
                if count:
                    visible.append({**family, "study_count": count})
            families = visible
        return JSONResponse({"families": families})

    @app.get("/api/apps/movement/family/{family_name}/studies")
    async def get_movement_studies(family_name: str, request: Request):
        try:
            family = require_configured_family(family_name)
            actor = current_actor(request)
            studies = []
            for study in list_studies(data_root, family):
                study_dir = configured_study_dir(family, str(study["name"]))
                if actor is not None and not can_read_study(study_dir, actor):
                    continue
                review_state = load_review_state(study_dir)
                review = active_review(review_state)
                studies.append(
                    {
                        **study,
                        "review_revision": int(review_state.get("revision") or 0),
                        "review": {
                            "review_id": review.get("review_id"),
                            "status": review.get("status"),
                            "reviewer": review.get("reviewer"),
                        }
                        if review
                        else None,
                    }
                )
            return JSONResponse(
                {
                    "family": family,
                    "studies": studies,
                }
            )
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/state")
    async def get_movement_study_state(family_name: str, study_name: str, request: Request):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            require_read(request, study_dir)
            return JSONResponse(project_state_payload(study_dir))
        except ReviewForbiddenError as exc:
            return json_error(str(exc), 404)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/graph")
    async def get_movement_study_graph(family_name: str, study_name: str, request: Request):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            require_read(request, study_dir)
            return JSONResponse(graph_payload(study_dir))
        except ReviewForbiddenError as exc:
            return json_error(str(exc), 404)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get(
        "/api/apps/movement/family/{family_name}/study/{study_name}/edit-profile"
    )
    async def get_movement_study_edit_profile(
        family_name: str,
        study_name: str,
        dataset_id: str,
        request: Request,
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            actor = require_read(request, study_dir)
            normalized_dataset_id = validate_path_part(dataset_id, label="dataset")
            return JSONResponse(
                combined_edit_profile(study_dir, normalized_dataset_id, actor)
            )
        except ReviewForbiddenError as exc:
            return json_error(str(exc), 404)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/load")
    async def get_movement_study_load(family_name: str, study_name: str, request: Request):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            actor = require_read(request, study_dir)
            payload = await run_in_threadpool(
                _build_initial_study_payload,
                study_dir,
                configured_artifact_filter,
            )
            payload["edit_profile"] = combined_edit_profile(
                study_dir,
                str(payload["dataset_id"]),
                actor,
            )
            return JSONResponse(payload)
        except ReviewForbiddenError as exc:
            return json_error(str(exc), 404)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/dataset/{dataset_id}")
    async def get_movement_study_dataset(
        family_name: str, study_name: str, dataset_id: str, request: Request
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            require_read(request, study_dir)
            return JSONResponse(load_dataset(study_dir, dataset_id))
        except ReviewForbiddenError as exc:
            return json_error(str(exc), 404)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/dataset/{dataset_id}/overview")
    async def get_movement_study_overview(
        family_name: str,
        study_name: str,
        dataset_id: str,
        logical_name: str,
        request: Request,
        burst_gap_mode: str | None = None,
        burst_gap_seconds: float | None = None,
        burst_gap_quantile: float | None = None,
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            require_read(request, study_dir)
            _, artifact_path = get_dataset_artifact(study_dir, dataset_id, logical_name)
            annotations = _load_dataset_review_annotations(study_dir, dataset_id=dataset_id)
            confirmed_fix_keys, confirmed_individual_tracks = confirmed_exclusion_scopes(
                annotations,
                source_artifact=logical_name,
            )
            payload = await run_in_threadpool(
                build_movement_overview,
                artifact_path,
                confirmed_fix_keys=confirmed_fix_keys,
                confirmed_individual_tracks=confirmed_individual_tracks,
                burst_gap_mode=parse_burst_gap_mode(burst_gap_mode),
                burst_gap_seconds=parse_burst_gap_seconds(burst_gap_seconds),
                burst_gap_quantile=parse_burst_gap_quantile(burst_gap_quantile),
                overview_fix_limit=configured_overview_fix_limit,
                max_series_points=configured_overview_series_points,
            )
            visible_annotations = display_annotations(study_dir, dataset_id, annotations)
            payload = apply_review_annotations(
                payload,
                visible_annotations,
                source_artifact=logical_name,
            )
            return JSONResponse(
                apply_review_annotation_counts(
                    payload,
                    artifact_path,
                    visible_annotations,
                    source_artifact=logical_name,
                )
            )
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/dataset/{dataset_id}/fixes")
    async def get_movement_study_fixes(
        family_name: str,
        study_name: str,
        dataset_id: str,
        request: Request,
        logical_name: str,
        individual: str = "",
        start_ms: int | None = None,
        end_ms: int | None = None,
        review_status: str = "",
        limit: int | None = None,
        burst_gap_mode: str | None = None,
        burst_gap_seconds: float | None = None,
        burst_gap_quantile: float | None = None,
        burst_gap_effective_seconds: float | None = None,
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            require_read(request, study_dir)
            _, artifact_path = get_dataset_artifact(study_dir, dataset_id, logical_name)
            individuals = parse_optional_individuals(request.query_params.getlist("individuals"))
            if not individuals:
                single_individual = parse_optional_individual(individual)
                if single_individual:
                    individuals = [single_individual]
            normalized_review_status = str(review_status or "").strip().lower()
            if normalized_review_status not in {"", "reviewed", "suspected", "confirmed"}:
                raise ValueError("Invalid review status")
            requested_limit = parse_optional_limit(limit) if limit not in (None, "") else DEFAULT_FIX_LIMIT
            annotations = _load_dataset_review_annotations(study_dir, dataset_id=dataset_id)
            annotation_fix_keys, annotation_individuals = _review_annotation_candidates(
                annotations,
                logical_name=logical_name,
            )
            confirmed_fix_keys, confirmed_individual_tracks = confirmed_exclusion_scopes(
                annotations,
                source_artifact=logical_name,
            )
            payload = await run_in_threadpool(
                build_movement_fixes,
                artifact_path,
                individuals=individuals or None,
                additional_review_fix_keys=annotation_fix_keys if normalized_review_status else None,
                additional_review_individuals=annotation_individuals if normalized_review_status else None,
                confirmed_fix_keys=confirmed_fix_keys,
                confirmed_individual_tracks=confirmed_individual_tracks,
                start_ms=parse_optional_int(start_ms, label="start_ms"),
                end_ms=parse_optional_int(end_ms, label="end_ms"),
                review_status=normalized_review_status,
                limit=None if normalized_review_status else requested_limit,
                burst_gap_mode=parse_burst_gap_mode(burst_gap_mode),
                burst_gap_seconds=parse_burst_gap_seconds(burst_gap_seconds),
                burst_gap_quantile=parse_burst_gap_quantile(burst_gap_quantile),
                burst_gap_effective_seconds=parse_optional_burst_gap_effective_seconds(
                    burst_gap_effective_seconds
                ),
            )
            visible_annotations = display_annotations(study_dir, dataset_id, annotations)
            payload = apply_review_annotations(
                payload,
                visible_annotations,
                source_artifact=logical_name,
            )
            payload = apply_review_annotation_counts(
                payload,
                artifact_path,
                visible_annotations,
                source_artifact=logical_name,
            )
            if normalized_review_status:
                payload = _filter_review_status_payload(
                    payload,
                    review_status=normalized_review_status,
                    limit=requested_limit,
                )
            return JSONResponse(payload)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/dataset/{dataset_id}/summary")
    async def get_movement_study_summary(
        family_name: str,
        study_name: str,
        dataset_id: str,
        logical_name: str,
        request: Request,
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            require_read(request, study_dir)
            _, artifact_path = get_dataset_artifact(study_dir, dataset_id, logical_name)
            annotations = _load_dataset_review_annotations(study_dir, dataset_id=dataset_id)
            confirmed_fix_keys, confirmed_individual_tracks = confirmed_exclusion_scopes(
                annotations,
                source_artifact=logical_name,
            )
            payload = await run_in_threadpool(
                build_movement_summary,
                artifact_path,
                confirmed_fix_keys=confirmed_fix_keys,
                confirmed_individual_tracks=confirmed_individual_tracks,
            )
            visible_annotations = display_annotations(study_dir, dataset_id, annotations)
            payload = apply_review_annotations(
                payload,
                visible_annotations,
                source_artifact=logical_name,
            )
            return JSONResponse(
                apply_review_annotation_counts(
                    payload,
                    artifact_path,
                    visible_annotations,
                    source_artifact=logical_name,
                )
            )
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/analyses")
    async def get_movement_study_analyses(
        family_name: str,
        study_name: str,
        dataset_id: str,
        logical_name: str,
        request: Request,
        burst_gap_mode: str | None = None,
        burst_gap_seconds: float | None = None,
        burst_gap_quantile: float | None = None,
        feature_set: str = "movement_only",
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            require_read(request, study_dir)
            normalized_dataset_id = validate_path_part(dataset_id, label="dataset")
            normalized_logical_name = validate_path_part(logical_name, label="artifact")
            get_dataset_artifact(study_dir, normalized_dataset_id, normalized_logical_name)
            return JSONResponse(
                await run_in_threadpool(
                    build_movement_analysis_history,
                    study_dir,
                    dataset_id=normalized_dataset_id,
                    logical_name=normalized_logical_name,
                    burst_gap_mode=parse_burst_gap_mode(burst_gap_mode),
                    burst_gap_seconds=parse_burst_gap_seconds(burst_gap_seconds),
                    burst_gap_quantile=parse_burst_gap_quantile(burst_gap_quantile),
                    feature_set=parse_anomaly_feature_set(feature_set),
                )
            )
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/analysis/{analysis_id}/artifact/{logical_name}")
    async def get_movement_analysis_artifact(
        family_name: str,
        study_name: str,
        analysis_id: str,
        logical_name: str,
        request: Request,
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            require_read(request, study_dir)
            analysis_dir = project_paths(study_dir)["analyses"] / validate_path_part(analysis_id, label="analysis")
            artifact_name = validate_path_part(logical_name, label="artifact")
            artifact_path = (analysis_dir / "outputs" / artifact_name).resolve()
            if study_dir.resolve() not in artifact_path.parents:
                raise ProjectStateError("Invalid artifact path")
            if not artifact_path.exists() or not artifact_path.is_file():
                raise ProjectStateError("Unknown artifact")
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)
        return FileResponse(artifact_path, media_type=media_type_for_path(artifact_path))

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/actions/run-burst-anomaly-ranking")
    async def post_movement_run_burst_anomaly_ranking(
        family_name: str,
        study_name: str,
        request: Request,
        background_tasks: BackgroundTasks,
    ):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = configured_study_dir(family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            dataset = load_dataset(study_dir, dataset_id)
            get_dataset_artifact(study_dir, dataset_id, logical_name)
            feature_set = parse_anomaly_feature_set(body.get("feature_set"))
            feature_set_label = (
                "movement + OSM context"
                if feature_set == "movement_plus_context"
                else "movement only"
            )
            user = effective_user(request, body)
            payload = {
                "user": user,
                "title": f"Rank automatic movement bursts ({feature_set_label}) for {logical_name}",
                "kind": "python",
                "script": BURST_ANOMALY_ANALYSIS_SCRIPT,
                "dataset_id": dataset_id,
                "input_artifacts": _movement_analysis_input_names(dataset, logical_name),
                "output_artifacts": ["burst_anomaly_ranking.json"],
                "parameters": {
                    "app": "movement",
                    "action": "run_burst_anomaly_ranking",
                    "target_artifact": logical_name,
                    "dataset_id": dataset_id,
                    "burst_gap_mode": parse_burst_gap_mode(body.get("burst_gap_mode")),
                    "burst_gap_seconds": parse_burst_gap_seconds(body.get("burst_gap_seconds")),
                    "burst_gap_quantile": parse_burst_gap_quantile(body.get("burst_gap_quantile")),
                    "feature_set": feature_set,
                    "user": user,
                },
            }
            payload = prepare_analysis_payload(request, study_dir, payload)
            if background_anomaly_ranking:
                prune_analysis_jobs()
                job_id = make_id("analysis_job")
                job = {
                    "job_id": job_id,
                    "family_name": family_name,
                    "study_name": study_name,
                    "status": "queued",
                    "created_at": now_iso(),
                    "actor": dict(payload.get("actor") or {}),
                    "review_id": str((payload.get("parameters") or {}).get("review_id") or ""),
                    "_updated_monotonic": monotonic(),
                }
                with analysis_jobs_lock:
                    analysis_jobs[job_id] = job
                background_tasks.add_task(
                    run_analysis_job,
                    job_id,
                    study_dir,
                    payload,
                )
                return JSONResponse(
                    {
                        key: value
                        for key, value in job.items()
                        if not key.startswith("_")
                    },
                    status_code=202,
                )
            return JSONResponse(create_analysis(study_dir, payload))
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.get(
        "/api/apps/movement/family/{family_name}/study/{study_name}/analysis-jobs/{job_id}"
    )
    async def get_movement_analysis_job(
        family_name: str,
        study_name: str,
        job_id: str,
        request: Request,
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            require_read(request, study_dir)
            normalized_job_id = validate_path_part(job_id, label="analysis job")
            with analysis_jobs_lock:
                job = dict(analysis_jobs.get(normalized_job_id) or {})
            if (
                not job
                or job.get("family_name") != family_name
                or job.get("study_name") != study_name
            ):
                raise ProjectStateError("Unknown analysis job")
            return JSONResponse(
                {
                    key: value
                    for key, value in job.items()
                    if not key.startswith("_")
                }
            )
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/admin/review-summary")
    async def get_movement_admin_review_summary(
        request: Request,
        family: str = "",
        study: str = "",
        include_individuals: bool = False,
    ):
        actor = current_actor(request)
        if actor is None or actor.role != "editor":
            return json_error("Only editors can view review summaries", 403)
        if include_individuals and (not family or not study):
            return json_error("family and study are required for individual details", 400)
        if bool(family) != bool(study):
            return json_error("family and study must be provided together", 400)
        try:
            if family and study:
                family_name = require_configured_family(family)
                studies = [
                    admin_review_summary(
                        family_name,
                        study,
                        include_individuals=include_individuals,
                    )
                ]
            else:
                studies = []
                families = list_families(data_root)
                if configured_families:
                    families = [
                        item for item in families
                        if item.get("name") in configured_families
                    ]
                for family_item in families:
                    family_name = str(family_item["name"])
                    for study_item in list_studies(data_root, family_name):
                        studies.append(
                            admin_review_summary(
                                family_name,
                                str(study_item["name"]),
                                include_individuals=False,
                            )
                        )
            return JSONResponse(
                {"studies": studies},
                headers={"Cache-Control": "no-store"},
            )
        except (ValueError, ProjectStateError, ReviewStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/reviewers")
    async def get_movement_reviewers(request: Request):
        actor = current_actor(request)
        if actor is None or actor.role != "editor":
            return json_error("Only editors can list reviewers", 403)
        return JSONResponse(
            {"reviewers": app.state.auth_manager.list_reviewers()},
            headers={"Cache-Control": "no-store"},
        )

    @app.post(
        "/api/apps/movement/family/{family_name}/study/{study_name}/review/assign"
    )
    async def post_movement_assign_review(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            actor = current_actor(request)
            if actor is None:
                raise ReviewForbiddenError("Authentication is required")
            study_dir = configured_study_dir(family_name, study_name)
            reviewer = app.state.auth_manager.actor_by_id(str(body.get("reviewer_user_id") or ""))
            if reviewer is None:
                raise ReviewStateError("Unknown reviewer")
            current_id = str(load_project_state(study_dir)["current_dataset_id"])
            dataset = load_dataset(study_dir, current_id)
            logical_name = str(body.get("logical_name") or "").strip()
            if logical_name:
                logical_name = validate_path_part(logical_name, label="artifact")
                artifact, artifact_path = get_dataset_artifact(study_dir, current_id, logical_name)
                if not configured_artifact_filter(artifact):
                    raise ReviewStateError("Artifact is not reviewable")
            else:
                candidates = [
                    item for item in dataset.get("artifacts") or [] if configured_artifact_filter(item)
                ]
                if not candidates:
                    raise ReviewStateError("Selected dataset has no reviewable movement artifact")
                logical_name = str(candidates[0]["logical_name"])
                _, artifact_path = get_dataset_artifact(study_dir, current_id, logical_name)
            individuals = await run_in_threadpool(list_movement_individuals, artifact_path)
            result = assign_review(
                study_dir,
                editor=actor,
                reviewer=reviewer,
                expected_current_dataset_id=str(body.get("expected_current_dataset_id") or ""),
                expected_review_revision=body.get("expected_review_revision"),
                individuals=individuals,
            )
            publish_state_event(
                family_name,
                study_name,
                study_dir,
                reason="review_assigned",
                actor=actor,
            )
            result["edit_profile"] = combined_edit_profile(study_dir, current_id, actor)
            return JSONResponse(result)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post(
        "/api/apps/movement/family/{family_name}/study/{study_name}/review/complete"
    )
    async def post_movement_complete_review(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            actor = current_actor(request)
            if actor is None:
                raise ReviewForbiddenError("Authentication is required")
            study_dir = configured_study_dir(family_name, study_name)
            result = complete_review(
                study_dir,
                actor=actor,
                expected_current_dataset_id=str(body.get("expected_current_dataset_id") or ""),
                expected_review_revision=body.get("expected_review_revision"),
                annotations=active_annotations(study_dir),
                reason=str(body.get("reason") or ""),
            )
            publish_state_event(
                family_name,
                study_name,
                study_dir,
                reason="review_completed",
                actor=actor,
            )
            return JSONResponse(result)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)

    @app.post(
        "/api/apps/movement/family/{family_name}/study/{study_name}/review/cancel"
    )
    async def post_movement_cancel_review(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            actor = current_actor(request)
            if actor is None:
                raise ReviewForbiddenError("Authentication is required")
            study_dir = configured_study_dir(family_name, study_name)
            result = cancel_review(
                study_dir,
                editor=actor,
                expected_current_dataset_id=str(body.get("expected_current_dataset_id") or ""),
                expected_review_revision=body.get("expected_review_revision"),
                reason=str(body.get("reason") or ""),
            )
            publish_state_event(
                family_name,
                study_name,
                study_dir,
                reason="review_cancelled",
                actor=actor,
            )
            return JSONResponse(result)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)

    @app.post(
        "/api/apps/movement/family/{family_name}/study/{study_name}/editor-control/start"
    )
    async def post_movement_start_editor_control(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            actor = current_actor(request)
            if actor is None:
                raise ReviewForbiddenError("Authentication is required")
            study_dir = configured_study_dir(family_name, study_name)
            result = start_editor_control(
                study_dir,
                editor=actor,
                expected_current_dataset_id=str(body.get("expected_current_dataset_id") or ""),
                expected_review_revision=body.get("expected_review_revision"),
                reason=str(body.get("reason") or ""),
            )
            publish_state_event(
                family_name,
                study_name,
                study_dir,
                reason="editor_control_started",
                actor=actor,
            )
            return JSONResponse(result)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)

    @app.post(
        "/api/apps/movement/family/{family_name}/study/{study_name}/editor-control/takeover"
    )
    async def post_movement_takeover_editor_control(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            actor = current_actor(request)
            if actor is None:
                raise ReviewForbiddenError("Authentication is required")
            study_dir = configured_study_dir(family_name, study_name)
            result = start_editor_control(
                study_dir,
                editor=actor,
                expected_current_dataset_id=str(body.get("expected_current_dataset_id") or ""),
                expected_review_revision=body.get("expected_review_revision"),
                reason=str(body.get("reason") or ""),
                takeover=True,
            )
            publish_state_event(
                family_name,
                study_name,
                study_dir,
                reason="editor_control_taken_over",
                actor=actor,
            )
            return JSONResponse(result)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)

    @app.post(
        "/api/apps/movement/family/{family_name}/study/{study_name}/editor-control/finish"
    )
    async def post_movement_finish_editor_control(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            actor = current_actor(request)
            if actor is None:
                raise ReviewForbiddenError("Authentication is required")
            study_dir = configured_study_dir(family_name, study_name)
            result = finish_editor_control(
                study_dir,
                editor=actor,
                expected_current_dataset_id=str(body.get("expected_current_dataset_id") or ""),
                expected_review_revision=body.get("expected_review_revision"),
                reason=str(body.get("reason") or ""),
            )
            publish_state_event(
                family_name,
                study_name,
                study_dir,
                reason="editor_control_released",
                actor=actor,
            )
            return JSONResponse(result)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)

    @app.get(
        "/api/apps/movement/family/{family_name}/study/{study_name}/events"
    )
    async def get_movement_study_events(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            actor = require_read(request, study_dir)
        except ReviewForbiddenError as exc:
            return json_error(str(exc), 404)

        async def stream():
            key = study_event_key(family_name, study_name)
            async with event_broker.subscribe(key) as queue:
                snapshot = state_event_payload(
                    study_dir,
                    reason="connected",
                    actor=actor,
                )
                yield f"event: study_state_changed\ndata: {json.dumps(snapshot)}\n\n"
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=60)
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
                        continue
                    yield f"event: study_state_changed\ndata: {json.dumps(event)}\n\n"

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-store",
                "X-Accel-Buffering": "no",
            },
        )

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/undo")
    async def post_movement_study_undo(
        family_name: str,
        study_name: str,
        request: Request,
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            body = await parse_json_body(request)
            expected_current_dataset_id = (
                validate_path_part(
                    body.get("expected_current_dataset_id"),
                    label="expected current dataset",
                )
                if body
                else str(load_project_state(study_dir)["current_dataset_id"])
            )
            actor = current_actor(request)
            preflight = None
            if actor is not None:
                expected_review_revision = (body or {}).get("expected_review_revision")

                def preflight() -> None:
                    require_history_change(
                        study_dir,
                        actor,
                        expected_review_revision=expected_review_revision,
                    )

            result = undo_guarded(
                study_dir,
                expected_current_dataset_id=expected_current_dataset_id,
                preflight=preflight,
            )
            publish_state_event(
                family_name,
                study_name,
                study_dir,
                reason="dataset_head_undone",
                actor=actor,
            )
            return JSONResponse(result)
        except EditConflictError as exc:
            return _edit_conflict_response(exc)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/resume")
    async def post_movement_study_resume(
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
            expected_current_dataset_id = validate_path_part(
                body.get("expected_current_dataset_id"),
                label="expected current dataset",
            )
            resume_token = str(body.get("resume_token") or "").strip()
            if not resume_token:
                raise ValueError("Resume token is required")
            actor = current_actor(request)
            preflight = None
            if actor is not None:
                expected_review_revision = body.get("expected_review_revision")

                def preflight() -> None:
                    require_history_change(
                        study_dir,
                        actor,
                        expected_review_revision=expected_review_revision,
                        selected_dataset_id=dataset_id,
                    )

            result = resume_from_dataset(
                study_dir,
                selected_dataset_id=dataset_id,
                expected_current_dataset_id=expected_current_dataset_id,
                resume_token=resume_token,
                user=actor.display_name if actor is not None else body.get("user"),
                preflight=preflight,
            )
            publish_state_event(
                family_name,
                study_name,
                study_dir,
                reason="dataset_history_resumed",
                actor=actor,
            )
            return JSONResponse(result)
        except EditConflictError as exc:
            return _edit_conflict_response(exc)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/actions/annotate-scope")
    async def post_movement_annotate_scope(family_name: str, study_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = configured_study_dir(family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            dataset = load_dataset(study_dir, dataset_id)
            get_dataset_artifact(study_dir, dataset_id, logical_name)
            raw_scope = body.get("scope")
            if not isinstance(raw_scope, dict):
                raise ValueError("Review scope is required")
            scope_kind = str(raw_scope.get("kind") or "").strip().lower()
            if scope_kind not in {"fix", "segment", "burst", "bursts", "individual", "filter"}:
                raise ValueError("Invalid review scope")
            scope: dict[str, object] = {"kind": scope_kind}
            if scope_kind in {"fix", "segment"}:
                selected_fix_keys = _validate_fix_keys(raw_scope.get("fix_keys"))
                scope["row_ranges"] = compress_fix_keys(selected_fix_keys)
                scope["fix_count"] = len(selected_fix_keys)
                if scope_kind == "segment":
                    scope["start_fix_key"] = _validate_fix_key(
                        raw_scope.get("start_fix_key"),
                        label="Start fix key",
                    )
                    scope["end_fix_key"] = _validate_fix_key(
                        raw_scope.get("end_fix_key"),
                        label="End fix key",
                    )
                    scope["individual"] = _validate_optional_text(
                        raw_scope.get("individual"),
                        label="Individual",
                        max_length=240,
                    )
                    scope["set_name"] = _validate_optional_text(
                        raw_scope.get("set_name"),
                        label="Set name",
                        max_length=40,
                    )
                    selection_method = _validate_optional_text(
                        raw_scope.get("selection_method"),
                        label="Selection method",
                        max_length=40,
                    )
                    if selection_method and selection_method not in {"map_double_click", "table_shift_click"}:
                        raise ValueError("Invalid segment selection method")
                    scope["selection_method"] = selection_method
            elif scope_kind == "individual":
                scope["individual"] = _normalize_individual_name(raw_scope.get("individual"))
                scope["set_name"] = _validate_optional_text(
                    raw_scope.get("set_name"),
                    label="Set name",
                    max_length=40,
                )
            elif scope_kind == "filter":
                scope["filter"] = _validate_filter_scope(raw_scope.get("filter"))
            elif scope_kind == "burst":
                scope["burst_id"] = _validate_required_text(
                    raw_scope.get("burst_id"),
                    label="Burst id",
                    max_length=240,
                )
            else:
                raw_burst_ids = raw_scope.get("burst_ids")
                if not isinstance(raw_burst_ids, list):
                    raise ValueError("Burst ids must be a list")
                burst_ids = list(
                    dict.fromkeys(
                        _validate_required_text(
                            value,
                            label="Burst id",
                            max_length=240,
                        )
                        for value in raw_burst_ids
                    )
                )
                if not burst_ids:
                    raise ValueError("Choose at least one burst")
                scope["burst_ids"] = burst_ids

            status = _validate_status(body.get("status"))
            if status != "suspected":
                raise ValueError("Use the confirm-issues action to confirm an existing suspected issue")
            issue_type = _validate_required_text(body.get("issue_type"), label="Issue type", max_length=120)
            comment = _validate_required_text(
                body.get("comment", body.get("issue_note")),
                label="Comment",
                max_length=1200,
            )
            owner_question = _validate_optional_text(
                body.get("owner_question"),
                label="Owner question",
                max_length=600,
            )
            source_analysis_id = _validate_optional_text(
                body.get("source_analysis_id"),
                label="Source analysis id",
                max_length=120,
            )
            raw_origin = str(body.get("origin") or "").strip().lower()
            if not raw_origin:
                raw_origin = "threshold" if body.get("issue_field") or body.get("issue_threshold") else "manual"
            if raw_origin not in {"manual", "threshold", "algorithm"}:
                raise ValueError("Invalid annotation origin")
            if scope_kind == "filter":
                raw_origin = "threshold"
            user = effective_user(request, body)
            input_artifacts = [logical_name]
            if any(
                artifact.get("logical_name") == "movement_review_annotations.json"
                for artifact in dataset.get("artifacts", [])
            ):
                input_artifacts.append("movement_review_annotations.json")
            scope_title = (
                f"{len(scope['burst_ids'])} bursts"
                if scope_kind == "bursts"
                else "dataset filter"
                if scope_kind == "filter"
                else scope_kind
            )
            payload = {
                "user": user,
                "title": f"Mark {scope_title} as {status} in {logical_name}",
                "kind": "python",
                "script": ANNOTATE_SCOPE_SCRIPT,
                "parameters": {
                    "app": "movement",
                    "action": "annotate_scope",
                    "target_artifact": logical_name,
                    "dataset_id": dataset_id,
                    "scope": scope,
                    "status": status,
                    "origin": raw_origin,
                    "issue_type": issue_type,
                    "issue_field": _validate_optional_text(
                        body.get("issue_field"),
                        label="Issue field",
                        max_length=240,
                    ),
                    "issue_threshold": _validate_optional_text(
                        body.get("issue_threshold"),
                        label="Issue threshold",
                        max_length=600,
                    ),
                    "comment": comment,
                    "owner_question": owner_question,
                    "source_analysis_id": source_analysis_id,
                    "burst_gap_mode": parse_burst_gap_mode(body.get("burst_gap_mode")),
                    "burst_gap_seconds": parse_burst_gap_seconds(body.get("burst_gap_seconds")),
                    "burst_gap_quantile": parse_burst_gap_quantile(body.get("burst_gap_quantile")),
                    "user": user,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": input_artifacts,
                "output_artifacts": ["movement_review_annotations.json"],
                "set_as_head": True,
            }
            payload, preflight, actor = prepare_step_payload(
                request,
                study_dir,
                body,
                payload,
                review_effect="annotation_only",
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
            publish_state_event(
                family_name, study_name, study_dir, reason="dataset_head_changed", actor=actor
            )
            return JSONResponse(result)
        except EditLockedError as exc:
            return _edit_locked_response(exc)
        except EditConflictError as exc:
            return _edit_conflict_response(exc)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/actions/confirm-issues")
    async def post_movement_confirm_issues(family_name: str, study_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = configured_study_dir(family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            dataset = load_dataset(study_dir, dataset_id)
            get_dataset_artifact(study_dir, dataset_id, logical_name)
            confirmations = _validate_confirmations(body.get("confirmations"))
            note = _validate_optional_text(
                body.get("note"),
                label="Confirmation note",
                max_length=1200,
            )
            input_artifacts = [logical_name]
            if any(
                artifact.get("logical_name") == "movement_review_annotations.json"
                for artifact in dataset.get("artifacts", [])
            ):
                input_artifacts.append("movement_review_annotations.json")
            user = effective_user(request, body)
            payload = {
                "user": user,
                "title": f"Confirm {sum(item['fix_count'] for item in confirmations)} suspected fix(es) in {logical_name}",
                "kind": "python",
                "script": CONFIRM_ISSUES_SCRIPT,
                "parameters": {
                    "app": "movement",
                    "action": "confirm_issues",
                    "target_artifact": logical_name,
                    "dataset_id": dataset_id,
                    "confirmations": confirmations,
                    "note": note,
                    "user": user,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": input_artifacts,
                "output_artifacts": ["movement_review_annotations.json"],
                "set_as_head": True,
            }
            payload, preflight, actor = prepare_step_payload(
                request,
                study_dir,
                body,
                payload,
                review_effect="annotation_only",
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
            publish_state_event(
                family_name, study_name, study_dir, reason="dataset_head_changed", actor=actor
            )
            return JSONResponse(result)
        except EditLockedError as exc:
            return _edit_locked_response(exc)
        except EditConflictError as exc:
            return _edit_conflict_response(exc)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/actions/dismiss-issues")
    async def post_movement_dismiss_issues(family_name: str, study_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = configured_study_dir(family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            dataset = load_dataset(study_dir, dataset_id)
            get_dataset_artifact(study_dir, dataset_id, logical_name)
            dismissals = _validate_dismissals(body.get("dismissals"))
            note = _validate_optional_text(
                body.get("note"),
                label="Dismissal note",
                max_length=1200,
            )
            input_artifacts = [logical_name]
            if any(
                artifact.get("logical_name") == "movement_review_annotations.json"
                for artifact in dataset.get("artifacts", [])
            ):
                input_artifacts.append("movement_review_annotations.json")
            user = effective_user(request, body)
            dismissed_fix_count = sum(item["fix_count"] for item in dismissals)
            payload = {
                "user": user,
                "title": f"Dismiss suspicion for {dismissed_fix_count} fix(es) in {logical_name}",
                "kind": "python",
                "script": DISMISS_ISSUES_SCRIPT,
                "parameters": {
                    "app": "movement",
                    "action": "dismiss_issues",
                    "target_artifact": logical_name,
                    "dataset_id": dataset_id,
                    "dismissals": dismissals,
                    "note": note,
                    "user": user,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": input_artifacts,
                "output_artifacts": ["movement_review_annotations.json"],
                "set_as_head": True,
            }
            payload, preflight, actor = prepare_step_payload(
                request,
                study_dir,
                body,
                payload,
                review_effect="annotation_only",
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
            publish_state_event(
                family_name, study_name, study_dir, reason="dataset_head_changed", actor=actor
            )
            return JSONResponse(result)
        except EditLockedError as exc:
            return _edit_locked_response(exc)
        except EditConflictError as exc:
            return _edit_conflict_response(exc)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post(
        "/api/apps/movement/family/{family_name}/study/{study_name}/actions/review-individuals"
    )
    async def post_movement_review_individuals(
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
            get_dataset_artifact(study_dir, dataset_id, logical_name)
            decisions = _validate_individual_review_decisions(body.get("decisions"))
            user = effective_user(request, body)
            input_artifacts = [logical_name]
            if any(
                artifact.get("logical_name") == "movement_review_annotations.json"
                for artifact in dataset.get("artifacts", [])
            ):
                input_artifacts.append("movement_review_annotations.json")
            payload = {
                "user": user,
                "title": (
                    f"Record review decisions for {len(decisions)} individual(s) "
                    f"in {logical_name}"
                ),
                "kind": "python",
                "script": REVIEW_INDIVIDUALS_SCRIPT,
                "parameters": {
                    "app": "movement",
                    "action": "review_individuals",
                    "target_artifact": logical_name,
                    "dataset_id": dataset_id,
                    "decisions": decisions,
                    "user": user,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": input_artifacts,
                "output_artifacts": ["movement_review_annotations.json"],
                "set_as_head": True,
            }
            payload, preflight, actor = prepare_step_payload(
                request,
                study_dir,
                body,
                payload,
                review_effect="annotation_only",
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
            publish_state_event(
                family_name, study_name, study_dir, reason="dataset_head_changed", actor=actor
            )
            return JSONResponse(result)
        except EditLockedError as exc:
            return _edit_locked_response(exc)
        except EditConflictError as exc:
            return _edit_conflict_response(exc)
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/actions/generate-report")
    async def post_movement_generate_report(family_name: str, study_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = configured_study_dir(family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            dataset = load_dataset(study_dir, dataset_id)
            report_input_artifacts = [logical_name]
            if any(
                artifact.get("logical_name") == "movement_review_annotations.json"
                for artifact in dataset.get("artifacts", [])
            ):
                report_input_artifacts.append("movement_review_annotations.json")
            fix_keys = _validate_fix_keys(body.get("fix_keys"), allow_empty=True)
            fix_row_ranges = compress_fix_keys(fix_keys)
            issue_ids = _validate_issue_ids(body.get("issue_ids"))
            report_type = _validate_report_type(body.get("report_type"))
            individuals = _validate_report_individuals(body.get("individuals"))
            output_mode = _validate_output_mode(body.get("output_mode"))
            snapshot_windows = _validate_snapshot_windows(body.get("snapshot_windows"))
            if report_type == "issue_first" and not fix_keys and not issue_ids:
                raise ValueError("Select at least one issue or fix")
            if report_type == "individual_profile" and not individuals:
                raise ValueError("Select at least one individual")
            screenshot_mode = _validate_screenshot_mode(body.get("screenshot_mode"))
            snapshots = _validate_snapshots(body.get("snapshots"))
            snapshot_parameters, snapshot_attachments = _report_snapshot_inputs(snapshots)
            user = effective_user(request, body)
            individual_report_artifacts = _build_individual_report_artifacts(individuals)
            effective_output_mode = output_mode if len(individuals) > 1 else "combined"
            if report_type == "issue_first":
                output_artifacts = [
                    "movement_outlier_report.md",
                    "movement_outlier_report.html",
                    "movement_outlier_fixes.csv",
                ]
            elif effective_output_mode == "combined":
                output_artifacts = [
                    "movement_individual_reports.md",
                    "movement_individual_reports.html",
                ]
            else:
                output_artifacts = [
                    "movement_individual_report_index.md",
                    "movement_individual_report_index.html",
                ]
                for item in individual_report_artifacts:
                    output_artifacts.extend([item["markdown_name"], item["html_name"]])
            output_artifacts.extend(snapshot["artifact_name"] for snapshot in snapshots)

            payload = {
                "user": user,
                "title": (
                    f"Generate outlier report for {logical_name}"
                    if report_type == "issue_first"
                    else f"Generate individual profile report for {logical_name}"
                ),
                "kind": "python",
                "script": GENERATE_REPORT_SCRIPT,
                "dataset_id": dataset_id,
                "input_artifacts": report_input_artifacts,
                "input_attachments": snapshot_attachments,
                "output_artifacts": output_artifacts,
                "parameters": {
                    "app": "movement",
                    "action": "generate_report",
                    "report_type": report_type,
                    "output_mode": effective_output_mode,
                    "target_artifact": logical_name,
                    "fix_row_ranges": fix_row_ranges,
                    "issue_ids": issue_ids,
                    "individuals": individuals,
                    "snapshot_windows": snapshot_windows,
                    "screenshot_mode": screenshot_mode,
                    "snapshots": snapshot_parameters,
                    "individual_report_artifacts": individual_report_artifacts,
                    "user": user,
                },
            }
            payload = prepare_analysis_payload(request, study_dir, payload)
            return JSONResponse(create_analysis(study_dir, payload))
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/actions/export-reviewed-csv")
    async def post_movement_export_reviewed_csv(family_name: str, study_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = configured_study_dir(family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            dataset = load_dataset(study_dir, dataset_id)
            get_dataset_artifact(study_dir, dataset_id, logical_name)
            input_artifacts = [logical_name]
            if any(
                artifact.get("logical_name") == "movement_review_annotations.json"
                for artifact in dataset.get("artifacts", [])
            ):
                input_artifacts.append("movement_review_annotations.json")
            output_artifact = _reviewed_csv_artifact_name(logical_name)
            user = effective_user(request, body)
            payload = {
                "user": user,
                "title": f"Export reviewed movement CSV for {logical_name}",
                "kind": "python",
                "script": EXPORT_REVIEWED_CSV_SCRIPT,
                "dataset_id": dataset_id,
                "input_artifacts": input_artifacts,
                "output_artifacts": [output_artifact],
                "parameters": {
                    "app": "movement",
                    "action": "export_reviewed_csv",
                    "target_artifact": logical_name,
                    "output_artifact": output_artifact,
                    "dataset_id": dataset_id,
                    "user": user,
                },
            }
            payload = prepare_analysis_payload(request, study_dir, payload)
            return JSONResponse(create_analysis(study_dir, payload))
        except (ReviewForbiddenError, ReviewConflictError, ReviewLockedError, ReviewStateError) as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.get("/api/project/{project_name}/apps/movement/dataset/{dataset_id}/summary")
    async def get_movement_summary(
        project_name: str,
        dataset_id: str,
        logical_name: str,
        request: Request,
    ):
        try:
            actor = current_actor(request)
            if actor is not None and actor.role != "editor":
                raise ReviewForbiddenError("Editor role required")
            project_dir = get_project_dir(data_root, project_name)
            _, artifact_path = get_dataset_artifact(project_dir, dataset_id, logical_name)
            return JSONResponse(await run_in_threadpool(build_movement_summary, artifact_path))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.post("/api/project/{project_name}/apps/movement/actions/generate-report")
    async def post_generate_report(project_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            actor = current_actor(request)
            if actor is not None and actor.role != "editor":
                raise ReviewForbiddenError("Editor role required")
            project_dir = get_project_dir(data_root, project_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            dataset = load_dataset(project_dir, dataset_id)
            report_input_artifacts = [logical_name]
            if any(
                artifact.get("logical_name") == "movement_review_annotations.json"
                for artifact in dataset.get("artifacts", [])
            ):
                report_input_artifacts.append("movement_review_annotations.json")
            fix_keys = _validate_fix_keys(body.get("fix_keys"), allow_empty=True)
            fix_row_ranges = compress_fix_keys(fix_keys)
            issue_ids = _validate_issue_ids(body.get("issue_ids"))
            report_type = _validate_report_type(body.get("report_type"))
            individuals = _validate_report_individuals(body.get("individuals"))
            output_mode = _validate_output_mode(body.get("output_mode"))
            snapshot_windows = _validate_snapshot_windows(body.get("snapshot_windows"))
            if report_type == "issue_first" and not fix_keys and not issue_ids:
                raise ValueError("Select at least one issue or fix")
            if report_type == "individual_profile" and not individuals:
                raise ValueError("Select at least one individual")
            screenshot_mode = _validate_screenshot_mode(body.get("screenshot_mode"))
            snapshots = _validate_snapshots(body.get("snapshots"))
            snapshot_parameters, snapshot_attachments = _report_snapshot_inputs(snapshots)
            user = actor.display_name if actor is not None else body.get("user")
            individual_report_artifacts = _build_individual_report_artifacts(individuals)
            effective_output_mode = output_mode if len(individuals) > 1 else "combined"
            if report_type == "issue_first":
                output_artifacts = [
                    "movement_outlier_report.md",
                    "movement_outlier_report.html",
                    "movement_outlier_fixes.csv",
                ]
            elif effective_output_mode == "combined":
                output_artifacts = [
                    "movement_individual_reports.md",
                    "movement_individual_reports.html",
                ]
            else:
                output_artifacts = [
                    "movement_individual_report_index.md",
                    "movement_individual_report_index.html",
                ]
                for item in individual_report_artifacts:
                    output_artifacts.extend([item["markdown_name"], item["html_name"]])
            output_artifacts.extend(snapshot["artifact_name"] for snapshot in snapshots)

            payload = {
                "user": user,
                "title": (
                    f"Generate outlier report for {logical_name}"
                    if report_type == "issue_first"
                    else f"Generate individual profile report for {logical_name}"
                ),
                "kind": "python",
                "script": GENERATE_REPORT_SCRIPT,
                "dataset_id": dataset_id,
                "input_artifacts": report_input_artifacts,
                "input_attachments": snapshot_attachments,
                "output_artifacts": output_artifacts,
                "parameters": {
                    "app": "movement",
                    "action": "generate_report",
                    "report_type": report_type,
                    "output_mode": effective_output_mode,
                    "target_artifact": logical_name,
                    "fix_row_ranges": fix_row_ranges,
                    "issue_ids": issue_ids,
                    "individuals": individuals,
                    "snapshot_windows": snapshot_windows,
                    "screenshot_mode": screenshot_mode,
                    "snapshots": snapshot_parameters,
                    "individual_report_artifacts": individual_report_artifacts,
                    "user": user,
                },
            }
            if actor is not None:
                payload = apply_actor(payload, actor)
            return JSONResponse(create_analysis(project_dir, payload))
        except ReviewForbiddenError as exc:
            return _review_error_response(exc)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    if include_dev_routes:
        from .dev_routes import register_movement_dev_routes

        register_movement_dev_routes(
            app,
            data_root=data_root,
            configured_study_dir=configured_study_dir,
            default_burst_gap_mode=DEFAULT_BURST_GAP_MODE,
            default_burst_gap_seconds=DEFAULT_BURST_GAP_SECONDS,
            default_burst_gap_quantile=DEFAULT_BURST_GAP_QUANTILE,
            annotate_scope_script=ANNOTATE_SCOPE_SCRIPT,
            publish_event=lambda family, study, directory, reason, actor: publish_state_event(
                family,
                study,
                directory,
                reason=reason,
                actor=actor,
            ),
        )
