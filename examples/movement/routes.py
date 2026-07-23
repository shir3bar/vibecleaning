import base64
import binascii
from collections.abc import Callable
import json
from math import isfinite
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

from app.execution import create_analysis, create_step, undo_to_parent
from app.state import (
    ProjectStateError,
    get_dataset_artifact,
    graph_payload,
    load_dataset,
    load_json,
    media_type_for_path,
    project_paths,
    project_state_payload,
)
from app.web import get_project_dir, json_error, parse_json_body, validate_path_part

from .analysis_history import build_movement_analysis_history
from .catalog import get_study_dir, list_families, list_studies
from .review_annotations import (
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
)


ArtifactFilter = Callable[[dict], bool]
MAX_REPORT_SNAPSHOTS = 100
MAX_REPORT_SNAPSHOT_BYTES = 20 * 1024 * 1024


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
):
    data_root = data_root.resolve()
    configured_families = set(allowed_families or [])

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
    async def get_movement_families():
        families = list_families(data_root)
        if configured_families:
            families = [item for item in families if item.get("name") in configured_families]
        return JSONResponse({"families": families})

    @app.get("/api/apps/movement/family/{family_name}/studies")
    async def get_movement_studies(family_name: str):
        try:
            family = require_configured_family(family_name)
            return JSONResponse(
                {
                    "family": family,
                    "studies": list_studies(data_root, family),
                }
            )
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/state")
    async def get_movement_study_state(family_name: str, study_name: str):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            return JSONResponse(project_state_payload(study_dir))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/graph")
    async def get_movement_study_graph(family_name: str, study_name: str):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            return JSONResponse(graph_payload(study_dir))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/load")
    async def get_movement_study_load(family_name: str, study_name: str):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            return JSONResponse(
                await run_in_threadpool(
                    _build_initial_study_payload,
                    study_dir,
                    configured_artifact_filter,
                )
            )
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/dataset/{dataset_id}")
    async def get_movement_study_dataset(family_name: str, study_name: str, dataset_id: str):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            return JSONResponse(load_dataset(study_dir, dataset_id))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/dataset/{dataset_id}/overview")
    async def get_movement_study_overview(
        family_name: str,
        study_name: str,
        dataset_id: str,
        logical_name: str,
        burst_gap_mode: str | None = None,
        burst_gap_seconds: float | None = None,
        burst_gap_quantile: float | None = None,
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
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
            )
            return JSONResponse(
                apply_review_annotations(payload, annotations, source_artifact=logical_name)
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
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
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
            )
            payload = apply_review_annotations(payload, annotations, source_artifact=logical_name)
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
    async def get_movement_study_summary(family_name: str, study_name: str, dataset_id: str, logical_name: str):
        try:
            study_dir = configured_study_dir(family_name, study_name)
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
            return JSONResponse(
                apply_review_annotations(payload, annotations, source_artifact=logical_name)
            )
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/analyses")
    async def get_movement_study_analyses(
        family_name: str,
        study_name: str,
        dataset_id: str,
        logical_name: str,
        burst_gap_mode: str | None = None,
        burst_gap_seconds: float | None = None,
        burst_gap_quantile: float | None = None,
        feature_set: str = "movement_only",
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            normalized_dataset_id = validate_path_part(dataset_id, label="dataset")
            normalized_logical_name = validate_path_part(logical_name, label="artifact")
            get_dataset_artifact(study_dir, normalized_dataset_id, normalized_logical_name)
            return JSONResponse(
                build_movement_analysis_history(
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
    ):
        try:
            study_dir = configured_study_dir(family_name, study_name)
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
    async def post_movement_run_burst_anomaly_ranking(family_name: str, study_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = configured_study_dir(family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            dataset = load_dataset(study_dir, dataset_id)
            feature_set = parse_anomaly_feature_set(body.get("feature_set"))
            feature_set_label = (
                "movement + OSM context"
                if feature_set == "movement_plus_context"
                else "movement only"
            )
            user = body.get("user")
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
            return JSONResponse(create_analysis(study_dir, payload))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/undo")
    async def post_movement_study_undo(family_name: str, study_name: str):
        try:
            study_dir = configured_study_dir(family_name, study_name)
            return JSONResponse(undo_to_parent(study_dir))
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
            if scope_kind not in {"fix", "segment", "burst", "individual"}:
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
            elif scope_kind == "individual":
                scope["individual"] = _normalize_individual_name(raw_scope.get("individual"))
                scope["set_name"] = _validate_optional_text(
                    raw_scope.get("set_name"),
                    label="Set name",
                    max_length=40,
                )
            else:
                scope["burst_id"] = _validate_required_text(
                    raw_scope.get("burst_id"),
                    label="Burst id",
                    max_length=240,
                )

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
            user = body.get("user")
            input_artifacts = [logical_name]
            if any(
                artifact.get("logical_name") == "movement_review_annotations.json"
                for artifact in dataset.get("artifacts", [])
            ):
                input_artifacts.append("movement_review_annotations.json")
            payload = {
                "user": user,
                "title": f"Mark {scope_kind} as {status} in {logical_name}",
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
            return JSONResponse(create_step(study_dir, payload))
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
            payload = {
                "user": body.get("user"),
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
                    "user": body.get("user"),
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": input_artifacts,
                "output_artifacts": ["movement_review_annotations.json"],
                "set_as_head": True,
            }
            return JSONResponse(create_step(study_dir, payload))
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
            user = body.get("user")
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
            return JSONResponse(create_analysis(study_dir, payload))
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
            user = body.get("user")
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
            return JSONResponse(create_analysis(study_dir, payload))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.get("/api/project/{project_name}/apps/movement/dataset/{dataset_id}/summary")
    async def get_movement_summary(project_name: str, dataset_id: str, logical_name: str):
        try:
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
            user = body.get("user")
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
            return JSONResponse(create_analysis(project_dir, payload))
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
        )
