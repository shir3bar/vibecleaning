import hashlib
import json
from math import isclose
from pathlib import Path

from app.state import (
    ProjectStateError,
    get_dataset_artifact_entry,
    list_history,
    load_dataset,
    load_json,
    project_paths,
)

from .bursts import (
    DEFAULT_BURST_GAP_MODE,
    DEFAULT_BURST_GAP_QUANTILE,
    DEFAULT_BURST_GAP_SECONDS,
)
from .review_annotations import confirmed_exclusion_scopes, load_review_annotations


RESTORABLE_ANALYSIS_OUTPUTS = {
    "run_burst_anomaly_ranking": "burst_anomaly_ranking.json",
    "run_burst_feature_space": "burst_feature_space.json",
}


def _artifact_signature(artifact: dict) -> str:
    payload = {
        "logical_name": str(artifact.get("logical_name") or ""),
        "path": str(artifact.get("path") or ""),
        "size": artifact.get("size"),
        "storage_type": str(artifact.get("storage_type") or ""),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _source_signature(project_dir: Path, dataset_id: str, logical_name: str) -> str:
    dataset = load_dataset(project_dir, dataset_id)
    return _artifact_signature(get_dataset_artifact_entry(dataset, logical_name))


def _ancestor_dataset_ids(project_dir: Path, dataset_id: str) -> set[str]:
    ancestors = set()
    current_id = str(dataset_id or "").strip()
    while current_id:
        if current_id in ancestors:
            raise ProjectStateError("Dataset lineage contains a cycle")
        ancestors.add(current_id)
        dataset = load_dataset(project_dir, current_id)
        current_id = str(dataset.get("parent_dataset_id") or "").strip()
    return ancestors


def _review_exclusion_signature(project_dir: Path, dataset_id: str, logical_name: str) -> str:
    dataset = load_dataset(project_dir, dataset_id)
    try:
        sidecar = get_dataset_artifact_entry(dataset, "movement_review_annotations.json")
    except ProjectStateError:
        annotations = []
    else:
        sidecar_path = project_dir / str(sidecar.get("path") or "")
        annotations = load_review_annotations(sidecar_path)
    fix_keys, individual_tracks = confirmed_exclusion_scopes(
        annotations,
        source_artifact=logical_name,
    )
    payload = {
        "fix_keys": sorted(fix_keys),
        "individual_tracks": sorted([list(item) for item in individual_tracks]),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _float_matches(left: object, right: object) -> bool:
    try:
        return isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-12)
    except (TypeError, ValueError):
        return False


def _analysis_parameters_match(
    action: str,
    parameters: dict,
    *,
    burst_gap_mode: str,
    burst_gap_seconds: float,
    burst_gap_quantile: float,
    feature_set: str,
) -> tuple[bool, list[str]]:
    if action not in RESTORABLE_ANALYSIS_OUTPUTS:
        return True, []

    reasons = []
    stored_mode = str(parameters.get("burst_gap_mode") or DEFAULT_BURST_GAP_MODE)
    stored_feature_set = str(parameters.get("feature_set") or "movement_only")
    if stored_mode != burst_gap_mode:
        reasons.append("burst gap mode differs")
    if not _float_matches(parameters.get("burst_gap_seconds", DEFAULT_BURST_GAP_SECONDS), burst_gap_seconds):
        reasons.append("burst gap fallback differs")
    if not _float_matches(parameters.get("burst_gap_quantile", DEFAULT_BURST_GAP_QUANTILE), burst_gap_quantile):
        reasons.append("burst gap quantile differs")
    if stored_feature_set != feature_set:
        reasons.append("feature set differs")
    return not reasons, reasons


def _load_analysis_summary(project_dir: Path, analysis: dict) -> dict:
    relative_path = str(analysis.get("summary_path") or "").strip()
    if not relative_path:
        return {}
    summary_path = (project_dir / relative_path).resolve()
    meta_root = project_paths(project_dir)["meta"].resolve()
    if meta_root not in summary_path.parents or not summary_path.is_file():
        return {}
    try:
        return load_json(summary_path)
    except ProjectStateError:
        return {}


def _realized_output_names(project_dir: Path, analysis: dict) -> list[str]:
    names = []
    for entry in analysis.get("realized_output_artifacts") or []:
        logical_name = str(entry.get("logical_name") or "").strip()
        relative_path = str(entry.get("path") or "").strip()
        if not logical_name or not relative_path:
            continue
        output_path = (project_dir / relative_path).resolve()
        if project_dir.resolve() not in output_path.parents or not output_path.is_file():
            continue
        names.append(logical_name)
    return sorted(set(names))


def _analysis_sort_key(project_dir: Path, analysis: dict) -> tuple[str, int]:
    analysis_id = str(analysis.get("analysis_id") or "").strip()
    record_path = project_paths(project_dir)["analyses"] / analysis_id / "analysis.json"
    try:
        modified_ns = record_path.stat().st_mtime_ns
    except OSError:
        modified_ns = 0
    return str(analysis.get("created_at") or ""), modified_ns


def build_movement_analysis_history(
    project_dir: Path,
    *,
    dataset_id: str,
    logical_name: str,
    burst_gap_mode: str = DEFAULT_BURST_GAP_MODE,
    burst_gap_seconds: float = DEFAULT_BURST_GAP_SECONDS,
    burst_gap_quantile: float = DEFAULT_BURST_GAP_QUANTILE,
    feature_set: str = "movement_only",
) -> dict:
    current_signature = _source_signature(project_dir, dataset_id, logical_name)
    ancestor_dataset_ids = _ancestor_dataset_ids(project_dir, dataset_id)
    current_exclusion_signature = _review_exclusion_signature(
        project_dir,
        dataset_id,
        logical_name,
    )
    items = []
    analyses = sorted(
        list_history(project_dir)["analyses"],
        key=lambda item: _analysis_sort_key(project_dir, item),
        reverse=True,
    )
    for analysis in analyses:
        analysis_dataset_id = str(analysis.get("dataset_id") or "").strip()
        if analysis_dataset_id not in ancestor_dataset_ids:
            continue
        parameters = dict(analysis.get("parameters") or {})
        if str(parameters.get("app") or "") != "movement":
            continue
        action = str(parameters.get("action") or "").strip()
        target_artifact = str(parameters.get("target_artifact") or "").strip()
        reasons = []
        source_matches = False
        exclusion_matches = False
        if target_artifact != logical_name:
            reasons.append("target artifact differs")
        else:
            try:
                stored_signature = _source_signature(
                    project_dir,
                    str(analysis.get("dataset_id") or ""),
                    target_artifact,
                )
                source_matches = stored_signature == current_signature
                stored_exclusion_signature = _review_exclusion_signature(
                    project_dir,
                    str(analysis.get("dataset_id") or ""),
                    target_artifact,
                )
                exclusion_matches = stored_exclusion_signature == current_exclusion_signature
            except (KeyError, ProjectStateError):
                source_matches = False
                exclusion_matches = False
            if not source_matches:
                reasons.append("source artifact differs")
            if not exclusion_matches:
                reasons.append("confirmed exclusion state differs")

        parameters_match, parameter_reasons = _analysis_parameters_match(
            action,
            parameters,
            burst_gap_mode=burst_gap_mode,
            burst_gap_seconds=burst_gap_seconds,
            burst_gap_quantile=burst_gap_quantile,
            feature_set=feature_set,
        )
        reasons.extend(parameter_reasons)
        realized_outputs = _realized_output_names(project_dir, analysis)
        expected_output = RESTORABLE_ANALYSIS_OUTPUTS.get(action, "")
        if expected_output and expected_output not in realized_outputs:
            reasons.append("saved output is missing")

        summary = _load_analysis_summary(project_dir, analysis)
        items.append(
            {
                "analysis_id": str(analysis.get("analysis_id") or ""),
                "dataset_id": str(analysis.get("dataset_id") or ""),
                "action": action,
                "title": str(analysis.get("title") or ""),
                "user": str(analysis.get("user") or ""),
                "created_at": str(analysis.get("created_at") or ""),
                "target_artifact": target_artifact,
                "parameters": parameters,
                "summary": summary,
                "realized_output_artifacts": realized_outputs,
                "expected_output_artifact": expected_output,
                "compatible": source_matches and exclusion_matches and parameters_match and not reasons,
                "compatibility_reasons": reasons,
            }
        )

    latest_compatible = {}
    for item in items:
        action = item["action"]
        if item["compatible"] and action in RESTORABLE_ANALYSIS_OUTPUTS and action not in latest_compatible:
            latest_compatible[action] = item["analysis_id"]
    return {
        "dataset_id": dataset_id,
        "logical_name": logical_name,
        "source_signature": current_signature,
        "confirmed_exclusion_signature": current_exclusion_signature,
        "items": items,
        "latest_compatible_by_action": latest_compatible,
    }
