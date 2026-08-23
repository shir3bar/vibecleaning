"""Movement input adapters shared by the CSV and RDS app wrappers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.state import get_dataset_artifact

from .analysis_history import artifact_signature
from .rds_index import (
    build_rds_fixes,
    build_rds_overview,
    ensure_rds_index,
    is_rds_artifact,
    list_rds_individuals,
)
from .summary import (
    build_movement_fixes,
    build_movement_overview,
    build_movement_summary,
    list_movement_individuals,
)


@dataclass(frozen=True)
class MovementSourceAdapter:
    source_format: str
    suffix: str
    bundle_scoped: bool

    def accepts(self, artifact: dict) -> bool:
        logical_name = str(artifact.get("logical_name") or "").lower()
        return logical_name.endswith(self.suffix)


CSV_SOURCE = MovementSourceAdapter("csv", ".csv", False)
RDS_SOURCE = MovementSourceAdapter("rds", ".rds", True)


def source_adapter(source_format: str) -> MovementSourceAdapter:
    normalized = str(source_format or "csv").strip().lower()
    if normalized == "csv":
        return CSV_SOURCE
    if normalized == "rds":
        return RDS_SOURCE
    raise ValueError(f"Unsupported movement source format: {source_format}")


def source_overview(
    adapter: MovementSourceAdapter,
    study_dir: Path,
    dataset_id: str,
    logical_name: str,
    **options,
) -> tuple[dict, str]:
    if adapter.bundle_scoped:
        bundle, index_path = ensure_rds_index(study_dir, dataset_id)
        payload = build_rds_overview(
            index_path,
            overview_fix_limit=options.get("overview_fix_limit", 25_000),
            max_series_points=options.get("max_series_points", 1_500),
        )
        payload["source_bundle_signature"] = bundle.signature
        payload["source_artifacts"] = [
            str(item.get("logical_name") or "") for item in bundle.artifacts
        ]
        return payload, bundle.signature
    artifact, path = get_dataset_artifact(study_dir, dataset_id, logical_name)
    payload = build_movement_overview(path, **options)
    payload["source_format"] = "csv"
    return payload, artifact_signature(artifact)


def source_fixes(
    adapter: MovementSourceAdapter,
    study_dir: Path,
    dataset_id: str,
    logical_name: str,
    **options,
) -> tuple[dict, str]:
    if adapter.bundle_scoped:
        bundle, index_path = ensure_rds_index(study_dir, dataset_id)
        payload = build_rds_fixes(
            index_path,
            individuals=options.get("individuals"),
            start_ms=options.get("start_ms"),
            end_ms=options.get("end_ms"),
            review_status=options.get("review_status", ""),
            limit=options.get("limit"),
            confirmed_fix_keys=options.get("confirmed_fix_keys"),
            confirmed_individual_tracks=options.get("confirmed_individual_tracks"),
            annotations=options.get("annotations"),
        )
        payload["source_format"] = "rds"
        payload["source_bundle_signature"] = bundle.signature
        return payload, bundle.signature
    artifact, path = get_dataset_artifact(study_dir, dataset_id, logical_name)
    csv_options = dict(options)
    csv_options.pop("annotations", None)
    payload = build_movement_fixes(path, **csv_options)
    payload["source_format"] = "csv"
    return payload, artifact_signature(artifact)


def source_summary(
    adapter: MovementSourceAdapter,
    study_dir: Path,
    dataset_id: str,
    logical_name: str,
    **options,
) -> tuple[dict, str]:
    if adapter.bundle_scoped:
        bundle, index_path = ensure_rds_index(study_dir, dataset_id)
        payload = build_rds_overview(index_path, overview_fix_limit=0, max_series_points=250)
        payload["source_bundle_signature"] = bundle.signature
        return payload, bundle.signature
    artifact, path = get_dataset_artifact(study_dir, dataset_id, logical_name)
    payload = build_movement_summary(path, **options)
    payload["source_format"] = "csv"
    return payload, artifact_signature(artifact)


def source_individuals(
    adapter: MovementSourceAdapter,
    study_dir: Path,
    dataset_id: str,
    logical_name: str,
) -> list[str]:
    if adapter.bundle_scoped:
        _bundle, index_path = ensure_rds_index(study_dir, dataset_id)
        return list_rds_individuals(index_path)
    _artifact, path = get_dataset_artifact(study_dir, dataset_id, logical_name)
    return list_movement_individuals(path)
