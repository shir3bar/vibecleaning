"""Migrate one copied-CSV movement review step to the annotation sidecar model."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.state import (
    get_dataset_artifact,
    load_dataset,
    load_json,
    media_type_for_path,
    project_paths,
    save_json,
)
from examples.movement.bursts import (
    DEFAULT_BURST_GAP_MODE,
    DEFAULT_BURST_GAP_QUANTILE,
    DEFAULT_BURST_GAP_SECONDS,
)
from examples.movement.review_annotations import (
    REVIEW_SIDECAR_NAME,
    compress_fix_keys,
    load_review_annotations,
    normalize_annotation,
)
from examples.movement.routes import ANNOTATE_SCOPE_SCRIPT


LEGACY_ACTION = "annotate_fixes"


def _producing_step(project_dir: Path, dataset_id: str) -> tuple[Path, dict]:
    matches = []
    for step_path in project_paths(project_dir)["steps"].glob("*/step.json"):
        step = load_json(step_path)
        if str(step.get("output_dataset_id") or "") == dataset_id:
            matches.append((step_path, step))
    if len(matches) != 1:
        raise ValueError(
            f"Expected one producing step for {dataset_id}; found {len(matches)}"
        )
    return matches[0]


def _assert_leaf_dataset(project_dir: Path, dataset_id: str) -> None:
    descendants = []
    for dataset_path in project_paths(project_dir)["datasets"].glob("dataset_*.json"):
        dataset = load_json(dataset_path)
        if str(dataset.get("parent_dataset_id") or "") == dataset_id:
            descendants.append(str(dataset.get("dataset_id") or dataset_path.stem))
    if descendants:
        raise ValueError(
            "Legacy review migration only supports leaf datasets; descendants: "
            + ", ".join(sorted(descendants))
        )


def _input_spec_entry(project_dir: Path, artifact: dict) -> dict:
    return {
        "logical_name": artifact["logical_name"],
        "path": str((project_dir / artifact["path"]).resolve()),
        "content_type": artifact.get("content_type")
        or media_type_for_path(Path(artifact["path"])),
        "metadata": dict(artifact.get("metadata") or {}),
    }


def build_migration(
    project_dir: Path,
    dataset_id: str,
) -> dict:
    project_dir = project_dir.resolve()
    dataset = load_dataset(project_dir, dataset_id)
    parent_dataset_id = str(dataset.get("parent_dataset_id") or "")
    if not parent_dataset_id:
        raise ValueError("The root dataset cannot be a legacy review step")
    _assert_leaf_dataset(project_dir, dataset_id)
    parent_dataset = load_dataset(project_dir, parent_dataset_id)
    step_path, step = _producing_step(project_dir, dataset_id)
    parameters = dict(step.get("parameters") or {})
    if parameters.get("app") != "movement" or parameters.get("action") != LEGACY_ACTION:
        raise ValueError(f"{dataset_id} is not a legacy movement review step")

    target_artifact = str(parameters.get("target_artifact") or "").strip()
    if not target_artifact:
        raise ValueError("Legacy movement review step has no target artifact")
    copied_artifact = next(
        (
            artifact
            for artifact in dataset.get("artifacts") or []
            if artifact.get("logical_name") == target_artifact
        ),
        None,
    )
    parent_artifact = next(
        (
            artifact
            for artifact in parent_dataset.get("artifacts") or []
            if artifact.get("logical_name") == target_artifact
        ),
        None,
    )
    if copied_artifact is None or parent_artifact is None:
        raise ValueError("Legacy movement source artifact is missing")
    if copied_artifact.get("storage_type") != "output":
        raise ValueError("Legacy movement source is not a copied output artifact")
    copied_path = (project_dir / copied_artifact["path"]).resolve()
    expected_output_dir = (
        project_paths(project_dir)["outputs"] / dataset_id
    ).resolve()
    if expected_output_dir not in copied_path.parents or copied_path.suffix.lower() != ".csv":
        raise ValueError("Legacy movement output path is not safe to remove")
    if not copied_path.is_file():
        raise ValueError("Legacy copied CSV is missing")

    fix_keys = sorted(
        {
            str(item).strip()
            for item in (
                parameters.get("fix_keys")
                or (step.get("summary") or {}).get("matched_fix_keys")
                or []
            )
            if str(item).strip()
        }
    )
    if not fix_keys:
        raise ValueError("Legacy movement review step has no selected fixes")
    step_id = str(step.get("step_id") or step_path.parent.name)
    status = str(parameters.get("status") or "suspected").strip().lower()
    if status not in {"suspected", "confirmed"}:
        raise ValueError("Legacy movement review status is invalid")
    old_summary = dict(step.get("summary") or {})
    annotation = normalize_annotation(
        {
            "annotation_id": step_id,
            "step_id": step_id,
            "source_artifact": target_artifact,
            "source_dataset_id": parent_dataset_id,
            "status": status,
            "origin": "manual",
            "issue_type": parameters.get("issue_type"),
            "comment": parameters.get("issue_note"),
            "owner_question": parameters.get("owner_question"),
            "user": parameters.get("user") or step.get("user"),
            "created_at": old_summary.get("reviewed_at") or step.get("created_at"),
            "resolved_fix_count": len(fix_keys),
            "scope": {
                "kind": "fix",
                "row_ranges": compress_fix_keys(fix_keys),
            },
        }
    )

    parent_sidecar = next(
        (
            artifact
            for artifact in parent_dataset.get("artifacts") or []
            if artifact.get("logical_name") == REVIEW_SIDECAR_NAME
        ),
        None,
    )
    annotations = load_review_annotations(
        (project_dir / parent_sidecar["path"]).resolve()
        if parent_sidecar is not None
        else None
    )
    annotations.append(annotation)
    sidecar_payload = {
        "schema_version": 1,
        "annotations": annotations,
    }
    sidecar_text = (
        json.dumps(sidecar_payload, indent=2, sort_keys=True) + "\n"
    )
    output_dir = project_paths(project_dir)["outputs"] / dataset_id
    sidecar_path = output_dir / REVIEW_SIDECAR_NAME
    sidecar_artifact = {
        "logical_name": REVIEW_SIDECAR_NAME,
        "path": str(sidecar_path.relative_to(project_dir)),
        "storage_type": "output",
        "size": len(sidecar_text.encode("utf-8")),
        "content_type": "application/json",
        "metadata": {},
    }
    next_artifacts = [
        dict(artifact)
        for artifact in parent_dataset.get("artifacts") or []
        if artifact.get("logical_name") != REVIEW_SIDECAR_NAME
    ]
    next_artifacts.append(sidecar_artifact)

    scope = {
        "kind": "fix",
        "row_ranges": compress_fix_keys(fix_keys),
        "fix_count": len(fix_keys),
    }
    next_parameters = {
        "app": "movement",
        "action": "annotate_scope",
        "target_artifact": target_artifact,
        "dataset_id": parent_dataset_id,
        "scope": scope,
        "status": status,
        "origin": "manual",
        "issue_type": str(parameters.get("issue_type") or ""),
        "comment": str(parameters.get("issue_note") or ""),
        "owner_question": str(parameters.get("owner_question") or ""),
        "source_analysis_id": "",
        "burst_gap_mode": DEFAULT_BURST_GAP_MODE,
        "burst_gap_seconds": DEFAULT_BURST_GAP_SECONDS,
        "burst_gap_quantile": DEFAULT_BURST_GAP_QUANTILE,
        "user": str(parameters.get("user") or step.get("user") or ""),
    }
    input_artifacts = [target_artifact]
    if parent_sidecar is not None:
        input_artifacts.append(REVIEW_SIDECAR_NAME)
    next_summary = {
        "app": "movement",
        "action": "annotate_scope",
        "annotation_id": step_id,
        "scope_kind": "fix",
        "status": status,
        "origin": "manual",
        "resolved_fix_count": len(fix_keys),
        "source_artifact": target_artifact,
        "materialized_csv": False,
    }
    next_step = {
        **step,
        "input_artifacts": input_artifacts,
        "output_artifacts": [REVIEW_SIDECAR_NAME],
        "parameters": next_parameters,
        "summary": next_summary,
    }
    next_dataset = {
        **dataset,
        "artifacts": next_artifacts,
    }
    spec_path = step_path.parent / "spec.json"
    script_path = step_path.parent / "transform.py"
    summary_path = step_path.parent / "summary.json"
    next_spec = {
        "mode": "step",
        "project_dir": str(project_dir),
        "project_name": project_dir.name,
        "parent_dataset": parent_dataset,
        "input_artifacts": [
            _input_spec_entry(project_dir, artifact)
            for artifact in parent_dataset.get("artifacts") or []
            if artifact.get("logical_name") in input_artifacts
        ],
        "output_artifacts": [
            {
                "logical_name": REVIEW_SIDECAR_NAME,
                "path": str(sidecar_path.resolve()),
            }
        ],
        "step": {
            "step_id": step_id,
            "title": str(step.get("title") or ""),
            "user": str(step.get("user") or ""),
            "parameters": next_parameters,
            "remove_artifacts": [],
        },
    }
    return {
        "project_dir": project_dir,
        "dataset_id": dataset_id,
        "dataset_path": project_paths(project_dir)["datasets"] / f"{dataset_id}.json",
        "dataset": next_dataset,
        "step_path": step_path,
        "step": next_step,
        "spec_path": spec_path,
        "spec": next_spec,
        "script_path": script_path,
        "summary_path": summary_path,
        "summary": next_summary,
        "sidecar_path": sidecar_path,
        "sidecar_text": sidecar_text,
        "copied_path": copied_path,
        "reclaimed_bytes": copied_path.stat().st_size,
    }


def apply_migration(migration: dict) -> None:
    sidecar_path = migration["sidecar_path"]
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path.write_text(migration["sidecar_text"], encoding="utf-8")
    migration["script_path"].write_text(ANNOTATE_SCOPE_SCRIPT, encoding="utf-8")
    save_json(migration["summary_path"], migration["summary"])
    save_json(migration["spec_path"], migration["spec"])
    save_json(migration["step_path"], migration["step"])
    save_json(migration["dataset_path"], migration["dataset"])
    migration["copied_path"].unlink()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Replace one legacy copied-CSV movement review node with a small "
            "annotation sidecar."
        )
    )
    parser.add_argument("project_dir", type=Path)
    parser.add_argument("dataset_id")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the migration and remove the redundant copied CSV.",
    )
    args = parser.parse_args()
    migration = build_migration(args.project_dir, args.dataset_id)
    action = "Migrated" if args.apply else "Would migrate"
    if args.apply:
        apply_migration(migration)
    print(
        f"{action} {migration['dataset_id']}: "
        f"{migration['copied_path']} -> {migration['sidecar_path']} "
        f"({migration['reclaimed_bytes']} bytes reclaimed)"
    )
    if not args.apply:
        print("Dry run only; rerun with --apply to make the change.")


if __name__ == "__main__":
    main()
