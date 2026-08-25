from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path
import shutil
from typing import Callable, Iterator

from .execution import create_step, set_current_head, undo_to_parent
from .state import (
    ProjectStateError,
    list_datasets,
    list_history,
    load_dataset,
    load_project_state,
    make_id,
    normalize_user,
    now_iso,
    project_paths,
    save_json,
    update_project_state,
)


class EditLockedError(ProjectStateError):
    def __init__(self, profile: dict):
        super().__init__("Selected dataset is locked for persistent edits")
        self.profile = profile


class EditConflictError(ProjectStateError):
    pass


def _blocker(code: str, message: str, *, scope: str) -> dict:
    return {
        "code": code,
        "scope": scope,
        "message": message,
        "owner": None,
        "acquired_at": None,
        "expires_at": None,
    }


def _dataset_index(project_dir: Path) -> tuple[list[dict], dict[str, dict]]:
    datasets = list_datasets(project_dir)
    return datasets, {
        str(dataset.get("dataset_id") or ""): dataset
        for dataset in datasets
        if dataset.get("dataset_id")
    }


def _ancestor_ids(dataset_by_id: dict[str, dict], dataset_id: str) -> list[str]:
    ancestors: list[str] = []
    seen: set[str] = set()
    current_id = dataset_id
    while current_id:
        if current_id in seen:
            raise ProjectStateError("Dataset lineage contains a cycle")
        dataset = dataset_by_id.get(current_id)
        if dataset is None:
            raise ProjectStateError("Dataset lineage references an unknown parent")
        seen.add(current_id)
        ancestors.append(current_id)
        current_id = str(dataset.get("parent_dataset_id") or "")
    ancestors.reverse()
    return ancestors


def _resume_plan(
    project_dir: Path,
    selected_dataset_id: str,
    *,
    datasets: list[dict] | None = None,
    dataset_by_id: dict[str, dict] | None = None,
) -> dict:
    if datasets is None or dataset_by_id is None:
        datasets, dataset_by_id = _dataset_index(project_dir)
    if selected_dataset_id not in dataset_by_id:
        raise ProjectStateError("Unknown dataset")
    state = load_project_state(project_dir)
    current_dataset_id = str(state["current_dataset_id"])
    keep_dataset_ids = _ancestor_ids(dataset_by_id, selected_dataset_id)
    keep_set = set(keep_dataset_ids)
    discard_dataset_ids = sorted(
        dataset_id
        for dataset_id in dataset_by_id
        if dataset_id not in keep_set
    )
    discard_set = set(discard_dataset_ids)
    history = list_history(project_dir)
    discard_steps = sorted(
        (
            step
            for step in history["steps"]
            if str(step.get("output_dataset_id") or "") in discard_set
        ),
        key=lambda item: str(item.get("step_id") or ""),
    )
    discard_analyses = sorted(
        (
            analysis
            for analysis in history["analyses"]
            if str(analysis.get("dataset_id") or "") in discard_set
        ),
        key=lambda item: str(item.get("analysis_id") or ""),
    )
    token_payload = {
        "selected_dataset_id": selected_dataset_id,
        "current_dataset_id": current_dataset_id,
        "keep_dataset_ids": keep_dataset_ids,
        "discard_dataset_ids": discard_dataset_ids,
        "discard_step_ids": [
            str(step.get("step_id") or "")
            for step in discard_steps
        ],
        "discard_analysis_ids": [
            str(analysis.get("analysis_id") or "")
            for analysis in discard_analyses
        ],
    }
    token = hashlib.sha256(
        json.dumps(token_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        **token_payload,
        "discard_steps": discard_steps,
        "discard_analyses": discard_analyses,
        "discard_dataset_count": len(discard_dataset_ids),
        "discard_step_count": len(discard_steps),
        "discard_analysis_count": len(discard_analyses),
        "token": token,
    }


def build_edit_lock_profile(
    project_dir: Path,
    selected_dataset_id: str,
    *,
    additional_blockers: list[dict] | None = None,
) -> dict:
    project_dir = project_dir.resolve()
    selected_dataset_id = str(selected_dataset_id or "").strip()
    datasets, dataset_by_id = _dataset_index(project_dir)
    if selected_dataset_id not in dataset_by_id:
        raise ProjectStateError("Unknown dataset")
    state = load_project_state(project_dir)
    current_dataset_id = str(state["current_dataset_id"])
    direct_children = sorted(
        str(dataset.get("dataset_id") or "")
        for dataset in datasets
        if str(dataset.get("parent_dataset_id") or "") == selected_dataset_id
    )
    blockers: list[dict] = []
    if selected_dataset_id != current_dataset_id:
        blockers.append(
            _blocker(
                "historical_version",
                "Historical versions are read-only until Resume discards forward history.",
                scope="dataset",
            )
        )
    elif direct_children:
        blockers.append(
            _blocker(
                "forward_history_pending",
                "Undo moved the current pointer backward; Resume is required before editing.",
                scope="study",
            )
        )
    blockers.extend(dict(item) for item in (additional_blockers or []))
    resume = _resume_plan(
        project_dir,
        selected_dataset_id,
        datasets=datasets,
        dataset_by_id=dataset_by_id,
    )
    resume_allowed = bool(resume["discard_dataset_ids"]) and any(
        blocker.get("code") in {"historical_version", "forward_history_pending"}
        for blocker in blockers
    )
    return {
        "resource": {
            "kind": "study",
            "project_name": project_dir.name,
        },
        "selected_dataset_id": selected_dataset_id,
        "current_dataset_id": current_dataset_id,
        "editable": not blockers,
        "blockers": blockers,
        "resume": {
            "allowed": resume_allowed,
            "target_dataset_id": selected_dataset_id,
            "discard_dataset_count": resume["discard_dataset_count"],
            "discard_step_count": resume["discard_step_count"],
            "discard_analysis_count": resume["discard_analysis_count"],
            "token": resume["token"] if resume_allowed else "",
        },
    }


def require_editable_dataset(
    project_dir: Path,
    selected_dataset_id: str,
    *,
    additional_blockers: list[dict] | None = None,
) -> dict:
    profile = build_edit_lock_profile(
        project_dir,
        selected_dataset_id,
        additional_blockers=additional_blockers,
    )
    if not profile["editable"]:
        raise EditLockedError(profile)
    return profile


@contextmanager
def project_mutation_lock(project_dir: Path) -> Iterator[None]:
    paths = project_paths(project_dir.resolve())
    paths["meta"].mkdir(parents=True, exist_ok=True)
    with paths["mutation_lock"].open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def create_guarded_step(
    project_dir: Path,
    payload: dict,
    *,
    selected_dataset_id: str,
    expected_current_dataset_id: str,
    additional_blockers: list[dict] | None = None,
    preflight: Callable[[], None] | None = None,
) -> dict:
    project_dir = project_dir.resolve()
    with project_mutation_lock(project_dir):
        state = load_project_state(project_dir)
        current_dataset_id = str(state["current_dataset_id"])
        if current_dataset_id != str(expected_current_dataset_id or ""):
            raise EditConflictError("Current dataset changed; reload before editing")
        if preflight is not None:
            preflight()
        require_editable_dataset(
            project_dir,
            selected_dataset_id,
            additional_blockers=additional_blockers,
        )
        guarded_payload = dict(payload)
        guarded_payload["parent_dataset_id"] = selected_dataset_id
        return create_step(project_dir, guarded_payload)


def undo_guarded(
    project_dir: Path,
    *,
    expected_current_dataset_id: str,
    preflight: Callable[[], None] | None = None,
) -> dict:
    project_dir = project_dir.resolve()
    with project_mutation_lock(project_dir):
        state = load_project_state(project_dir)
        current_dataset_id = str(state["current_dataset_id"])
        if current_dataset_id != str(expected_current_dataset_id or ""):
            raise EditConflictError("Current dataset changed; reload before undoing")
        if preflight is not None:
            preflight()
        return undo_to_parent(project_dir)


def restore_forward_head_guarded(
    project_dir: Path,
    *,
    selected_dataset_id: str,
    expected_current_dataset_id: str,
    preflight: Callable[[], None] | None = None,
) -> dict:
    """Move a rewound current pointer to an existing descendant graph tip."""
    project_dir = project_dir.resolve()
    with project_mutation_lock(project_dir):
        state = load_project_state(project_dir)
        current_dataset_id = str(state["current_dataset_id"])
        if current_dataset_id != str(expected_current_dataset_id or ""):
            raise EditConflictError("Current dataset changed; reload before restoring history")

        selected_dataset_id = str(selected_dataset_id or "").strip()
        datasets, dataset_by_id = _dataset_index(project_dir)
        if selected_dataset_id not in dataset_by_id:
            raise ProjectStateError("Unknown dataset")
        if selected_dataset_id == current_dataset_id:
            raise ProjectStateError("Selected dataset is already current")
        if current_dataset_id not in _ancestor_ids(dataset_by_id, selected_dataset_id):
            raise ProjectStateError("Selected dataset is not forward history of the current version")
        if any(
            str(dataset.get("parent_dataset_id") or "") == selected_dataset_id
            for dataset in datasets
        ):
            raise ProjectStateError("Selected dataset is not a graph tip")
        if preflight is not None:
            preflight()
        return {
            "dataset": set_current_head(project_dir, selected_dataset_id),
            "history": list_history(project_dir),
        }


def _file_inventory(path: Path, project_dir: Path) -> list[dict]:
    if not path.exists():
        return []
    candidates = [path] if path.is_file() else sorted(
        candidate for candidate in path.rglob("*") if candidate.is_file()
    )
    return [
        {
            "path": candidate.relative_to(project_dir).as_posix(),
            "size": candidate.stat().st_size,
        }
        for candidate in candidates
    ]


def _copy_analysis_metadata(source: Path, destination: Path):
    destination.mkdir(parents=True, exist_ok=True)
    for name in ("analysis.json", "analysis.py", "spec.json", "summary.json"):
        source_path = source / name
        if source_path.exists() and source_path.is_file():
            shutil.copy2(source_path, destination / name)


def resume_from_dataset(
    project_dir: Path,
    *,
    selected_dataset_id: str,
    expected_current_dataset_id: str,
    resume_token: str,
    user: str,
    preflight: Callable[[], None] | None = None,
) -> dict:
    project_dir = project_dir.resolve()
    normalized_user = normalize_user(user)
    with project_mutation_lock(project_dir):
        state = load_project_state(project_dir)
        current_dataset_id = str(state["current_dataset_id"])
        if current_dataset_id != str(expected_current_dataset_id or ""):
            raise EditConflictError("Current dataset changed; reopen the Resume confirmation")
        if preflight is not None:
            preflight()
        plan = _resume_plan(project_dir, selected_dataset_id)
        if not plan["discard_dataset_ids"]:
            raise ProjectStateError("Selected dataset has no forward history to discard")
        if plan["token"] != str(resume_token or ""):
            raise EditConflictError("History changed; reopen the Resume confirmation")

        paths = project_paths(project_dir)
        archive_id = make_id("archive")
        staging_dir = paths["archives"] / f".staging_{archive_id}"
        archive_dir = paths["archives"] / archive_id
        staging_dir.mkdir(parents=True, exist_ok=False)
        removed_files: list[dict] = []
        try:
            for dataset_id in plan["discard_dataset_ids"]:
                source = paths["datasets"] / f"{dataset_id}.json"
                removed_files.extend(_file_inventory(source, project_dir))
                if source.exists():
                    destination = staging_dir / "datasets" / source.name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, destination)
                output_dir = paths["outputs"] / dataset_id
                removed_files.extend(_file_inventory(output_dir, project_dir))

            for step in plan["discard_steps"]:
                step_id = str(step.get("step_id") or "")
                source = paths["steps"] / step_id
                removed_files.extend(_file_inventory(source, project_dir))
                if source.exists():
                    shutil.copytree(source, staging_dir / "steps" / step_id)

            for analysis in plan["discard_analyses"]:
                analysis_id = str(analysis.get("analysis_id") or "")
                source = paths["analyses"] / analysis_id
                removed_files.extend(_file_inventory(source, project_dir))
                if source.exists():
                    _copy_analysis_metadata(
                        source,
                        staging_dir / "analyses" / analysis_id,
                    )

            manifest = {
                "archive_id": archive_id,
                "created_at": now_iso(),
                "user": normalized_user,
                "source_current_dataset_id": current_dataset_id,
                "target_dataset_id": selected_dataset_id,
                "kept_dataset_ids": plan["keep_dataset_ids"],
                "discarded_dataset_ids": plan["discard_dataset_ids"],
                "discarded_step_ids": [
                    str(step.get("step_id") or "")
                    for step in plan["discard_steps"]
                ],
                "discarded_analysis_ids": [
                    str(analysis.get("analysis_id") or "")
                    for analysis in plan["discard_analyses"]
                ],
                "removed_files": removed_files,
            }
            save_json(staging_dir / "manifest.json", manifest)
            paths["archives"].mkdir(parents=True, exist_ok=True)
            staging_dir.replace(archive_dir)
        except Exception:
            shutil.rmtree(staging_dir, ignore_errors=True)
            raise

        update_project_state(project_dir, {"current_dataset_id": selected_dataset_id})
        for dataset_id in plan["discard_dataset_ids"]:
            (paths["datasets"] / f"{dataset_id}.json").unlink(missing_ok=True)
            shutil.rmtree(paths["outputs"] / dataset_id, ignore_errors=True)
        for step in plan["discard_steps"]:
            shutil.rmtree(paths["steps"] / str(step.get("step_id") or ""), ignore_errors=True)
        for analysis in plan["discard_analyses"]:
            shutil.rmtree(
                paths["analyses"] / str(analysis.get("analysis_id") or ""),
                ignore_errors=True,
            )

        dataset = load_dataset(project_dir, selected_dataset_id)
        return {
            "dataset": dataset,
            "history": list_history(project_dir),
            "profile": build_edit_lock_profile(project_dir, selected_dataset_id),
            "archive": {
                "archive_id": archive_id,
                "discarded_dataset_count": plan["discard_dataset_count"],
                "discarded_step_count": plan["discard_step_count"],
                "discarded_analysis_count": plan["discard_analysis_count"],
            },
        }
