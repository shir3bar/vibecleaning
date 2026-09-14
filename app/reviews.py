from __future__ import annotations

import json
from pathlib import Path
import secrets
from threading import RLock
from typing import Callable, Iterable

from .auth import Actor
from .edit_locks import project_mutation_lock
from .filesystem import atomic_write_json
from .state import (
    ProjectStateError,
    list_history,
    load_dataset,
    load_project_state,
    now_iso,
    project_paths,
    update_project_state,
)


REVIEW_STATE_SCHEMA_VERSION = 1
VALID_REVIEW_EFFECTS = frozenset(
    {"annotation_only", "preserves_individual_scope", "changes_individual_scope"}
)
VALID_REVIEW_DECISIONS = frozenset({"ok", "fix_keep", "remove"})


class ReviewStateError(ProjectStateError):
    pass


class ReviewForbiddenError(ReviewStateError):
    pass


class ReviewConflictError(ReviewStateError):
    pass


class ReviewLockedError(ReviewStateError):
    def __init__(self, message: str, *, code: str):
        super().__init__(message)
        self.code = code


_cache_lock = RLock()
_state_cache: dict[str, tuple[int, int, dict]] = {}


def review_state_path(project_dir: Path) -> Path:
    return project_paths(project_dir.resolve())["meta"] / "reviews.json"


def empty_review_state() -> dict:
    return {
        "schema_version": REVIEW_STATE_SCHEMA_VERSION,
        "revision": 0,
        "reviews": [],
        "editor_control": None,
        "events": [],
    }


def _normalize_actor_snapshot(value: object) -> dict:
    if not isinstance(value, dict):
        return {}
    return {
        "user_id": str(value.get("user_id") or ""),
        "username": str(value.get("username") or ""),
        "display_name": str(value.get("display_name") or ""),
        "role": str(value.get("role") or ""),
    }


def normalize_review_state(payload: object) -> dict:
    if not isinstance(payload, dict):
        raise ReviewStateError("Invalid review state")
    if payload.get("schema_version") != REVIEW_STATE_SCHEMA_VERSION:
        raise ReviewStateError("Unsupported review state")
    reviews = payload.get("reviews")
    events = payload.get("events")
    if not isinstance(reviews, list) or not isinstance(events, list):
        raise ReviewStateError("Invalid review state")
    normalized = {
        "schema_version": REVIEW_STATE_SCHEMA_VERSION,
        "revision": max(0, int(payload.get("revision") or 0)),
        "reviews": [dict(review) for review in reviews if isinstance(review, dict)],
        "editor_control": dict(payload["editor_control"])
        if isinstance(payload.get("editor_control"), dict)
        else None,
        "events": [dict(event) for event in events if isinstance(event, dict)],
    }
    return normalized


def load_review_state(project_dir: Path) -> dict:
    path = review_state_path(project_dir)
    try:
        stat = path.stat()
    except FileNotFoundError:
        return empty_review_state()
    key = str(path.resolve())
    with _cache_lock:
        cached = _state_cache.get(key)
        if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
            return json.loads(json.dumps(cached[2]))
    try:
        payload = normalize_review_state(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReviewStateError("Invalid review state") from exc
    with _cache_lock:
        _state_cache[key] = (stat.st_mtime_ns, stat.st_size, payload)
    return json.loads(json.dumps(payload))


def save_review_state(project_dir: Path, state: dict) -> dict:
    normalized = normalize_review_state(state)
    path = review_state_path(project_dir)
    atomic_write_json(path, normalized)
    stat = path.stat()
    with _cache_lock:
        _state_cache[str(path.resolve())] = (stat.st_mtime_ns, stat.st_size, normalized)
    return normalized


def active_review(state: dict) -> dict | None:
    for review in reversed(state.get("reviews") or []):
        if review.get("status") == "active":
            return review
    return None


def preceding_completed_review(state: dict, *, review_id: str = "") -> dict | None:
    """Return the newest completed review before the requested/current review."""
    for candidate in reversed(state.get("reviews") or []):
        if str(candidate.get("review_id") or "") == str(review_id or ""):
            continue
        if candidate.get("status") == "completed" and candidate.get("final_dataset_id"):
            return candidate
    return None


def review_round_number(state: dict, review: dict | None = None) -> int:
    """Return a stable one-based completed-review round for new and legacy records."""
    explicit = (review or {}).get("review_round")
    try:
        if int(explicit) > 0:
            return int(explicit)
    except (TypeError, ValueError):
        pass
    target_id = str((review or {}).get("review_id") or "")
    completed = 0
    for candidate in state.get("reviews") or []:
        if candidate.get("status") == "completed":
            completed += 1
        if target_id and str(candidate.get("review_id") or "") == target_id:
            return max(1, completed if candidate.get("status") == "completed" else completed + 1)
    return completed + 1


def review_with_provenance(state: dict, review: dict | None) -> dict | None:
    if review is None:
        return None
    result = dict(review)
    result["review_round"] = review_round_number(state, result)
    result["prior_review_id"] = str(
        result.get("prior_review_id")
        or (preceding_completed_review(state, review_id=result.get("review_id")) or {}).get("review_id")
        or ""
    )
    return result


def actor_has_review_history(state: dict, actor: Actor) -> bool:
    if actor.role == "editor":
        return True
    return any(
        str(review.get("reviewer_user_id") or "") == actor.user_id
        for review in state.get("reviews") or []
    )


def can_read_study(project_dir: Path, actor: Actor) -> bool:
    return actor.role == "editor" or actor_has_review_history(load_review_state(project_dir), actor)


def require_study_read(project_dir: Path, actor: Actor) -> dict:
    state = load_review_state(project_dir)
    if actor.role != "editor" and not actor_has_review_history(state, actor):
        raise ReviewForbiddenError("Study is not assigned to this reviewer")
    return state


def _event(state: dict, event_type: str, actor: Actor, **details) -> None:
    state["revision"] = int(state.get("revision") or 0) + 1
    state.setdefault("events", []).append(
        {
            "event_id": f"review_event_{secrets.token_hex(6)}",
            "type": event_type,
            "revision": state["revision"],
            "created_at": now_iso(),
            "actor": actor.as_dict(),
            **details,
        }
    )


def _check_expected_revision(state: dict, expected_revision: object) -> None:
    try:
        value = int(expected_revision)
    except (TypeError, ValueError) as exc:
        raise ReviewConflictError("Review state changed; reload before continuing") from exc
    if value != int(state.get("revision") or 0):
        raise ReviewConflictError("Review state changed; reload before continuing")


def _check_expected_head(project_dir: Path, expected_dataset_id: object) -> str:
    current = str(load_project_state(project_dir)["current_dataset_id"])
    if str(expected_dataset_id or "") != current:
        raise ReviewConflictError("Current dataset changed; reload before continuing")
    return current


def assign_review(
    project_dir: Path,
    *,
    editor: Actor,
    reviewer: Actor,
    expected_current_dataset_id: str,
    expected_review_revision: int,
    individuals: Iterable[str],
    initializer: Callable[[dict, str], dict | None] | None = None,
) -> dict:
    if editor.role != "editor":
        raise ReviewForbiddenError("Only editors can assign reviews")
    if reviewer.role not in {"reviewer", "editor"}:
        raise ReviewStateError("Reviews must be assigned to a reviewer or editor")
    required = sorted({str(item).strip() for item in individuals if str(item).strip()})
    if not required:
        raise ReviewStateError("The study has no individuals to review")
    with project_mutation_lock(project_dir):
        state = load_review_state(project_dir)
        _check_expected_revision(state, expected_review_revision)
        current = _check_expected_head(project_dir, expected_current_dataset_id)
        if active_review(state) is not None:
            raise ReviewLockedError("The study already has an active review", code="active_review")
        prior_review = preceding_completed_review(state)
        review = {
            "review_id": f"review_{secrets.token_hex(6)}",
            "review_round": review_round_number(state),
            "prior_review_id": str((prior_review or {}).get("review_id") or ""),
            "status": "active",
            "reviewer_user_id": reviewer.user_id,
            "reviewer": reviewer.as_dict(),
            "assigned_by": editor.as_dict(),
            "assigned_at": now_iso(),
            "baseline_dataset_id": current,
            "final_dataset_id": None,
            "initial_individuals": required,
        }
        initialization = initializer(dict(review), current) if initializer is not None else None
        initialized_dataset_id = str(
            ((initialization or {}).get("dataset") or {}).get("dataset_id") or current
        )
        state["reviews"].append(review)
        state["editor_control"] = None
        _event(
            state,
            "review_assigned",
            editor,
            review_id=review["review_id"],
            reviewer=reviewer.as_dict(),
            dataset_id=initialized_dataset_id,
            review_round=review["review_round"],
            prior_review_id=review["prior_review_id"],
        )
        try:
            save_review_state(project_dir, state)
        except Exception:
            if initialized_dataset_id != current:
                update_project_state(project_dir, {"current_dataset_id": current})
            raise
        return {
            "review": review,
            "state": state,
            "initialization": initialization,
            "current_dataset_id": initialized_dataset_id,
        }


def cancel_review(
    project_dir: Path,
    *,
    editor: Actor,
    expected_current_dataset_id: str,
    expected_review_revision: int,
    reason: str,
) -> dict:
    if editor.role != "editor":
        raise ReviewForbiddenError("Only editors can cancel reviews")
    normalized_reason = " ".join(str(reason or "").strip().split())
    if not normalized_reason:
        raise ReviewStateError("Cancellation reason is required")
    with project_mutation_lock(project_dir):
        state = load_review_state(project_dir)
        _check_expected_revision(state, expected_review_revision)
        current = _check_expected_head(project_dir, expected_current_dataset_id)
        review = active_review(state)
        if review is None:
            raise ReviewStateError("The study has no active review")
        review["status"] = "cancelled"
        review["final_dataset_id"] = current
        review["cancelled_at"] = now_iso()
        review["cancelled_by"] = editor.as_dict()
        review["cancellation_reason"] = normalized_reason
        state["editor_control"] = None
        _event(
            state,
            "review_cancelled",
            editor,
            review_id=review["review_id"],
            dataset_id=current,
            reason=normalized_reason,
        )
        save_review_state(project_dir, state)
        return {"review": review, "state": state}


def reconcile_review_state_after_resume(
    project_dir: Path,
    *,
    actor: Actor,
    target_dataset_id: str,
    kept_dataset_ids: Iterable[str],
    archive_id: str,
) -> dict:
    """Reconcile review records after lineage pruning while its mutation lock is held."""
    state = load_review_state(project_dir)
    kept = {str(dataset_id) for dataset_id in kept_dataset_ids if str(dataset_id)}
    target = str(target_dataset_id or "")
    discarded_review_ids: list[str] = []
    cancelled_review_ids: list[str] = []
    changed = False

    for review in state.get("reviews") or []:
        status = str(review.get("status") or "")
        baseline = str(review.get("baseline_dataset_id") or "")
        final_dataset = str(review.get("final_dataset_id") or "")
        baseline_discarded = bool(baseline) and baseline not in kept
        final_discarded = status == "completed" and bool(final_dataset) and final_dataset not in kept
        if status == "active" and baseline_discarded:
            review["status"] = "cancelled"
            review["final_dataset_id"] = target
            review["cancelled_at"] = now_iso()
            review["cancelled_by"] = actor.as_dict()
            review["cancellation_reason"] = (
                "The review assignment baseline was discarded by Resume."
            )
            review["history_resume_archive_id"] = str(archive_id or "")
            cancelled_review_ids.append(str(review.get("review_id") or ""))
            _event(
                state,
                "review_cancelled_by_history_resume",
                actor,
                review_id=str(review.get("review_id") or ""),
                dataset_id=target,
                discarded_baseline_dataset_id=baseline,
                archive_id=str(archive_id or ""),
            )
            changed = True
        elif status == "completed" and (baseline_discarded or final_discarded):
            review["status"] = "discarded"
            review["discarded_at"] = now_iso()
            review["discarded_by"] = actor.as_dict()
            review["discarded_by_history_resume"] = True
            review["history_resume_target_dataset_id"] = target
            review["history_resume_archive_id"] = str(archive_id or "")
            discarded_review_ids.append(str(review.get("review_id") or ""))
            _event(
                state,
                "completed_review_discarded_by_history_resume",
                actor,
                review_id=str(review.get("review_id") or ""),
                dataset_id=target,
                prior_final_dataset_id=final_dataset,
                archive_id=str(archive_id or ""),
            )
            changed = True

    if changed:
        if active_review(state) is None:
            state["editor_control"] = None
        state = save_review_state(project_dir, state)
    return {
        "state": state,
        "cancelled_review_ids": cancelled_review_ids,
        "discarded_review_ids": discarded_review_ids,
    }


def start_editor_control(
    project_dir: Path,
    *,
    editor: Actor,
    expected_current_dataset_id: str,
    expected_review_revision: int,
    reason: str,
    takeover: bool = False,
) -> dict:
    if editor.role != "editor":
        raise ReviewForbiddenError("Only editors can take edit control")
    normalized_reason = " ".join(str(reason or "").strip().split())
    if not normalized_reason:
        raise ReviewStateError("Editor-control reason is required")
    with project_mutation_lock(project_dir):
        state = load_review_state(project_dir)
        _check_expected_revision(state, expected_review_revision)
        current = _check_expected_head(project_dir, expected_current_dataset_id)
        review = active_review(state)
        if review is None:
            raise ReviewStateError("The study has no active review")
        existing = state.get("editor_control")
        if existing and existing.get("owner_user_id") != editor.user_id and not takeover:
            raise ReviewLockedError(
                f"Editor control is held by {existing.get('owner_display_name') or 'another editor'}",
                code="editor_control",
            )
        event_type = "editor_control_taken_over" if existing and takeover else "editor_control_started"
        state["editor_control"] = {
            "owner_user_id": editor.user_id,
            "owner_display_name": editor.display_name,
            "owner": editor.as_dict(),
            "reason": normalized_reason,
            "started_at": now_iso(),
        }
        _event(
            state,
            event_type,
            editor,
            review_id=review["review_id"],
            dataset_id=current,
            reason=normalized_reason,
            previous_control=dict(existing) if existing else None,
        )
        save_review_state(project_dir, state)
        return {"editor_control": state["editor_control"], "state": state}


def finish_editor_control(
    project_dir: Path,
    *,
    editor: Actor,
    expected_current_dataset_id: str,
    expected_review_revision: int,
    reason: str = "",
) -> dict:
    if editor.role != "editor":
        raise ReviewForbiddenError("Only editors can release edit control")
    with project_mutation_lock(project_dir):
        state = load_review_state(project_dir)
        _check_expected_revision(state, expected_review_revision)
        current = _check_expected_head(project_dir, expected_current_dataset_id)
        control = state.get("editor_control")
        if not control:
            raise ReviewStateError("Editor control is not active")
        different_owner = control.get("owner_user_id") != editor.user_id
        normalized_reason = " ".join(str(reason or "").strip().split())
        if different_owner and not normalized_reason:
            raise ReviewStateError("A reason is required to release another editor's control")
        state["editor_control"] = None
        _event(
            state,
            "editor_control_released",
            editor,
            review_id=str((active_review(state) or {}).get("review_id") or ""),
            dataset_id=current,
            reason=normalized_reason,
            released_control=dict(control),
        )
        save_review_state(project_dir, state)
        return {"editor_control": None, "state": state}


def normalize_review_decision(value: object) -> str:
    decision = str(value or "").strip().lower()
    if decision not in VALID_REVIEW_DECISIONS:
        raise ReviewStateError("Review decision must be ok, fix_keep, or remove")
    return decision


def _lineage(project_dir: Path, current_dataset_id: str, baseline_dataset_id: str) -> list[str]:
    lineage: list[str] = []
    seen: set[str] = set()
    current = current_dataset_id
    while current:
        if current in seen:
            raise ReviewStateError("Dataset lineage contains a cycle")
        seen.add(current)
        lineage.append(current)
        if current == baseline_dataset_id:
            lineage.reverse()
            return lineage
        current = str(load_dataset(project_dir, current).get("parent_dataset_id") or "")
    raise ReviewStateError("The current dataset is outside the active review lineage")


def _workflow(step: dict) -> tuple[str, dict]:
    parameters = step.get("parameters") or {}
    workflow = parameters.get("workflow") if isinstance(parameters, dict) else None
    if not isinstance(workflow, dict):
        return "changes_individual_scope", {"scope": "all"}
    effect = str(workflow.get("review_effect") or "").strip()
    if effect not in VALID_REVIEW_EFFECTS:
        effect = "changes_individual_scope"
    impact = workflow.get("review_impact")
    return effect, dict(impact) if isinstance(impact, dict) else {"scope": "all"}


def review_scope(project_dir: Path, review: dict, *, current_dataset_id: str | None = None) -> dict:
    current = current_dataset_id or str(load_project_state(project_dir)["current_dataset_id"])
    baseline = str(review.get("baseline_dataset_id") or "")
    lineage = _lineage(project_dir, current, baseline)
    position = {dataset_id: index for index, dataset_id in enumerate(lineage)}
    required_since = {str(item): 0 for item in review.get("initial_individuals") or [] if str(item)}
    history_steps = list_history(project_dir)["steps"]
    steps_by_output = {
        str(step.get("output_dataset_id") or ""): step
        for step in history_steps
    }
    known_step_ids = {
        str(step.get("step_id") or "")
        for step in history_steps
        if str(step.get("step_id") or "")
    }
    step_output_position = {
        str(step.get("step_id") or ""): position[output_dataset_id]
        for output_dataset_id, step in steps_by_output.items()
        if output_dataset_id in position and str(step.get("step_id") or "")
    }
    for index, dataset_id in enumerate(lineage[1:], start=1):
        step = steps_by_output.get(dataset_id)
        if step is None:
            continue
        effect, impact = _workflow(step)
        if effect != "changes_individual_scope":
            continue
        removed = {str(item).strip() for item in impact.get("removed_individuals") or [] if str(item).strip()}
        added = {str(item).strip() for item in impact.get("added_individuals") or [] if str(item).strip()}
        changed = {str(item).strip() for item in impact.get("changed_individuals") or [] if str(item).strip()}
        for individual in removed:
            required_since.pop(individual, None)
        if str(impact.get("scope") or "all") == "all":
            for individual in list(required_since):
                required_since[individual] = index
        for individual in added | changed:
            required_since[individual] = index
    return {
        "lineage": lineage,
        "position": position,
        "required_since": required_since,
        "required_individuals": sorted(required_since),
        "known_step_ids": known_step_ids,
        "step_output_position": step_output_position,
    }


def review_coverage(
    project_dir: Path,
    review: dict,
    annotations: Iterable[dict],
    *,
    current_dataset_id: str | None = None,
) -> dict:
    scope = review_scope(project_dir, review, current_dataset_id=current_dataset_id)
    latest = _valid_review_decisions(review, annotations, scope)
    required = scope["required_individuals"]
    remaining = [individual for individual in required if individual not in latest]
    needs_check = [
        individual
        for individual in required
        if individual in latest and latest[individual].get("needs_check") is True
    ]
    return {
        "required_count": len(required),
        "reviewed_count": len(required) - len(remaining),
        "remaining_count": len(remaining),
        "remaining_individuals": remaining,
        "needs_check_count": len(needs_check),
        "needs_check_individuals": needs_check,
        "complete_allowed": not remaining,
    }


def _valid_review_decisions(
    review: dict,
    annotations: Iterable[dict],
    scope: dict,
) -> dict[str, dict]:
    latest: dict[str, tuple[tuple[int, int], dict]] = {}
    review_id = str(review.get("review_id") or "")
    for annotation_index, annotation in enumerate(annotations):
        if annotation.get("annotation_kind") != "individual_review" or not annotation.get("reviewed"):
            continue
        if str(annotation.get("review_id") or "") != review_id:
            continue
        individual = str((annotation.get("scope") or {}).get("individual") or "").strip()
        step_id = str(annotation.get("step_id") or "")
        if step_id and step_id in scope["known_step_ids"]:
            dataset_position = scope["step_output_position"].get(step_id)
            if dataset_position is None:
                # The decision belongs to an abandoned descendant or sibling branch.
                continue
        else:
            # Legacy annotations predate reliable step provenance and identify the
            # dataset on which their decision step was based.
            dataset_id = str(annotation.get("source_dataset_id") or "")
            dataset_position = scope["position"].get(dataset_id)
        if individual not in scope["required_since"] or dataset_position is None:
            continue
        if dataset_position < scope["required_since"][individual]:
            continue
        decision = normalize_review_decision(annotation.get("review_decision"))
        order = (dataset_position, annotation_index)
        if individual not in latest or order >= latest[individual][0]:
            latest[individual] = (order, {**annotation, "review_decision": decision})
    return {individual: item[1] for individual, item in latest.items()}


def valid_review_decisions(
    project_dir: Path,
    review: dict,
    annotations: Iterable[dict],
    *,
    current_dataset_id: str | None = None,
) -> dict[str, dict]:
    scope = review_scope(project_dir, review, current_dataset_id=current_dataset_id)
    return _valid_review_decisions(review, annotations, scope)


def carryover_needs_checks(
    project_dir: Path,
    review: dict,
    annotations: Iterable[dict],
    *,
    current_dataset_id: str | None = None,
) -> list[str]:
    annotation_list = list(annotations)
    scope = review_scope(project_dir, review, current_dataset_id=current_dataset_id)
    current = _valid_review_decisions(review, annotation_list, scope)
    previous_latest = prior_review_decisions(
        project_dir,
        review,
        annotation_list,
        current_dataset_id=current_dataset_id,
    )
    return sorted(
        individual
        for individual, item in previous_latest.items()
        if individual not in current and item.get("needs_check") is True
    )


def prior_review_decisions(
    project_dir: Path,
    review: dict,
    annotations: Iterable[dict],
    *,
    current_dataset_id: str | None = None,
) -> dict[str, dict]:
    """Return valid decisions from the immediately preceding completed review."""
    state = load_review_state(project_dir)
    prior_review = preceding_completed_review(
        state,
        review_id=str(review.get("review_id") or ""),
    )
    if prior_review is None:
        return {}
    current_scope = review_scope(project_dir, review, current_dataset_id=current_dataset_id)
    try:
        prior_scope = review_scope(
            project_dir,
            prior_review,
            current_dataset_id=str(prior_review["final_dataset_id"]),
        )
    except ReviewStateError:
        return {}
    valid = _valid_review_decisions(prior_review, annotations, prior_scope)
    reviewer = _normalize_actor_snapshot(prior_review.get("reviewer"))
    result: dict[str, dict] = {}
    for individual in current_scope["required_individuals"]:
        annotation = valid.get(individual)
        if annotation is None:
            continue
        result[individual] = {
            **annotation,
            "review_decision": str(annotation.get("review_decision") or ""),
            "needs_check": annotation.get("needs_check") is True,
            "review_id": str(prior_review.get("review_id") or ""),
            "review_round": review_round_number(state, prior_review),
            "reviewer": reviewer,
            "reviewed_at": str(
                annotation.get("created_at")
                or annotation.get("reviewed_at")
                or prior_review.get("completed_at")
                or ""
            ),
            "annotation_id": str(annotation.get("annotation_id") or ""),
        }
    return result


def complete_review(
    project_dir: Path,
    *,
    actor: Actor,
    expected_current_dataset_id: str,
    expected_review_revision: int,
    annotations: Iterable[dict],
    reason: str = "",
) -> dict:
    with project_mutation_lock(project_dir):
        state = load_review_state(project_dir)
        _check_expected_revision(state, expected_review_revision)
        current = _check_expected_head(project_dir, expected_current_dataset_id)
        review = active_review(state)
        if review is None:
            raise ReviewStateError("The study has no active review")
        assigned = str(review.get("reviewer_user_id") or "") == actor.user_id
        if actor.role != "editor" and not assigned:
            raise ReviewForbiddenError("Only the assigned reviewer or an editor can complete this review")
        normalized_reason = " ".join(str(reason or "").strip().split())
        if actor.role == "editor" and not assigned and not normalized_reason:
            raise ReviewStateError("An editor reason is required when completing for a reviewer")
        coverage = review_coverage(project_dir, review, annotations, current_dataset_id=current)
        if not coverage["complete_allowed"]:
            raise ReviewLockedError(
                f"{coverage['remaining_count']} individual(s) still require review",
                code="review_incomplete",
            )
        review["status"] = "completed"
        review["final_dataset_id"] = current
        review["completed_at"] = now_iso()
        review["completed_by"] = actor.as_dict()
        review["completion_reason"] = normalized_reason
        review["completion_coverage"] = coverage
        state["editor_control"] = None
        _event(
            state,
            "review_completed",
            actor,
            review_id=review["review_id"],
            dataset_id=current,
            reason=normalized_reason,
        )
        save_review_state(project_dir, state)
        return {"review": review, "coverage": coverage, "state": state}


def authorize_analysis(project_dir: Path, actor: Actor) -> dict | None:
    state = load_review_state(project_dir)
    review = review_with_provenance(state, active_review(state))
    if actor.role == "editor":
        return review
    if review is None or str(review.get("reviewer_user_id") or "") != actor.user_id:
        raise ReviewForbiddenError("Only the assigned reviewer may run analyses")
    if state.get("editor_control"):
        raise ReviewLockedError("An editor currently has edit control", code="editor_control")
    return review


def authorize_persistent_change(
    project_dir: Path,
    actor: Actor,
    *,
    expected_review_revision: object,
    review_effect: str = "annotation_only",
) -> dict | None:
    state = load_review_state(project_dir)
    _check_expected_revision(state, expected_review_revision)
    review = review_with_provenance(state, active_review(state))
    control = state.get("editor_control")
    assigned = bool(review and str(review.get("reviewer_user_id") or "") == actor.user_id)
    if actor.role == "editor":
        if review is not None and assigned and review_effect != "changes_individual_scope" and not control:
            return review
        if review is not None and (not control or control.get("owner_user_id") != actor.user_id):
            raise ReviewLockedError(
                "Take editor control before changing an active review",
                code="editor_control_required",
            )
        return review
    if review_effect == "changes_individual_scope":
        raise ReviewForbiddenError("Only editors can apply dataset updates")
    if review is None or str(review.get("reviewer_user_id") or "") != actor.user_id:
        raise ReviewForbiddenError("Only the assigned reviewer may change this study")
    if control:
        raise ReviewLockedError(
            f"Editor control is held by {control.get('owner_display_name') or 'an editor'}",
            code="editor_control",
        )
    return review


def review_profile(project_dir: Path, actor: Actor, annotations: Iterable[dict] = ()) -> dict:
    annotation_list = list(annotations)
    state = load_review_state(project_dir)
    review = review_with_provenance(state, active_review(state))
    assigned = bool(review and review.get("reviewer_user_id") == actor.user_id)
    control = state.get("editor_control")
    editor_owns_control = bool(control and control.get("owner_user_id") == actor.user_id)
    can_review = (
        (assigned and not control)
        or (actor.role == "editor" and (review is None or editor_owns_control))
    )
    coverage = review_coverage(project_dir, review, annotation_list) if review else None
    if review and coverage is not None:
        prior_decisions = prior_review_decisions(project_dir, review, annotation_list)
        coverage["prior_decisions_by_individual"] = prior_decisions
        carryover = sorted(
            individual
            for individual, item in prior_decisions.items()
            if item.get("needs_check") is True
        )
        coverage["prior_needs_check_count"] = len(carryover)
        coverage["prior_needs_check_individuals"] = carryover
    return {
        "actor": actor.as_dict(),
        "review_revision": int(state.get("revision") or 0),
        "review": dict(review) if review else None,
        "editor_control": dict(control) if control else None,
        "coverage": coverage,
        "capabilities": {
            "can_read": actor.role == "editor" or actor_has_review_history(state, actor),
            "can_review": can_review,
            "can_analyze": actor.role == "editor" or (assigned and not control),
            "can_complete": bool(review and coverage and coverage["complete_allowed"] and (assigned or actor.role == "editor")),
            "can_manage_assignment": actor.role == "editor",
            "can_intervene": actor.role == "editor" and review is not None and not assigned,
            "can_update_dataset": actor.role == "editor" and (review is None or editor_owns_control),
            "can_undo": can_review,
        },
    }
