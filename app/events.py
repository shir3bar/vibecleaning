from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
import hashlib
import json
from pathlib import Path
from threading import RLock

from .reviews import active_review, load_review_state
from .state import load_project_state


def _fingerprint(payload: object) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def shared_study_state(project_dir: Path) -> dict:
    """Return the cheap shared fields polled by each local app instance."""
    state = load_review_state(project_dir)
    review = active_review(state)
    assignment = {
        "review_id": str((review or {}).get("review_id") or ""),
        "status": str((review or {}).get("status") or ""),
        "reviewer_user_id": str((review or {}).get("reviewer_user_id") or ""),
        "baseline_dataset_id": str((review or {}).get("baseline_dataset_id") or ""),
    }
    raw_control = state.get("editor_control")
    control = raw_control if isinstance(raw_control, dict) else {}
    editor_control = {
        "review_id": str(control.get("review_id") or ""),
        "owner_user_id": str(control.get("owner_user_id") or ""),
        "started_at": str(control.get("started_at") or ""),
    }
    current_dataset_id = str(load_project_state(project_dir)["current_dataset_id"])
    review_revision = int(state.get("revision") or 0)
    assignment_fingerprint = _fingerprint(assignment)
    editor_control_fingerprint = _fingerprint(editor_control)
    return {
        "review_revision": review_revision,
        "current_dataset_id": current_dataset_id,
        "assignment_fingerprint": assignment_fingerprint,
        "editor_control_fingerprint": editor_control_fingerprint,
        "state_fingerprint": _fingerprint(
            [
                current_dataset_id,
                review_revision,
                assignment_fingerprint,
                editor_control_fingerprint,
            ]
        ),
    }


class StudyEventBroker:
    """Small single-process broadcaster for study-state invalidation events."""

    def __init__(self):
        self._lock = RLock()
        self._queues: dict[str, set[asyncio.Queue]] = defaultdict(set)

    @asynccontextmanager
    async def subscribe(self, key: str):
        queue: asyncio.Queue = asyncio.Queue(maxsize=8)
        with self._lock:
            self._queues[key].add(queue)
        try:
            yield queue
        finally:
            with self._lock:
                queues = self._queues.get(key)
                if queues is not None:
                    queues.discard(queue)
                    if not queues:
                        self._queues.pop(key, None)

    def publish(self, key: str, payload: dict) -> None:
        with self._lock:
            queues = list(self._queues.get(key) or ())
        for queue in queues:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(dict(payload))
            except asyncio.QueueFull:
                pass
