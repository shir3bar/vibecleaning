from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from threading import RLock


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
