from __future__ import annotations

from contextlib import contextmanager
import json
import logging
import os
from pathlib import Path
import tempfile
from threading import Lock, RLock, local
import time
from typing import Iterator

import portalocker

from .runtime import COOPERATIVE_MODE_WARNING, shared_locking_mode


DEFAULT_LOCK_TIMEOUT_SECONDS = 15.0
DEFAULT_REPLACE_ATTEMPTS = 6
_TRANSIENT_WINERRORS = frozenset({5, 32, 33})
_registry_guard = Lock()
_process_locks: dict[str, RLock] = {}
_thread_state = local()
_cooperative_warning_emitted = False


class FileLockTimeoutError(TimeoutError):
    pass


def _process_lock(path: Path) -> RLock:
    key = os.path.normcase(os.path.abspath(str(path)))
    with _registry_guard:
        return _process_locks.setdefault(key, RLock())


def _lock_depths() -> dict[str, int]:
    depths = getattr(_thread_state, "lock_depths", None)
    if depths is None:
        depths = {}
        _thread_state.lock_depths = depths
    return depths


@contextmanager
def exclusive_file_lock(
    path: Path,
    *,
    mode: str | None = None,
    timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> Iterator[None]:
    global _cooperative_warning_emitted
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    key = os.path.normcase(os.path.abspath(str(path)))
    process_lock = _process_lock(path)
    with process_lock:
        depths = _lock_depths()
        if depths.get(key, 0):
            depths[key] += 1
            try:
                yield
            finally:
                depths[key] -= 1
            return

        configured_mode = shared_locking_mode(mode)
        depths[key] = 1
        try:
            if configured_mode == "disabled":
                if not _cooperative_warning_emitted:
                    logging.getLogger(__name__).warning(COOPERATIVE_MODE_WARNING)
                    _cooperative_warning_emitted = True
                yield
                return
            try:
                with portalocker.Lock(str(path), mode="a+", timeout=float(timeout)):
                    yield
            except portalocker.exceptions.LockException as exc:
                raise FileLockTimeoutError(
                    f"Timed out waiting for the shared lock: {path}"
                ) from exc
        finally:
            depths.pop(key, None)


def _is_transient_replace_error(exc: OSError) -> bool:
    return isinstance(exc, PermissionError) or getattr(exc, "winerror", None) in _TRANSIENT_WINERRORS


def atomic_replace(
    source: Path,
    destination: Path,
    *,
    attempts: int = DEFAULT_REPLACE_ATTEMPTS,
) -> None:
    source = Path(source)
    destination = Path(destination)
    delay = 0.05
    for attempt in range(max(1, int(attempts))):
        try:
            os.replace(source, destination)
            _sync_directory(destination.parent)
            return
        except OSError as exc:
            if attempt + 1 >= attempts or not _is_transient_replace_error(exc):
                raise
            time.sleep(delay)
            delay = min(delay * 2, 0.8)


def _sync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, content: bytes, *, mode: int | None = None) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if mode is not None and os.name != "nt":
            temporary.chmod(mode)
        atomic_replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def atomic_write_text(
    path: Path,
    content: str,
    *,
    encoding: str = "utf-8",
    mode: int | None = None,
) -> None:
    atomic_write_bytes(path, content.encode(encoding), mode=mode)


def atomic_write_json(path: Path, payload: object, *, mode: int | None = None) -> None:
    atomic_write_text(
        path,
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        mode=mode,
    )
