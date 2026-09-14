from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator


DATA_ROOT_ENV = "VIBECLEANING_DATA_ROOT"
CACHE_ROOT_ENV = "VIBECLEANING_CACHE_ROOT"
SHARED_LOCKING_ENV = "VIBECLEANING_SHARED_LOCKING"
SHARED_LOCKING_MODES = frozenset({"required", "disabled"})
COOPERATIVE_MODE_WARNING = (
    "Cross-machine locking is disabled. Coordinate one active writer at a time; "
    "atomic writes and stale-state checks remain enabled."
)
_request_shared_locking: ContextVar[str | None] = ContextVar(
    "vibecleaning_shared_locking",
    default=None,
)


class RuntimeConfigurationError(ValueError):
    pass


def _normalized_path(value: str | os.PathLike[str] | Path) -> Path:
    return Path(value).expanduser().resolve()


def resolve_data_root(
    explicit: str | os.PathLike[str] | Path | None = None,
    *,
    default: str | os.PathLike[str] | Path | None = None,
) -> Path:
    if explicit is not None:
        return _normalized_path(explicit)
    configured = os.environ.get(DATA_ROOT_ENV, "").strip()
    if configured:
        return _normalized_path(configured)
    if default is None:
        raise RuntimeConfigurationError(
            f"Set {DATA_ROOT_ENV} or provide an explicit data root"
        )
    return _normalized_path(default)


def default_cache_root() -> Path:
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA", "").strip()
        if base:
            return _normalized_path(Path(base) / "Vibecleaning" / "cache")
        return _normalized_path(Path.home() / "AppData" / "Local" / "Vibecleaning" / "cache")
    if sys.platform == "darwin":
        return _normalized_path(Path.home() / "Library" / "Caches" / "Vibecleaning")
    base = os.environ.get("XDG_CACHE_HOME", "").strip()
    if base:
        return _normalized_path(Path(base) / "vibecleaning")
    return _normalized_path(Path.home() / ".cache" / "vibecleaning")


def resolve_cache_root(
    explicit: str | os.PathLike[str] | Path | None = None,
) -> Path:
    if explicit is not None:
        return _normalized_path(explicit)
    configured = os.environ.get(CACHE_ROOT_ENV, "").strip()
    if configured:
        return _normalized_path(configured)
    return default_cache_root()


def shared_locking_mode(explicit: str | None = None) -> str:
    request_mode = _request_shared_locking.get()
    value = str(
        explicit
        if explicit is not None
        else request_mode
        if request_mode is not None
        else os.environ.get(SHARED_LOCKING_ENV, "required")
    ).strip().lower()
    if value not in SHARED_LOCKING_MODES:
        supported = ", ".join(sorted(SHARED_LOCKING_MODES))
        raise RuntimeConfigurationError(
            f"{SHARED_LOCKING_ENV} must be one of: {supported}"
        )
    return value


@contextmanager
def shared_locking_scope(mode: str) -> Iterator[None]:
    normalized = shared_locking_mode(mode)
    token = _request_shared_locking.set(normalized)
    try:
        yield
    finally:
        _request_shared_locking.reset(token)


def _probe_writable_directory(path: Path, *, label: str, create: bool) -> Path:
    try:
        if create:
            path.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            raise RuntimeConfigurationError(f"{label} does not exist: {path}")
        if not path.is_dir():
            raise RuntimeConfigurationError(f"{label} is not a directory: {path}")
        with tempfile.NamedTemporaryFile(prefix=".vibecleaning-write-probe-", dir=path):
            pass
    except RuntimeConfigurationError:
        raise
    except OSError as exc:
        raise RuntimeConfigurationError(f"{label} is not writable: {path}: {exc}") from exc
    return path


def validate_data_root(path: Path) -> Path:
    path = _normalized_path(path)
    if not path.exists():
        raise RuntimeConfigurationError(f"Data root does not exist: {path}")
    if not path.is_dir():
        raise RuntimeConfigurationError(f"Data root is not a directory: {path}")
    return _probe_writable_directory(
        path / ".vibecleaning",
        label="Data-root metadata directory",
        create=True,
    ).parent


def validate_cache_root(path: Path) -> Path:
    return _probe_writable_directory(
        _normalized_path(path),
        label="Cache root",
        create=True,
    )
