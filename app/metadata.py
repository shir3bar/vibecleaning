"""One-time conversion of legacy metadata; normal operation uses scrubdata only."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path, PureWindowsPath
import shutil

from .filesystem import atomic_write_json, exclusive_file_lock


META_DIR_NAME = "scrubdata"
LEGACY_DIR_NAME = ".vibecleaning"
BACKUP_DIR_NAME = ".vibecleaning-backup"
STAGING_DIR_NAME = ".scrubdata-migrating"
MIGRATION_RECORD = "metadata-migration.json"


class MetadataMigrationError(ValueError):
    pass


def _rewrite_path(value: str, project_dir: Path, former_root: str) -> str:
    # Manifests use relative POSIX paths. Specs also contain absolute paths;
    # account for specs saved on Windows or before the study was relocated.
    normalized = value.replace("\\", "/")
    if normalized == LEGACY_DIR_NAME or normalized.startswith(LEGACY_DIR_NAME + "/"):
        return META_DIR_NAME + normalized[len(LEGACY_DIR_NAME):]
    former = former_root.replace("\\", "/").rstrip("/")
    comparable = normalized.casefold() if PureWindowsPath(former).drive else normalized
    prefix = former.casefold() if PureWindowsPath(former).drive else former
    if comparable.startswith(prefix + "/"):
        relative = normalized[len(former) + 1:]
        if relative == LEGACY_DIR_NAME or relative.startswith(LEGACY_DIR_NAME + "/"):
            relative = META_DIR_NAME + relative[len(LEGACY_DIR_NAME):]
        return str(project_dir / relative)
    return value


def _rewrite_record(value, project_dir: Path, former_root: str):
    if isinstance(value, list):
        return [_rewrite_record(item, project_dir, former_root) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        # User parameters, notes and artifact contents are provenance, not paths
        # owned by the framework. Never do a blanket text replacement.
        if key in {"parameters", "metadata", "summary"}:
            result[key] = item
        elif key in {"path", "script_path", "spec_path", "summary_path"} and isinstance(item, str):
            result[key] = _rewrite_path(item, project_dir, former_root)
        elif key == "project_dir" and isinstance(item, str):
            result[key] = str(project_dir)
        else:
            result[key] = _rewrite_record(item, project_dir, former_root)
    return result


def _prepare_copy(source: Path, staging: Path, project_dir: Path) -> None:
    # Reject links instead of following them outside the authoritative history.
    if any(path.is_symlink() for path in source.rglob("*")):
        raise MetadataMigrationError(f"Metadata contains symbolic links; migrate manually: {source}")
    shutil.copytree(source, staging)
    records = list(staging.glob("datasets/*.json"))
    for directory, name in (("steps", "step.json"), ("analyses", "analysis.json")):
        records.extend(staging.glob(f"{directory}/*/{name}"))
        records.extend(staging.glob(f"{directory}/*/spec.json"))
    for path in records:
        original = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(original, dict):
            raise MetadataMigrationError(f"Invalid history record: {path}")
        former_root = original.get("project_dir", str(project_dir))
        if not isinstance(former_root, str) or not former_root:
            raise MetadataMigrationError(f"Invalid project directory in history record: {path}")
        updated = _rewrite_record(original, project_dir, former_root)
        if updated != original:
            atomic_write_json(path, updated)
    # Written last: a partial copy must never be published as live state.
    atomic_write_json(staging / MIGRATION_RECORD, {
        "version": 1,
        "from": LEGACY_DIR_NAME,
        "to": META_DIR_NAME,
        "backup": BACKUP_DIR_NAME,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })


def metadata_dir(directory: Path) -> Path:
    """Migrate when first encountered. All old app instances must be stopped."""
    directory = Path(directory).resolve()
    legacy = directory / LEGACY_DIR_NAME
    current = directory / META_DIR_NAME
    staging = directory / STAGING_DIR_NAME
    backup = directory / BACKUP_DIR_NAME
    if current.exists() and not current.is_dir():
        raise MetadataMigrationError(f"The metadata path is not a directory: {current}")
    if not legacy.exists() and not legacy.is_symlink() and not staging.exists() and not staging.is_symlink():
        if not current.exists() and backup.exists():
            raise MetadataMigrationError(f"Only the old metadata backup exists at {backup}; restore it before opening.")
        return current

    # Outside both metadata directories so renames also work on Windows.
    # Always coordinate migration, even in cooperative editing mode.
    with exclusive_file_lock(directory / ".scrubdata-migration.lock", mode="required"):
        if legacy.exists() and current.exists():
            raise MetadataMigrationError(
                f"Both {legacy} and {current} exist. Stop all app instances and choose the history to keep; nothing was merged."
            )
        if current.exists():
            return current
        if legacy.is_symlink() or staging.is_symlink() or backup.is_symlink():
            raise MetadataMigrationError(f"Metadata migration does not follow symbolic links: {directory}")
        if not legacy.exists():
            # Recover an interruption between archiving the original and
            # publishing the fully prepared copy. The original is untouched.
            ready = staging / MIGRATION_RECORD
            if backup.is_dir() and ready.is_file():
                record = json.loads(ready.read_text(encoding="utf-8"))
                if (record.get("version") == 1 and record.get("from") == LEGACY_DIR_NAME
                        and record.get("to") == META_DIR_NAME and record.get("backup") == BACKUP_DIR_NAME):
                    staging.rename(current)
                    return current
            raise MetadataMigrationError(f"Incomplete metadata migration at {directory}; restore {backup} before opening.")
        if not legacy.is_dir() or backup.exists():
            raise MetadataMigrationError(f"Cannot migrate {legacy}; check the existing metadata and backup folders.")
        # A failed preparation leaves the original authoritative. Retry from it.
        if staging.exists():
            shutil.rmtree(staging)
        try:
            _prepare_copy(legacy, staging, directory)
            legacy.rename(backup)
            staging.rename(current)
        except (OSError, ValueError) as exc:
            raise MetadataMigrationError(
                f"Could not migrate {legacy}: {exc}. Original history is in {legacy} or {backup}. "
                "Stop all old app instances and retry."
            ) from exc
    return current
