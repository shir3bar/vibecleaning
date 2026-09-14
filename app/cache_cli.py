from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

from .filesystem import FileLockTimeoutError, exclusive_file_lock
from .runtime import resolve_cache_root


def _rds_cache_entries(cache_root: Path) -> list[Path]:
    directory = cache_root / "movement" / "rds"
    if not directory.exists():
        return []
    return sorted(directory.glob("*.sqlite"))


def clear_rds_cache(
    cache_root: Path,
    *,
    remove_all: bool = False,
    older_than_days: int | None = None,
) -> list[Path]:
    if remove_all == (older_than_days is not None):
        raise ValueError("Choose exactly one of --all or --older-than-days")
    if older_than_days is not None and older_than_days < 0:
        raise ValueError("--older-than-days must be zero or greater")

    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=older_than_days)
        if older_than_days is not None
        else None
    )
    removed: list[Path] = []
    for path in _rds_cache_entries(Path(cache_root)):
        if cutoff is not None:
            modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            if modified >= cutoff:
                continue
        lock_path = path.with_suffix(".lock")
        with exclusive_file_lock(lock_path, mode="required"):
            if not path.exists():
                continue
            path.unlink()
            removed.append(path)
    return removed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage disposable Vibecleaning caches")
    parser.add_argument("--cache-root", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    clear_rds = commands.add_parser("clear-rds", help="Remove local RDS SQLite indexes")
    selection = clear_rds.add_mutually_exclusive_group(required=True)
    selection.add_argument("--all", action="store_true", help="Remove every RDS index")
    selection.add_argument("--older-than-days", type=int, metavar="N")
    args = parser.parse_args(argv)

    try:
        removed = clear_rds_cache(
            resolve_cache_root(args.cache_root),
            remove_all=bool(args.all),
            older_than_days=args.older_than_days,
        )
    except (OSError, ValueError, FileLockTimeoutError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(f"Removed {len(removed)} local RDS cache file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
