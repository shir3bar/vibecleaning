from __future__ import annotations

import shutil
from pathlib import Path

from examples.movement.catalog import detect_study_slug


ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = ROOT / "data"
LEGACY_ROOT = DATA_ROOT / "movement_example"

VARIANT_TO_FAMILY = {
    "raw_v1": "movement_raw",
    "cleaned_topout_v1": "movement_clean",
    "cleaned_hightemporalres": "movement_hightemporalres",
}
def copy_file(source_path: Path, dest_path: Path):
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.exists():
        if source_path.read_bytes() != dest_path.read_bytes():
            raise ValueError(f"Destination already exists with different contents: {dest_path}")
        return
    shutil.copy2(source_path, dest_path)


def migrate_variant(variant_name: str, family_name: str) -> list[tuple[Path, Path]]:
    source_dir = LEGACY_ROOT / variant_name
    family_dir = DATA_ROOT / family_name
    migrated = []
    if not source_dir.exists() or not source_dir.is_dir():
        return migrated
    for source_path in sorted(source_dir.iterdir()):
        if source_path.name.startswith(".") or not source_path.is_file():
            continue
        study_slug = detect_study_slug(source_path.name)
        dest_path = family_dir / study_slug / source_path.name
        copy_file(source_path, dest_path)
        migrated.append((source_path, dest_path))
    return migrated


def main():
    all_migrated = []
    for variant_name, family_name in VARIANT_TO_FAMILY.items():
        migrated = migrate_variant(variant_name, family_name)
        all_migrated.extend(migrated)
        print(f"{variant_name} -> {family_name}: {len(migrated)} file(s)")
        for _, dest_path in migrated:
            print(f"  {dest_path.relative_to(ROOT)}")
    print(f"Total migrated files: {len(all_migrated)}")


if __name__ == "__main__":
    main()
