import argparse
from pathlib import Path

from app.state import ensure_project_state
from examples.movement.rds_index import ensure_rds_index, import_flat_rds_studies


def main() -> int:
    parser = argparse.ArgumentParser(description="Import flat movement RDS files by study")
    parser.add_argument("source", type=Path)
    parser.add_argument("--family-dir", type=Path)
    parser.add_argument("--build-index", action="store_true")
    args = parser.parse_args()
    result = import_flat_rds_studies(args.source, args.family_dir)
    family_dir = Path(result["family_dir"])
    if args.build_index:
        for item in result["studies"]:
            study_dir = family_dir / str(item["study_id"])
            state = ensure_project_state(study_dir)
            ensure_rds_index(study_dir, str(state["current_dataset_id"]))
    print(
        f"Imported {result['study_count']} studies; "
        f"copied {result['copied_file_count']} files, skipped {result['skipped_file_count']}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
