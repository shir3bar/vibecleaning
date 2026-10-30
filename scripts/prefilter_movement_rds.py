"""Create optional study-filter steps beside untouched RDS inputs."""
from __future__ import annotations

import argparse
import getpass
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.edit_locks import create_guarded_step
from app.state import ensure_project_state, load_project_state, load_dataset, project_paths
from examples.movement.study_filters import StudyFilters, STAGES, step_payload


def prefilter_folder(folder, *, user=None, refresh=False, dry_run=False, stages=STAGES):
    from examples.movement.routes import RDS_REVIEW_STEP_SCRIPT
    folder = Path(folder).resolve()
    if not folder.is_dir():
        raise ValueError(f"Movement folder does not exist: {folder}")
    report = {"folder": str(folder), "dry_run": dry_run, "studies": [], "failures": []}
    studies = [p for p in sorted(folder.iterdir()) if p.is_dir() and not p.name.startswith(".") and any(p.glob("*.rds"))]
    if not studies:
        raise ValueError("No study folders containing RDS files found")
    for study in studies:
        entry = {"study": study.name, "filters": []}
        report["studies"].append(entry)
        try:
            initialized = project_paths(study)["project"].exists()
            if dry_run and not initialized:
                entry["would_initialize"] = True
                entry["filters"] = [{"stage": s, "action": "would_run"} for s in stages]
                continue
            state = load_project_state(study) if initialized else ensure_project_state(study)
            for stage in stages:
                dataset_id = state["current_dataset_id"]
                context = StudyFilters(study, dataset_id)
                status = next(s for s in context.status()["stages"] if s["stage"] == stage)
                if status["saved"] and (not refresh or status["status"] == "current"):
                    entry["filters"].append({"stage": stage, "action": "skipped", "status": status["status"]})
                    continue
                if dry_run:
                    entry["filters"].append({"stage": stage, "action": "would_run", "status": status["status"]})
                    continue
                preview = context.preview(stage)
                dataset = load_dataset(study, dataset_id)
                names = [a["logical_name"] for a in dataset["artifacts"] if a["logical_name"].lower().endswith(".rds") or a["logical_name"] == "movement_review_annotations.json"]
                payload = step_payload(preview, user=user or getpass.getuser(), script=RDS_REVIEW_STEP_SCRIPT, source_artifacts=names)
                result = create_guarded_step(study, payload, selected_dataset_id=dataset_id, expected_current_dataset_id=dataset_id)
                state = {"current_dataset_id": result["dataset"]["dataset_id"]}
                summary = {"stage": stage, "action": "applied", "dataset_id": state["current_dataset_id"],
                    "added_count": preview["added_count"], "removed_count": preview["removed_count"],
                    "statistics": preview["snapshot"]["statistics"]}
                entry["filters"].append(summary)
                print(f"{study.name}: {stage}: {preview['added_count']} new; {preview['removed_count']} obsolete", flush=True)
        except Exception as exc:
            failure = {"study": study.name, "error": str(exc)}
            report["failures"].append(failure)
            print(f"{study.name}: FAILED: {exc}", file=sys.stderr, flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("movement_rds", type=Path)
    parser.add_argument("--user", default=getpass.getuser())
    parser.add_argument("--dry-run", action="store_true", help="Report intended work without creating review state")
    parser.add_argument("--resume", action="store_true", help="Resume missing filters (also the default); matching saved runs are skipped")
    parser.add_argument("--refresh", action="store_true", help="Explicitly recalculate outdated saved filters, retaining human decisions")
    parser.add_argument("--filter", choices=STAGES, action="append", dest="stages", help="Limit work to named filters")
    parser.add_argument("--report", type=Path, help="Optional JSON report destination")
    args = parser.parse_args()
    try:
        requested = tuple(s for s in STAGES if not args.stages or s in args.stages)
        result = prefilter_folder(args.movement_rds, user=args.user, dry_run=args.dry_run, refresh=args.refresh, stages=requested)
    except ValueError as exc:
        parser.error(str(exc))
    if args.report:
        args.report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 1 if result["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
