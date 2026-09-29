import json
import os
from pathlib import Path


OUTPUT_ARTIFACT_NAME = "writer_manifest.json"


def main():
    spec_path = Path(os.environ["SCRUBDATA_SPEC_PATH"])
    summary_path = Path(os.environ["SCRUBDATA_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    params = dict(spec["analysis"].get("parameters") or {})
    from examples.movement.rds_export import export_reviewed_rds_bundle
    from examples.movement.rds_index import read_movement_rds, validate_movement_rds
    from examples.movement.review_annotations import load_review_annotations
    from app.filesystem import atomic_write_json
    from app.state import now_iso

    def progress(stage, completed_files, total_files, logical_name=""):
        atomic_write_json(spec_path.with_name("progress.json"), {
            "stage": stage,
            "completed_files": completed_files,
            "total_files": total_files,
            "logical_name": logical_name,
        })

    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    output = outputs.get(OUTPUT_ARTIFACT_NAME)
    if output is None:
        raise SystemExit("Reviewed RDS export manifest was not declared")
    sidecar = inputs.get("movement_review_annotations.json")
    annotations = load_review_annotations(Path(sidecar["path"]) if sidecar else None)
    sources = []
    rows_by_artifact = {}
    total_files = sum(name.lower().endswith(".rds") for name in inputs)
    for logical_name, item in sorted(inputs.items()):
        if not logical_name.lower().endswith(".rds"):
            continue
        source_path = Path(item["path"])
        progress("preparing", len(sources), total_files, logical_name)
        frame = read_movement_rds(source_path)
        info = validate_movement_rds(Path(logical_name), frame)
        rows = []
        for zero_index in range(len(frame)):
            source_row = zero_index + 1
            def value(column):
                if column not in frame.columns:
                    return ""
                raw = frame.iloc[zero_index][column]
                try:
                    missing = bool(raw is None or __import__("pandas").isna(raw))
                except (TypeError, ValueError):
                    missing = raw is None
                return "" if missing else str(raw).strip()
            rows.append({
                "fix_key": f"file:{logical_name}#row:{source_row}",
                "logical_name": logical_name,
                "source_row": source_row,
                "identifier": str(info["individual"]),
                "source_outlier_status": value("outlier_status"),
                "source_outlier_issue_type": value("outlier_issue_type"),
                "source_outlier_comments": value("outlier_comments"),
                "source_outlier_flag_step_ids": value("outlier_flag_step_ids"),
            })
        sources.append((logical_name, source_path))
        rows_by_artifact[logical_name] = rows
    if not sources:
        raise SystemExit("No RDS movement inputs were declared")
    result = export_reviewed_rds_bundle(
        sources=sources,
        rows_by_artifact=rows_by_artifact,
        annotations=annotations,
        output_dir=Path(output["path"]).parent,
        # Use the directory containing this analysis; do not initiate metadata
        # migration from a worker while an export is already in progress.
        cleaned_dir=spec_path.parents[2] / "cleaned_files",
        provenance={
            "dataset_id": params["dataset_id"],
            "analysis_id": spec["analysis"]["analysis_id"],
            "user": spec["analysis"]["user"],
            "exported_at": now_iso(),
        },
        writer=os.environ.get("VIBECLEANING_RDS_WRITER", params.get("writer", "auto")),
        progress=progress,
    )
    summary_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
