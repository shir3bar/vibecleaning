"""Save a reproducible annotated bundle; publication is a separate guarded action."""
import hashlib
import json
import os
from pathlib import Path


def main():
    from app.filesystem import atomic_write_json
    from app.state import now_iso
    from examples.movement.decision_export import save_decision_export
    from examples.movement.review_annotations import export_reviewed_csv

    spec_path = Path(os.environ["SCRUBDATA_SPEC_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    params = spec["analysis"]["parameters"]
    if params["source_format"] == "rds":
        from examples.movement.export_reviewed_rds_analysis_template import main as export_rds
        export_rds()
        return
    inputs = {item["logical_name"]: item for item in spec["input_artifacts"]}
    output_dir = Path(spec["output_artifacts"][0]["path"]).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    sidecar = inputs.get("movement_review_annotations.json")
    sources = [(name, item) for name, item in inputs.items() if name.lower().endswith(".csv")]
    files = []
    for name, item in sources:
        atomic_write_json(spec_path.with_name("progress.json"), {
            "stage": "writing", "completed_files": len(files), "total_files": len(sources), "logical_name": name})
        source = Path(item["path"])
        output = output_dir / (Path(name).stem + "_cleaned.csv")
        summary = export_reviewed_csv(source, output, source_artifact=name,
            sidecar_path=Path(sidecar["path"]) if sidecar else None,
            allowed_individual_review_annotation_ids=set(params["valid_individual_review_annotation_ids"]))
        files.append({"logical_name": name, "output_name": output.name,
            "row_count": summary["exported_row_count"],
            "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "output_sha256": hashlib.sha256(output.read_bytes()).hexdigest()})
    manifest = {"schema_version": 2, "dataset_id": spec["dataset"]["dataset_id"],
        "analysis_id": spec["analysis"]["analysis_id"], "user": spec["analysis"]["user"],
        "actor": spec["analysis"].get("actor"), "exported_at": now_iso(),
        "review_id": params["review_id"], "review_round": params["review_round"],
        "review_revision": params["review_revision"], "files": files,
        "auxiliary_files": [save_decision_export(output_dir, params["individual_decisions"])]}
    atomic_write_json(output_dir / "writer_manifest.json", manifest)
    atomic_write_json(Path(os.environ["SCRUBDATA_SUMMARY_PATH"]), {
        "file_count": len(files), "row_count": sum(item["row_count"] for item in files),
        "output_directory": str(output_dir), "output_artifact": "writer_manifest.json"})


if __name__ == "__main__":
    main()
