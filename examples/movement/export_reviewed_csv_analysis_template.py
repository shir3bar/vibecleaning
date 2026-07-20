import json
import os
import sys
from pathlib import Path


REVIEW_SIDECAR_NAME = "movement_review_annotations.json"


def main():
    spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    params = dict(spec["analysis"].get("parameters") or {})
    repo_root = str(params.get("repo_root") or "").strip()
    if repo_root:
        sys.path.insert(0, repo_root)

    from examples.movement.review_annotations import export_reviewed_csv
    from app.state import list_history

    target_artifact = str(params.get("target_artifact") or "").strip()
    output_artifact = str(params.get("output_artifact") or "movement_reviewed.csv").strip()
    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    source = inputs.get(target_artifact)
    output = outputs.get(output_artifact)
    if source is None or output is None:
        raise SystemExit("Reviewed CSV input or output was not declared")
    sidecar = inputs.get(REVIEW_SIDECAR_NAME)
    annotation_step_ids = {}
    for step in list_history(Path(spec["project_dir"]))["steps"]:
        parameters = dict(step.get("parameters") or {})
        if parameters.get("action") != "annotate_scope":
            continue
        legacy_annotation_id = str(parameters.get("annotation_id") or "").strip()
        if legacy_annotation_id:
            annotation_step_ids[legacy_annotation_id] = str(step.get("step_id") or "").strip()
    summary = export_reviewed_csv(
        Path(source["path"]),
        Path(output["path"]),
        source_artifact=target_artifact,
        sidecar_path=Path(sidecar["path"]) if sidecar else None,
        annotation_step_ids=annotation_step_ids,
    )
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
