import json
import os
from pathlib import Path


REVIEW_SIDECAR_NAME = "movement_review_annotations.json"


def main():
    spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    params = dict(spec["analysis"].get("parameters") or {})
    from examples.movement.review_annotations import export_reviewed_csv

    target_artifact = str(params.get("target_artifact") or "").strip()
    output_artifact = str(params.get("output_artifact") or "movement_reviewed.csv").strip()
    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    source = inputs.get(target_artifact)
    output = outputs.get(output_artifact)
    if source is None or output is None:
        raise SystemExit("Reviewed CSV input or output was not declared")
    sidecar = inputs.get(REVIEW_SIDECAR_NAME)
    summary = export_reviewed_csv(
        Path(source["path"]),
        Path(output["path"]),
        source_artifact=target_artifact,
        sidecar_path=Path(sidecar["path"]) if sidecar else None,
        allowed_individual_review_annotation_ids=set(
            params.get("valid_individual_review_annotation_ids") or []
        ) if params.get("review_id") else None,
    )
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
