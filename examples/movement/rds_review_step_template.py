import json
import os
from datetime import datetime, timezone
from pathlib import Path


REVIEW_SIDECAR_NAME = "movement_review_annotations.json"


def main():
    spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    params = dict(spec["step"].get("parameters") or {})
    step_id = str(spec["step"].get("step_id") or "").strip()
    from examples.movement.review_annotations import load_review_annotations

    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    existing = inputs.get(REVIEW_SIDECAR_NAME)
    output = outputs.get(REVIEW_SIDECAR_NAME)
    if output is None:
        raise SystemExit("Movement review sidecar output was not declared")
    annotations = load_review_annotations(Path(existing["path"]) if existing else None)
    requested = list(params.get("records") or [])
    if not requested:
        raise SystemExit("RDS review step did not declare any records")
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    appended = []
    for index, raw in enumerate(requested, start=1):
        record = dict(raw)
        record.update({
            "annotation_id": step_id if len(requested) == 1 else f"{step_id}:scope:{index:04d}",
            "step_id": step_id,
            "source_id": str(params.get("source_bundle_signature") or ""),
            "source_dataset_id": str(params.get("dataset_id") or ""),
            "user": str(params.get("user") or ""),
            "actor": dict(params.get("actor") or {}),
            "review_id": str(params.get("review_id") or ""),
            "created_at": created_at,
        })
        annotations.append(record)
        appended.append(record)
    output_path = Path(output["path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps({"schema_version": 6, "annotations": annotations}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary = {
        "app": "movement",
        "action": str(params.get("action") or "rds_review"),
        "annotation_id": appended[0]["annotation_id"],
        "annotation_ids": [item["annotation_id"] for item in appended],
        "annotation_count": len(appended),
        "resolved_fix_count": sum(int(item.get("resolved_fix_count") or 0) for item in appended),
        "source_bundle_signature": str(params.get("source_bundle_signature") or ""),
        "materialized_rds": False,
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
