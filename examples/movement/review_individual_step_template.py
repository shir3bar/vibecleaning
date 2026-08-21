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

    target_artifact = str(params.get("target_artifact") or "").strip()
    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    if target_artifact not in inputs or REVIEW_SIDECAR_NAME not in outputs:
        raise SystemExit("Movement source or review sidecar output was not declared")

    existing = inputs.get(REVIEW_SIDECAR_NAME)
    annotations = load_review_annotations(Path(existing["path"]) if existing else None)
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    decision = dict(params.get("decision") or {})
    individual = str(decision.get("individual") or "").strip()
    if not individual:
        raise SystemExit("Individual review decision is missing an individual")
    annotation = {
        "annotation_id": f"{step_id}:individual_review",
        "step_id": step_id,
        "annotation_kind": "individual_review",
        "source_artifact": target_artifact,
        "source_dataset_id": str(params.get("dataset_id") or "").strip(),
        "status": "",
        "origin": "manual",
        "reviewed": True,
        "review_decision": str(decision.get("review_decision") or "").strip(),
        "needs_check": decision.get("needs_check") is True,
        "comment": str(decision.get("comment") or "").strip(),
        "user": str(params.get("user") or "").strip(),
        "actor": dict(params.get("actor") or {}),
        "review_id": str(params.get("review_id") or "").strip(),
        "created_at": created_at,
        "scope": {
            "kind": "individual",
            "individual": individual,
            "set_name": "",
        },
        "resolved_fix_count": 0,
    }
    annotations.append(annotation)

    output = outputs[REVIEW_SIDECAR_NAME]
    output_path = Path(output["path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps({"schema_version": 6, "annotations": annotations}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "app": "movement",
                "action": "review_individual",
                "reviewed_individual_count": 1,
                "reviewed_ok_count": 1 if annotation["review_decision"] == "ok" else 0,
                "needs_check_count": 1 if annotation["needs_check"] else 0,
                "source_artifact": target_artifact,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
