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
    from examples.movement.summary import build_movement_fixes

    target_artifact = str(params.get("target_artifact") or "").strip()
    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    source = inputs.get(target_artifact)
    output = outputs.get(REVIEW_SIDECAR_NAME)
    if source is None or output is None:
        raise SystemExit("Movement source or review sidecar output was not declared")

    movement = build_movement_fixes(
        Path(source["path"]),
        limit=None,
        burst_gap_mode=params.get("burst_gap_mode"),
        burst_gap_seconds=params.get("burst_gap_seconds"),
        burst_gap_quantile=params.get("burst_gap_quantile"),
    )
    fixes = movement["fixes"]
    fix_by_key = {item["fix_key"]: item for item in fixes}
    raw_scope = dict(params.get("scope") or {})
    kind = str(raw_scope.get("kind") or "fix")
    resolved_fix_keys = []
    scope = {"kind": kind}
    if kind in {"fix", "segment"}:
        requested = sorted({str(item).strip() for item in raw_scope.get("fix_keys", []) if str(item).strip()})
        missing = sorted(set(requested) - set(fix_by_key))
        if missing:
            raise SystemExit("Some selected fixes were not found in the current dataset")
        resolved_fix_keys = requested
        scope["fix_keys"] = requested
        if kind == "segment":
            scope["start_fix_key"] = str(raw_scope.get("start_fix_key") or "").strip()
            scope["end_fix_key"] = str(raw_scope.get("end_fix_key") or "").strip()
    elif kind == "individual":
        individual = str(raw_scope.get("individual") or "").strip()
        set_name = str(raw_scope.get("set_name") or "").strip()
        resolved_fix_keys = [
            item["fix_key"]
            for item in fixes
            if item["individual"] == individual and (not set_name or item.get("set") == set_name)
        ]
        scope.update({"individual": individual, "set_name": set_name})
    elif kind == "burst":
        burst_id = str(raw_scope.get("burst_id") or "").strip()
        burst = next((item for item in movement["auto_bursts"] if item.get("burst_id") == burst_id), None)
        if burst is None:
            raise SystemExit("Selected burst was not found with the current burst settings")
        resolved_fix_keys = list(burst.get("fix_keys") or [])
        scope.update(
            {
                "burst_id": burst_id,
                "individual": str(burst.get("individual") or ""),
                "set_name": str(burst.get("set_name") or ""),
                "start_fix_key": str(burst.get("start_fix_key") or ""),
                "end_fix_key": str(burst.get("end_fix_key") or ""),
                "fix_keys": resolved_fix_keys,
                "burst_gap": movement.get("burst_gap") or {},
            }
        )
    else:
        raise SystemExit("Invalid review scope")
    if not resolved_fix_keys:
        raise SystemExit("Review scope did not resolve to any fixes")

    existing_input = inputs.get(REVIEW_SIDECAR_NAME)
    annotations = load_review_annotations(Path(existing_input["path"]) if existing_input else None)
    annotation = {
        "annotation_id": step_id,
        "step_id": step_id,
        "source_artifact": target_artifact,
        "source_dataset_id": str(params.get("dataset_id") or "").strip(),
        "status": str(params.get("status") or "").strip(),
        "origin": str(params.get("origin") or "manual").strip(),
        "issue_type": str(params.get("issue_type") or "").strip(),
        "comment": str(params.get("comment") or "").strip(),
        "owner_question": str(params.get("owner_question") or "").strip(),
        "user": str(params.get("user") or "").strip(),
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_analysis_id": str(params.get("source_analysis_id") or "").strip(),
        "scope": scope,
        "resolved_fix_count": len(resolved_fix_keys),
    }
    annotations.append(annotation)
    output_path = Path(output["path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps({"schema_version": 1, "annotations": annotations}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "app": "movement",
                "action": "annotate_scope",
                "annotation_id": annotation["annotation_id"],
                "scope_kind": kind,
                "status": annotation["status"],
                "origin": annotation["origin"],
                "resolved_fix_count": len(resolved_fix_keys),
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
