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
    from examples.movement.review_annotations import (
        compress_fix_keys,
        confirmed_exclusion_scopes,
        load_review_annotations,
        normalize_row_ranges,
        resolve_filter_row_ranges,
    )
    from examples.movement.summary import build_movement_fixes

    target_artifact = str(params.get("target_artifact") or "").strip()
    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    source = inputs.get(target_artifact)
    output = outputs.get(REVIEW_SIDECAR_NAME)
    if source is None or output is None:
        raise SystemExit("Movement source or review sidecar output was not declared")

    existing_input = inputs.get(REVIEW_SIDECAR_NAME)
    annotations = load_review_annotations(Path(existing_input["path"]) if existing_input else None)

    raw_scope = dict(params.get("scope") or {})
    kind = str(raw_scope.get("kind") or "fix")
    resolved_scopes = []
    if kind in {"fix", "segment"}:
        row_ranges = normalize_row_ranges(raw_scope.get("row_ranges") or [])
        resolved_fix_count = sum(end - start + 1 for start, end in row_ranges)
        expected_count = int(raw_scope.get("fix_count") or 0)
        if expected_count and resolved_fix_count != expected_count:
            raise SystemExit("Selected fix count does not match the annotation scope")
        scope = {"kind": kind, "row_ranges": row_ranges}
        if kind == "segment":
            scope["start_fix_key"] = str(raw_scope.get("start_fix_key") or "").strip()
            scope["end_fix_key"] = str(raw_scope.get("end_fix_key") or "").strip()
            scope["individual"] = str(raw_scope.get("individual") or "").strip()
            scope["set_name"] = str(raw_scope.get("set_name") or "").strip()
            scope["selection_method"] = str(raw_scope.get("selection_method") or "").strip()
        resolved_scopes.append((scope, resolved_fix_count))
    elif kind == "filter":
        filter_spec = dict(raw_scope.get("filter") or {})
        confirmed_fix_keys, confirmed_individual_tracks = confirmed_exclusion_scopes(
            annotations,
            source_artifact=target_artifact,
        )
        row_ranges, resolved_fix_count = resolve_filter_row_ranges(
            Path(source["path"]),
            filter_spec,
            confirmed_fix_keys=confirmed_fix_keys,
            confirmed_individual_tracks=confirmed_individual_tracks,
        )
        resolved_scopes.append(
            (
                {"kind": "filter", "filter": filter_spec, "row_ranges": row_ranges},
                resolved_fix_count,
            )
        )
    elif kind == "individual":
        movement = build_movement_fixes(
            Path(source["path"]),
            limit=None,
            burst_gap_mode=params.get("burst_gap_mode"),
            burst_gap_seconds=params.get("burst_gap_seconds"),
            burst_gap_quantile=params.get("burst_gap_quantile"),
        )
        fixes = movement["fixes"]
        individual = str(raw_scope.get("individual") or "").strip()
        set_name = str(raw_scope.get("set_name") or "").strip()
        resolved_fix_keys = [
            item["fix_key"]
            for item in fixes
            if item["individual"] == individual and (not set_name or item.get("set") == set_name)
        ]
        resolved_scopes.append(
            (
                {"kind": kind, "individual": individual, "set_name": set_name},
                len(resolved_fix_keys),
            )
        )
    elif kind in {"burst", "bursts"}:
        movement = build_movement_fixes(
            Path(source["path"]),
            limit=None,
            burst_gap_mode=params.get("burst_gap_mode"),
            burst_gap_seconds=params.get("burst_gap_seconds"),
            burst_gap_quantile=params.get("burst_gap_quantile"),
        )
        burst_ids = (
            [str(raw_scope.get("burst_id") or "").strip()]
            if kind == "burst"
            else [
                str(item).strip()
                for item in raw_scope.get("burst_ids") or []
                if str(item).strip()
            ]
        )
        bursts_by_id = {
            str(item.get("burst_id") or ""): item
            for item in movement["auto_bursts"]
        }
        for burst_id in burst_ids:
            burst = bursts_by_id.get(burst_id)
            if burst is None:
                raise SystemExit("Selected burst was not found with the current burst settings")
            resolved_fix_keys = list(burst.get("fix_keys") or [])
            resolved_scopes.append(
                (
                    {
                        "kind": "burst",
                        "burst_id": burst_id,
                        "individual": str(burst.get("individual") or ""),
                        "set_name": str(burst.get("set_name") or ""),
                        "start_fix_key": str(burst.get("start_fix_key") or ""),
                        "end_fix_key": str(burst.get("end_fix_key") or ""),
                        "row_ranges": compress_fix_keys(resolved_fix_keys),
                        "burst_gap": movement.get("burst_gap") or {},
                    },
                    len(resolved_fix_keys),
                )
            )
    else:
        raise SystemExit("Invalid review scope")
    if not resolved_scopes or any(not fix_count for _, fix_count in resolved_scopes):
        raise SystemExit("Review scope did not resolve to any fixes")

    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    new_annotations = []
    for index, (scope, resolved_fix_count) in enumerate(resolved_scopes):
        annotation = {
            "annotation_id": (
                step_id
                if len(resolved_scopes) == 1
                else f"{step_id}:scope:{index + 1:04d}"
            ),
            "step_id": step_id,
            "source_artifact": target_artifact,
            "source_dataset_id": str(params.get("dataset_id") or "").strip(),
            "status": str(params.get("status") or "").strip(),
            "origin": str(params.get("origin") or "manual").strip(),
            "issue_type": str(params.get("issue_type") or "").strip(),
            "issue_field": str(params.get("issue_field") or "").strip(),
            "issue_threshold": str(params.get("issue_threshold") or "").strip(),
            "comment": str(params.get("comment") or "").strip(),
            "owner_question": str(params.get("owner_question") or "").strip(),
            "user": str(params.get("user") or "").strip(),
            "actor": dict(params.get("actor") or {}),
            "review_id": str(params.get("review_id") or "").strip(),
            "created_at": created_at,
            "source_analysis_id": str(params.get("source_analysis_id") or "").strip(),
            "workflow_context": dict(params.get("workflow_context") or {}),
            "scope": scope,
            "resolved_fix_count": resolved_fix_count,
        }
        annotations.append(annotation)
        new_annotations.append(annotation)
    output_path = Path(output["path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps({"schema_version": 5, "annotations": annotations}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "app": "movement",
                "action": "annotate_scope",
                "annotation_id": new_annotations[0]["annotation_id"],
                "annotation_ids": [item["annotation_id"] for item in new_annotations],
                "annotation_count": len(new_annotations),
                "scope_kind": kind,
                "status": new_annotations[0]["status"],
                "origin": new_annotations[0]["origin"],
                "resolved_fix_count": sum(
                    item["resolved_fix_count"]
                    for item in new_annotations
                ),
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
