import csv
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
        annotation_applies,
        compress_fix_keys,
        fix_key_row_number,
        load_review_annotations,
        normalize_row_ranges,
        row_number_in_ranges,
        row_tokens_for_scope,
        source_row_annotation,
    )
    from examples.movement.summary import (
        _make_fix_key,
        detect_columns,
        is_valid_coordinate,
        parse_time_ms,
        try_float,
    )

    target_artifact = str(params.get("target_artifact") or "").strip()
    requested_dismissals = list(params.get("dismissals") or [])
    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    source = inputs.get(target_artifact)
    sidecar_output = outputs.get(REVIEW_SIDECAR_NAME)
    if source is None or sidecar_output is None:
        raise SystemExit("Movement CSV input and review sidecar output must be declared")
    if not requested_dismissals:
        raise SystemExit("At least one suspected issue must be selected for dismissal")

    requested_parent_ids = {
        str(item.get("parent_annotation_id") or "").strip()
        for item in requested_dismissals
        if isinstance(item, dict)
    }
    requested_row_ranges = normalize_row_ranges(
        [
            row_range
            for item in requested_dismissals
            if isinstance(item, dict)
            for row_range in normalize_row_ranges(item.get("row_ranges") or [])
        ]
    )
    existing_sidecar = inputs.get(REVIEW_SIDECAR_NAME)
    annotations = load_review_annotations(Path(existing_sidecar["path"]) if existing_sidecar else None)

    row_context_by_number = {}
    source_annotations_by_id = {}
    with Path(source["path"]).open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = detect_columns(list(reader.fieldnames or []))
        if not columns["individual"] or not columns["time"] or not columns["lon"] or not columns["lat"]:
            raise SystemExit("CSV is missing required movement columns")
        for row_index, raw in enumerate(reader, start=1):
            individual = str(raw.get(columns["individual"], "")).strip()
            time_ms = parse_time_ms(raw.get(columns["time"]))
            lon = try_float(raw.get(columns["lon"]))
            lat = try_float(raw.get(columns["lat"]))
            if not individual or time_ms is None or not is_valid_coordinate(lon, lat):
                continue
            fix_id = str(raw.get(columns["fix_id"], "")).strip() if columns["fix_id"] else ""
            fix_key = _make_fix_key(row_index, fix_id, individual, time_ms)
            row_number = fix_key_row_number(fix_key)
            if not row_number_in_ranges(row_number, requested_row_ranges):
                continue
            set_name = str(raw.get(columns["set"], "")).strip().lower() if columns["set"] else "train"
            if set_name != "test":
                set_name = "train"
            row_context_by_number[row_number] = {
                "fix_key": fix_key,
                "individual": individual,
                "set_name": set_name,
            }
            source_annotation = source_row_annotation(
                raw,
                fix_key=fix_key,
                source_artifact=target_artifact,
            )
            if (
                source_annotation
                and source_annotation["status"] == "suspected"
                and source_annotation["annotation_id"] in requested_parent_ids
            ):
                source_annotations_by_id[source_annotation["annotation_id"]] = source_annotation

    parent_by_id = {
        item["annotation_id"]: item
        for item in annotations
        if item["annotation_id"] and item["status"] == "suspected"
        and not item.get("parent_annotation_id")
        and (not item["source_artifact"] or item["source_artifact"] == target_artifact)
    }
    parent_by_id.update(source_annotations_by_id)
    already_resolved = {
        (item["parent_annotation_id"], row_token)
        for item in annotations
        if item.get("parent_annotation_id") and item.get("status") in {"confirmed", "dismissed"}
        for row_token in row_tokens_for_scope(item.get("scope") or {})
    }

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    note = str(params.get("note") or "").strip()
    records = []
    dismissed_fix_keys = set()
    for index, requested in enumerate(requested_dismissals, start=1):
        if not isinstance(requested, dict):
            raise SystemExit("Invalid dismissal request")
        parent_id = str(requested.get("parent_annotation_id") or "").strip()
        parent = parent_by_id.get(parent_id)
        if parent is None:
            raise SystemExit(f"Suspected issue was not found: {parent_id}")
        row_ranges = normalize_row_ranges(requested.get("row_ranges") or [])
        contexts = [
            context
            for row_number, context in row_context_by_number.items()
            if row_number_in_ranges(row_number, row_ranges)
        ]
        fix_keys = sorted(context["fix_key"] for context in contexts)
        if not fix_keys:
            raise SystemExit(f"No fixes were selected for suspected issue: {parent_id}")
        if int(requested.get("fix_count") or 0) not in {0, len(fix_keys)}:
            raise SystemExit(f"Some selected fixes were not found for suspected issue: {parent_id}")
        for fix_key in fix_keys:
            context = row_context_by_number[fix_key_row_number(fix_key)]
            if not annotation_applies(
                parent,
                fix_key=fix_key,
                individual=context["individual"],
                set_name=context["set_name"],
            ):
                raise SystemExit(f"Fix does not belong to suspected issue {parent_id}: {fix_key}")
            pair = (parent_id, f"row:{fix_key_row_number(fix_key)}")
            if pair in already_resolved:
                raise SystemExit(f"Fix is already resolved for suspected issue {parent_id}: {fix_key}")
            dismissed_fix_keys.add(fix_key)

        records.append(
            {
                "annotation_id": f"{step_id}:dismissal:{index}",
                "annotation_kind": "dismissal",
                "parent_annotation_id": parent_id,
                "step_id": step_id,
                "source_artifact": target_artifact,
                "source_dataset_id": str(params.get("dataset_id") or "").strip(),
                "status": "dismissed",
                "origin": parent["origin"],
                "issue_type": parent["issue_type"],
                "comment": note,
                "owner_question": parent["owner_question"],
                "user": str(params.get("user") or "").strip(),
                "actor": dict(params.get("actor") or {}),
                "review_id": str(params.get("review_id") or "").strip(),
                "created_at": now,
                "source_analysis_id": parent["source_analysis_id"],
                "scope": {
                    "kind": "dismissal",
                    "row_ranges": compress_fix_keys(fix_keys),
                    "burst_id": str(parent.get("scope", {}).get("burst_id") or ""),
                    "individual": str(parent.get("scope", {}).get("individual") or ""),
                    "set_name": str(parent.get("scope", {}).get("set_name") or ""),
                },
                "resolved_fix_count": len(fix_keys),
            }
        )

    annotations.extend(records)
    output_path = Path(sidecar_output["path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps({"schema_version": 5, "annotations": annotations}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "app": "movement",
                "action": "dismiss_issues",
                "source_artifact": target_artifact,
                "dismissal_count": len(records),
                "dismissed_fix_count": len(dismissed_fix_keys),
                "parent_annotation_ids": sorted(item["parent_annotation_id"] for item in records),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
