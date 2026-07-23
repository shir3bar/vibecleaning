import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


REVIEW_SIDECAR_NAME = "movement_review_annotations.json"
PORTABLE_REVIEW_COLUMNS = [
    "visible",
    "manually-marked-outlier",
    "algorithm-marked-outlier",
    "outlier_status",
    "outlier_issue_type",
    "outlier_comments",
    "outlier_flag_step_ids",
]
DEPRECATED_REVIEW_COLUMNS = {
    "manually_marked_outliers",
    "algorithm_marked_outliers",
    "outlier_annotation_ids",
}


def _split_values(value, separator=";"):
    return [item.strip() for item in str(value or "").split(separator) if item.strip()]


def _append_unique(items, value):
    normalized = str(value or "").strip()
    if normalized and normalized not in items:
        items.append(normalized)


def main():
    spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    params = dict(spec["step"].get("parameters") or {})
    step_id = str(spec["step"].get("step_id") or "").strip()
    repo_root = str(params.get("repo_root") or "").strip()
    if repo_root:
        sys.path.insert(0, repo_root)

    from examples.movement.review_annotations import (
        _flag_is_true,
        _legacy_annotations,
        _original_visible,
        annotation_applies,
        load_review_annotations,
    )
    from examples.movement.summary import (
        ALL_REVIEW_COLUMNS,
        _make_fix_key,
        detect_columns,
        is_valid_coordinate,
        parse_time_ms,
        try_float,
    )

    target_artifact = str(params.get("target_artifact") or "").strip()
    requested_confirmations = list(params.get("confirmations") or [])
    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    source = inputs.get(target_artifact)
    csv_output = outputs.get(target_artifact)
    sidecar_output = outputs.get(REVIEW_SIDECAR_NAME)
    if source is None or csv_output is None or sidecar_output is None:
        raise SystemExit("Movement CSV and review sidecar inputs/outputs must be declared")
    if not requested_confirmations:
        raise SystemExit("At least one suspected issue must be selected for confirmation")

    source_path = Path(source["path"])
    with source_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        source_fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    columns = detect_columns(source_fieldnames)
    if not columns["individual"] or not columns["time"] or not columns["lon"] or not columns["lat"]:
        raise SystemExit("CSV is missing required movement columns")

    row_context_by_fix_key = {}
    legacy_by_id = {}
    for row_index, raw in enumerate(rows, start=1):
        individual = str(raw.get(columns["individual"], "")).strip()
        time_ms = parse_time_ms(raw.get(columns["time"]))
        lon = try_float(raw.get(columns["lon"]))
        lat = try_float(raw.get(columns["lat"]))
        if not individual or time_ms is None or not is_valid_coordinate(lon, lat):
            continue
        fix_id = str(raw.get(columns["fix_id"], "")).strip() if columns["fix_id"] else ""
        set_name = str(raw.get(columns["set"], "")).strip().lower() if columns["set"] else "train"
        if set_name != "test":
            set_name = "train"
        fix_key = _make_fix_key(row_index, fix_id, individual, time_ms)
        row_context_by_fix_key[fix_key] = {
            "row_index": row_index,
            "raw": raw,
            "individual": individual,
            "set_name": set_name,
        }
        for annotation in _legacy_annotations(raw, fix_key=fix_key, source_artifact=target_artifact):
            legacy_by_id[annotation["annotation_id"]] = annotation

    existing_sidecar = inputs.get(REVIEW_SIDECAR_NAME)
    annotations = load_review_annotations(Path(existing_sidecar["path"]) if existing_sidecar else None)
    parent_by_id = {
        item["annotation_id"]: item
        for item in annotations
        if item["annotation_id"] and item["status"] == "suspected"
        and (not item["source_artifact"] or item["source_artifact"] == target_artifact)
    }
    parent_by_id.update({
        key: item
        for key, item in legacy_by_id.items()
        if item["status"] == "suspected"
    })

    already_confirmed = {
        (item["parent_annotation_id"], fix_key)
        for item in annotations
        if item["status"] == "confirmed" and item["parent_annotation_id"]
        for fix_key in item.get("scope", {}).get("fix_keys", [])
    }
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    user = str(params.get("user") or "").strip()
    note = str(params.get("note") or "").strip()
    confirmation_records = []
    row_updates = {}

    for index, requested in enumerate(requested_confirmations, start=1):
        if not isinstance(requested, dict):
            raise SystemExit("Invalid confirmation request")
        parent_id = str(requested.get("parent_annotation_id") or "").strip()
        parent = parent_by_id.get(parent_id)
        if parent is None:
            raise SystemExit(f"Suspected issue was not found: {parent_id}")
        fix_keys = sorted({
            str(item).strip()
            for item in requested.get("fix_keys", [])
            if str(item).strip()
        })
        if not fix_keys:
            raise SystemExit(f"No fixes were selected for suspected issue: {parent_id}")
        for fix_key in fix_keys:
            context = row_context_by_fix_key.get(fix_key)
            if context is None:
                raise SystemExit(f"Selected fix was not found: {fix_key}")
            if not annotation_applies(
                parent,
                fix_key=fix_key,
                individual=context["individual"],
                set_name=context["set_name"],
            ):
                raise SystemExit(f"Fix does not belong to suspected issue {parent_id}: {fix_key}")
            if (parent_id, fix_key) in already_confirmed:
                raise SystemExit(f"Fix is already confirmed for suspected issue {parent_id}: {fix_key}")
            row_updates.setdefault(fix_key, []).append(parent)

        confirmation_records.append({
            "annotation_id": f"{step_id}:confirmation:{index}",
            "annotation_kind": "confirmation",
            "parent_annotation_id": parent_id,
            "step_id": step_id,
            "source_artifact": target_artifact,
            "source_dataset_id": str(params.get("dataset_id") or "").strip(),
            "status": "confirmed",
            "origin": parent["origin"],
            "issue_type": parent["issue_type"],
            "comment": note,
            "owner_question": parent["owner_question"],
            "user": user,
            "created_at": now,
            "source_analysis_id": parent["source_analysis_id"],
            "scope": {
                "kind": "confirmation",
                "fix_keys": fix_keys,
                "burst_id": str(parent.get("scope", {}).get("burst_id") or ""),
                "individual": str(parent.get("scope", {}).get("individual") or ""),
                "set_name": str(parent.get("scope", {}).get("set_name") or ""),
                "burst_gap": dict(parent.get("scope", {}).get("burst_gap") or {}),
            },
            "resolved_fix_count": len(fix_keys),
        })

    output_fieldnames = [
        name
        for name in source_fieldnames
        if name not in ALL_REVIEW_COLUMNS
        and not str(name).startswith("vc_")
        and name not in PORTABLE_REVIEW_COLUMNS
        and name not in DEPRECATED_REVIEW_COLUMNS
    ] + PORTABLE_REVIEW_COLUMNS
    csv_output_path = Path(csv_output["path"])
    csv_output_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fieldnames)
        writer.writeheader()
        for row_index, raw in enumerate(rows, start=1):
            output_row = {name: raw.get(name, "") for name in output_fieldnames}
            individual = str(raw.get(columns["individual"], "")).strip()
            time_ms = parse_time_ms(raw.get(columns["time"]))
            fix_id = str(raw.get(columns["fix_id"], "")).strip() if columns["fix_id"] else ""
            fix_key = _make_fix_key(row_index, fix_id, individual, time_ms) if individual and time_ms is not None else ""
            parents = row_updates.get(fix_key, [])
            output_row["visible"] = "true" if _original_visible(raw.get("visible")) else "false"
            output_row["manually-marked-outlier"] = "true" if (
                _flag_is_true(raw.get("manually-marked-outlier"))
                or _flag_is_true(raw.get("manually_marked_outliers"))
            ) else "false"
            output_row["algorithm-marked-outlier"] = "true" if (
                _flag_is_true(raw.get("algorithm-marked-outlier"))
                or _flag_is_true(raw.get("algorithm_marked_outliers"))
            ) else "false"
            output_row["outlier_status"] = str(raw.get("outlier_status") or "").strip()
            output_row["outlier_issue_type"] = str(raw.get("outlier_issue_type") or "").strip()
            output_row["outlier_comments"] = str(raw.get("outlier_comments") or "").strip()
            output_row["outlier_flag_step_ids"] = str(raw.get("outlier_flag_step_ids") or "").strip()
            if parents:
                output_row["visible"] = "false"
                output_row["outlier_status"] = "confirmed"
                issue_types = _split_values(output_row["outlier_issue_type"])
                step_ids = _split_values(output_row["outlier_flag_step_ids"])
                for parent in parents:
                    if parent["origin"] == "manual":
                        output_row["manually-marked-outlier"] = "true"
                    else:
                        output_row["algorithm-marked-outlier"] = "true"
                    _append_unique(issue_types, parent["issue_type"])
                    _append_unique(step_ids, parent["step_id"] or parent["annotation_id"])
                _append_unique(step_ids, step_id)
                output_row["outlier_issue_type"] = "; ".join(issue_types)
                output_row["outlier_flag_step_ids"] = ";".join(step_ids)
            writer.writerow(output_row)

    annotations.extend(confirmation_records)
    sidecar_output_path = Path(sidecar_output["path"])
    sidecar_output_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_output_path.write_text(
        json.dumps({"schema_version": 2, "annotations": annotations}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "app": "movement",
                "action": "confirm_issues",
                "source_artifact": target_artifact,
                "confirmation_count": len(confirmation_records),
                "confirmed_fix_count": len(row_updates),
                "parent_annotation_ids": sorted(item["parent_annotation_id"] for item in confirmation_records),
                "manually_marked_fix_count": sum(
                    1 for parents in row_updates.values() if any(parent["origin"] == "manual" for parent in parents)
                ),
                "algorithm_marked_fix_count": sum(
                    1 for parents in row_updates.values() if any(parent["origin"] in {"threshold", "algorithm"} for parent in parents)
                ),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
