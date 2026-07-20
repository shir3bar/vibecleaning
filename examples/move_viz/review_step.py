import json
import os
from datetime import datetime, timezone
from pathlib import Path


REVIEW_ARTIFACT = "move_viz_review_annotations.json"


def main():
    spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    params = dict(spec["step"].get("parameters") or {})
    step_id = str(spec["step"].get("step_id") or "").strip()
    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    existing = inputs.get(REVIEW_ARTIFACT)
    output = outputs.get(REVIEW_ARTIFACT)
    if output is None:
        raise SystemExit("Review annotation output was not declared")

    payload = {"schema_version": 1, "tables": {}}
    if existing:
        payload = json.loads(Path(existing["path"]).read_text(encoding="utf-8"))
    tables = payload.setdefault("tables", {})
    table_name = str(params.get("table") or "").strip()
    table = tables.setdefault(table_name, {"flags": {}})
    flags = table.setdefault("flags", {})
    row_keys = sorted({str(item).strip() for item in params.get("row_keys", []) if str(item).strip()})
    operation = str(params.get("operation") or "flag").strip().lower()
    if operation == "flag":
        created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        for row_key in row_keys:
            flags[row_key] = {
                "row_key": row_key,
                "comment": str(params.get("comment") or "").strip(),
                "scope": str(params.get("scope") or "fix").strip(),
                "user": str(params.get("user") or "").strip(),
                "created_at": created_at,
                "step_id": step_id,
            }
    elif operation == "unflag":
        for row_key in row_keys:
            flags.pop(row_key, None)
    else:
        raise SystemExit("Invalid review operation")

    output_path = Path(output["path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(
        json.dumps(
            {
                "app": "move_viz",
                "action": operation,
                "table": table_name,
                "scope": str(params.get("scope") or "fix"),
                "affected_row_count": len(row_keys),
                "remaining_flag_count": len(flags),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
