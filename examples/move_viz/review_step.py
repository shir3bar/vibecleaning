import json
import os
from datetime import datetime, timezone
from pathlib import Path


REVIEW_ARTIFACT = "move_viz_review_annotations.json"


def normalize_ranges(raw_ranges):
    ranges = []
    for item in raw_ranges or []:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise SystemExit("Invalid review row ranges")
        start, end = int(item[0]), int(item[1])
        if start < 1 or end < start:
            raise SystemExit("Invalid review row ranges")
        ranges.append((start, end))
    merged = []
    for start, end in sorted(ranges):
        if merged and start <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def subtract_range(run, removed_ranges):
    fragments = [(int(run["start_row"]), int(run["end_row"]))]
    for removed_start, removed_end in removed_ranges:
        next_fragments = []
        for start, end in fragments:
            if removed_end < start or removed_start > end:
                next_fragments.append((start, end))
                continue
            if removed_start > start:
                next_fragments.append((start, removed_start - 1))
            if removed_end < end:
                next_fragments.append((removed_end + 1, end))
        fragments = next_fragments
    return [
        {**run, "start_row": start, "end_row": end}
        for start, end in fragments
    ]


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

    payload = {"schema_version": 2, "tables": {}}
    if existing:
        payload = json.loads(Path(existing["path"]).read_text(encoding="utf-8"))
    tables = payload.setdefault("tables", {})
    table_name = str(params.get("table") or "").strip()
    table = tables.setdefault(table_name, {"flag_runs": []})
    flag_runs = [
        dict(item)
        for item in table.get("flag_runs", [])
        if isinstance(item, dict)
    ]
    row_ranges = normalize_ranges(params.get("row_ranges") or [])
    affected_row_count = sum(end - start + 1 for start, end in row_ranges)
    flag_runs = [
        fragment
        for run in flag_runs
        for fragment in subtract_range(run, row_ranges)
    ]
    operation = str(params.get("operation") or "flag").strip().lower()
    if operation == "flag":
        created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        for start_row, end_row in row_ranges:
            flag_runs.append({
                "start_row": start_row,
                "end_row": end_row,
                "comment": str(params.get("comment") or "").strip(),
                "scope": str(params.get("scope") or "fix").strip(),
                "user": str(params.get("user") or "").strip(),
                "created_at": created_at,
                "step_id": step_id,
            })
    elif operation == "unflag":
        pass
    else:
        raise SystemExit("Invalid review operation")
    flag_runs.sort(key=lambda item: (int(item["start_row"]), int(item["end_row"])))
    table.pop("flags", None)
    table["flag_runs"] = flag_runs
    payload["schema_version"] = 2

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
                "affected_row_count": affected_row_count,
                "remaining_flag_count": sum(
                    int(run["end_row"]) - int(run["start_row"]) + 1
                    for run in flag_runs
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
