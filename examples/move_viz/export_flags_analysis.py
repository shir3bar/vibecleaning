import csv
import json
import os
from pathlib import Path
import sqlite3
from urllib.parse import quote


REVIEW_ARTIFACT = "move_viz_review_annotations.json"
OUTPUT_ARTIFACT = "move_viz_flags.csv"


def quoted_identifier(value):
    return '"' + str(value).replace('"', '""') + '"'


def truthy(value):
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def flagged_rowids(flags):
    rowids = []
    for row_key in flags:
        suffix = str(row_key).rsplit("#row:", 1)[-1] if "#row:" in str(row_key) else str(row_key).removeprefix("row:")
        try:
            rowids.append(int(suffix))
        except ValueError:
            continue
    return sorted(set(rowids))


def rows_for_rowids(connection, table, rowids):
    for offset in range(0, len(rowids), 500):
        chunk = rowids[offset : offset + 500]
        placeholders = ", ".join("?" for _ in chunk)
        yield from connection.execute(
            f'SELECT rowid AS "__move_viz_rowid__", * FROM {table} '
            f"WHERE rowid IN ({placeholders}) ORDER BY rowid",
            chunk,
        )


def main():
    spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    params = dict(spec["analysis"].get("parameters") or {})
    inputs = {item["logical_name"]: item for item in spec.get("input_artifacts", [])}
    outputs = {item["logical_name"]: item for item in spec.get("output_artifacts", [])}
    source = inputs.get("source.sqlite")
    review = inputs.get(REVIEW_ARTIFACT)
    output = outputs.get(OUTPUT_ARTIFACT)
    if source is None or review is None or output is None:
        raise SystemExit("SQLite source, review annotations, or CSV output was not declared")

    review_payload = json.loads(Path(review["path"]).read_text(encoding="utf-8"))
    table_name = str(params.get("table") or "")
    flags = (
        review_payload.get("tables", {})
        .get(table_name, {})
        .get("flags", {})
    )
    mapping = dict(params.get("mapping") or {})
    uri = f"file:{quote(str(Path(source['path']).resolve()))}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    table = quoted_identifier(table_name)
    timestamp = str(mapping.get("timestamp") or "")
    order_clause = f" ORDER BY {quoted_identifier(timestamp)}" if timestamp else ""
    try:
        connection.execute(f"SELECT rowid FROM {table} LIMIT 0")
        has_rowid = True
    except sqlite3.OperationalError:
        has_rowid = False
    rows = (
        rows_for_rowids(connection, table, flagged_rowids(flags))
        if has_rowid
        else connection.execute(f"SELECT * FROM {table}{order_clause}")
    )
    columns = {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}

    fieldnames = [
        "source_file",
        "project",
        "dataset_id",
        "table",
        "row_key",
        "event_id",
        "individual",
        "timestamp",
        "longitude",
        "latitude",
        "selection_scope",
        "flag_step_id",
        "manually-marked-outlier",
        "outlier_comments",
    ]
    output_path = Path(output["path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    exported = 0
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for index, row in enumerate(rows):
            event_column = str(mapping.get("event_id") or "")
            event_id = row[event_column] if event_column else None
            rowid = row["__move_viz_rowid__"] if has_rowid else index + 1
            row_key = f"event:{event_id}#row:{rowid}" if event_id not in (None, "") else f"row:{rowid}"
            flag = flags.get(row_key)
            if not isinstance(flag, dict):
                continue
            comments = [str(flag.get("comment") or "").strip()]
            if "manually-marked-outlier" in columns and truthy(row["manually-marked-outlier"]):
                comments.append("Already flagged in source: manually-marked-outlier=true")
            if "algorithm-marked-outlier" in columns and truthy(row["algorithm-marked-outlier"]):
                comments.append("Already flagged in source: algorithm-marked-outlier=true")
            individual_column = str(mapping.get("individual") or "")
            longitude_column = str(mapping.get("longitude") or "")
            latitude_column = str(mapping.get("latitude") or "")
            writer.writerow(
                {
                    "source_file": params.get("source_filename", ""),
                    "project": spec.get("project_name", ""),
                    "dataset_id": spec.get("dataset", {}).get("dataset_id", ""),
                    "table": table_name,
                    "row_key": row_key,
                    "event_id": event_id if event_id is not None else "",
                    "individual": row[individual_column] if individual_column else "All fixes",
                    "timestamp": row[timestamp] if timestamp else "",
                    "longitude": row[longitude_column],
                    "latitude": row[latitude_column],
                    "selection_scope": flag.get("scope", "fix"),
                    "flag_step_id": flag.get("step_id", ""),
                    "manually-marked-outlier": "true",
                    "outlier_comments": "; ".join(item for item in comments if item),
                }
            )
            exported += 1
    connection.close()
    summary_path.write_text(
        json.dumps(
            {
                "app": "move_viz",
                "action": "export_flags_csv",
                "table": table_name,
                "flagged_row_count": exported,
                "output_artifact": OUTPUT_ARTIFACT,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
