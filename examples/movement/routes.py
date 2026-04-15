import textwrap
import uuid
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

from app.execution import create_analysis, create_step, undo_to_parent
from app.state import (
    ProjectStateError,
    get_dataset_artifact,
    graph_payload,
    load_dataset,
    load_json,
    media_type_for_path,
    project_paths,
    project_state_payload,
)
from app.web import get_project_dir, json_error, parse_json_body, validate_path_part

from .catalog import get_study_dir, list_families, list_studies
from .summary import DEFAULT_FIX_LIMIT, build_movement_fixes, build_movement_overview, build_movement_summary


def _build_initial_study_payload(study_dir: Path) -> dict:
    state = project_state_payload(study_dir)
    graph = graph_payload(study_dir)
    dataset_id = state["current_dataset"]["dataset_id"]
    dataset = load_dataset(study_dir, dataset_id)
    artifacts = list(dataset.get("artifacts") or [])
    if not artifacts:
        raise ProjectStateError("Selected dataset has no artifacts")
    logical_name = str(artifacts[0].get("logical_name") or "").strip()
    if not logical_name:
        raise ProjectStateError("Selected dataset has no artifacts")
    return {
        "state": state,
        "graph": graph,
        "dataset": dataset,
        "dataset_id": dataset_id,
        "logical_name": logical_name,
    }


SCRIPT_SHARED = textwrap.dedent(
    """
    import base64
    import csv
    import html
    import json
    import os
    from datetime import datetime, timezone
    from pathlib import Path


    REVIEW_COLUMNS = [
        "vc_outlier_status",
        "vc_issue_id",
        "vc_issue_type",
        "vc_issue_field",
        "vc_issue_threshold",
        "vc_issue_refs",
        "vc_issue_note",
        "vc_owner_question",
        "vc_review_user",
        "vc_reviewed_at",
    ]

    QUALITY_KEYWORDS = (
        "gps",
        "quality",
        "fix",
        "visible",
        "outlier",
        "manual",
        "algorithm",
        "hdop",
        "pdop",
        "dop",
        "satellite",
        "sat",
        "accuracy",
        "precision",
        "error",
        "usedtime",
        "timetogetfix",
        "heightabovemsl",
    )


    def normalize_header(header):
        return str(header or "").lower().replace("-", "").replace("_", "").replace(":", "").replace(" ", "")


    def find_column(normalized_map, aliases):
        for alias in aliases:
            if alias in normalized_map:
                return normalized_map[alias]
        return None


    def detect_columns(fieldnames):
        normalized = {normalize_header(name): name for name in fieldnames}
        columns = {
            "fix_id": find_column(normalized, [
                "eventid",
                "fixid",
                "observationid",
                "rowid",
                "recordid",
                "id",
            ]),
            "individual": find_column(normalized, [
                "individual",
                "individualid",
                "individuallocalidentifier",
                "animalid",
                "trackid",
                "taglocalidentifier",
            ]),
            "time": find_column(normalized, [
                "timestamp",
                "time",
                "datetime",
                "eventtime",
                "transmissiontimestamp",
                "studylocaltimestamp",
            ]),
            "lon": find_column(normalized, [
                "longitude",
                "lon",
                "locationlong",
                "stependlocationlong",
                "x",
            ]),
            "lat": find_column(normalized, [
                "latitude",
                "lat",
                "locationlat",
                "stependlocationlat",
                "y",
            ]),
            "set": find_column(normalized, ["set", "split", "partition"]),
        }
        for name in REVIEW_COLUMNS:
            columns[name] = normalized.get(normalize_header(name))
        return columns


    def parse_time_ms(raw_value):
        value = str(raw_value or "").strip()
        if not value:
            return None
        normalized = value.replace("Z", "+00:00")
        parsers = (
            lambda item: datetime.fromisoformat(item),
            lambda item: datetime.strptime(item, "%Y-%m-%d %H:%M:%S"),
            lambda item: datetime.strptime(item, "%Y-%m-%d %H:%M:%S.%f"),
        )
        for parser in parsers:
            try:
                return int(parser(normalized).timestamp() * 1000)
            except ValueError:
                continue
        return None


    def try_float(raw_value):
        value = str(raw_value or "").strip()
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None


    def is_valid_coordinate(lon, lat):
        return lon is not None and lat is not None and -180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0


    def make_fix_key(row_index, fix_id, fix_id_is_unique, individual, time_ms):
        if fix_id:
            return f"id:{fix_id}#row:{row_index}"
        return f"row:{row_index}|{individual}|{time_ms}"


    def format_timestamp(time_ms):
        return datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc).isoformat()


    def now_iso():
        return datetime.now(timezone.utc).isoformat(timespec="seconds")


    def normalize_review_status(raw_value):
        value = str(raw_value or "").strip().lower()
        return value if value in {"suspected", "confirmed"} else ""


    def clean_issue_payload(item, fallback_status=""):
        status = normalize_review_status(item.get("status")) or normalize_review_status(fallback_status)
        if not status:
            return {}
        issue = {
            "status": status,
            "issue_id": str(item.get("issue_id", "")).strip(),
            "issue_type": str(item.get("issue_type", "")).strip(),
            "issue_field": str(item.get("issue_field", "")).strip(),
            "issue_threshold": str(item.get("issue_threshold", "")).strip(),
            "issue_note": str(item.get("issue_note", "")).strip(),
            "owner_question": str(item.get("owner_question", "")).strip(),
            "review_user": str(item.get("review_user", "")).strip(),
            "reviewed_at": str(item.get("reviewed_at", "")).strip(),
        }
        cleaned = {key: value for key, value in issue.items() if value}
        if not cleaned.get("issue_id") and not cleaned.get("issue_type"):
            return {}
        return cleaned


    def parse_issue_refs(raw):
        row_status = normalize_review_status(raw.get("vc_outlier_status"))
        raw_refs = str(raw.get("vc_issue_refs", "")).strip()
        if raw_refs:
            try:
                parsed = json.loads(raw_refs)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                issues = [clean_issue_payload(item, row_status) for item in parsed if isinstance(item, dict)]
                issues = [item for item in issues if item]
                if issues:
                    return issues
        legacy = clean_issue_payload({
            "status": raw.get("vc_outlier_status"),
            "issue_id": raw.get("vc_issue_id"),
            "issue_type": raw.get("vc_issue_type"),
            "issue_field": raw.get("vc_issue_field"),
            "issue_threshold": raw.get("vc_issue_threshold"),
            "issue_note": raw.get("vc_issue_note"),
            "owner_question": raw.get("vc_owner_question"),
            "review_user": raw.get("vc_review_user"),
            "reviewed_at": raw.get("vc_reviewed_at"),
        }, row_status)
        return [legacy] if legacy else []


    def aggregate_issue_status(issues):
        statuses = {str(item.get("status", "")).strip().lower() for item in issues if str(item.get("status", "")).strip()}
        if "suspected" in statuses:
            return "suspected"
        if "confirmed" in statuses:
            return "confirmed"
        return ""


    def extract_quality_fields(fieldnames, columns):
        excluded = {
            value
            for key, value in columns.items()
            if value and key not in REVIEW_COLUMNS
        }
        result = []
        for name in fieldnames:
            if name in excluded or name in REVIEW_COLUMNS:
                continue
            normalized = normalize_header(name)
            if any(keyword in normalized for keyword in QUALITY_KEYWORDS):
                result.append(name)
        return result


    def load_rows_with_context(source_path):
        with Path(source_path).open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = list(reader.fieldnames or [])
            if not fieldnames:
                raise SystemExit("CSV did not contain a header row")
            columns = detect_columns(fieldnames)
            if not columns["individual"] or not columns["time"] or not columns["lon"] or not columns["lat"]:
                raise SystemExit("CSV is missing required columns for movement visualization")
            rows = list(reader)

        valid_records = []
        fix_id_counts = {}
        for row_index, raw in enumerate(rows, start=1):
            individual = str(raw.get(columns["individual"], "")).strip()
            if not individual:
                continue
            time_ms = parse_time_ms(raw.get(columns["time"]))
            lon = try_float(raw.get(columns["lon"]))
            lat = try_float(raw.get(columns["lat"]))
            if time_ms is None or not is_valid_coordinate(lon, lat):
                continue
            fix_id = str(raw.get(columns["fix_id"], "")).strip() if columns["fix_id"] else ""
            if fix_id:
                fix_id_counts[fix_id] = fix_id_counts.get(fix_id, 0) + 1

        previous_by_group = {}
        for row_index, raw in enumerate(rows, start=1):
            individual = str(raw.get(columns["individual"], "")).strip()
            if not individual:
                continue
            time_ms = parse_time_ms(raw.get(columns["time"]))
            lon = try_float(raw.get(columns["lon"]))
            lat = try_float(raw.get(columns["lat"]))
            if time_ms is None or not is_valid_coordinate(lon, lat):
                continue

            set_name = str(raw.get(columns["set"], "")).strip().lower() if columns["set"] else "train"
            if set_name != "test":
                set_name = "train"
            group_key = (individual, set_name)
            previous = previous_by_group.get(group_key)
            time_delta_s = None
            step_length_m = None
            speed_mps = None
            if previous and time_ms > previous["time_ms"]:
                time_delta_s = (time_ms - previous["time_ms"]) / 1000.0
                from math import atan2, cos, radians, sin, sqrt
                phi1 = radians(previous["lat"])
                phi2 = radians(lat)
                delta_phi = radians(lat - previous["lat"])
                delta_lambda = radians(lon - previous["lon"])
                a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
                step_length_m = 6371000.0 * 2 * atan2(sqrt(a), sqrt(1 - a))
                speed_mps = step_length_m / time_delta_s if time_delta_s > 0 else None
            previous_by_group[group_key] = {"time_ms": time_ms, "lon": lon, "lat": lat}

            fix_id = str(raw.get(columns["fix_id"], "")).strip() if columns["fix_id"] else ""
            fix_key = make_fix_key(row_index, fix_id, bool(fix_id) and fix_id_counts.get(fix_id, 0) == 1, individual, time_ms)
            review = {}
            for name in REVIEW_COLUMNS:
                review[name] = str(raw.get(name, "")).strip()
            review["issues"] = parse_issue_refs(raw)
            valid_records.append(
                {
                    "row_index": row_index,
                    "fix_key": fix_key,
                    "individual": individual,
                    "set_name": set_name,
                    "time_ms": time_ms,
                    "lon": lon,
                    "lat": lat,
                    "time_text": str(raw.get(columns["time"], "")).strip(),
                    "raw": dict(raw),
                    "review": review,
                    "step_length_m": step_length_m,
                    "speed_mps": speed_mps,
                    "time_delta_s": time_delta_s,
                }
            )
        return fieldnames, columns, rows, valid_records


    def selected_contexts(valid_records, selected_fix_keys=None, selected_issue_ids=None):
        fix_keys = set(selected_fix_keys or [])
        issue_ids = set(selected_issue_ids or [])
        result = []
        for record in valid_records:
            review_issue_ids = {
                str(item.get("issue_id", "")).strip()
                for item in record["review"].get("issues", [])
                if str(item.get("issue_id", "")).strip()
            }
            if issue_ids and review_issue_ids.intersection(issue_ids):
                result.append(record)
                continue
            if fix_keys and record["fix_key"] in fix_keys:
                result.append(record)
        return result


    def write_json(path, payload):
        Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\\n")


    def decode_data_url(data_url):
        raw = str(data_url or "").strip()
        if not raw:
            raise ValueError("Missing snapshot data")
        if "," in raw:
            _, encoded = raw.split(",", 1)
        else:
            encoded = raw
        return base64.b64decode(encoded)


    def html_escape(value):
        return html.escape(str(value or ""), quote=True)
    """
).strip() + "\n"


ANNOTATE_FIXES_SCRIPT = SCRIPT_SHARED + textwrap.dedent(
    """

def main():
        spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
        summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
        spec = json.loads(spec_path.read_text())
        params = dict(spec["step"].get("parameters") or {})
        target_artifact = str(params.get("target_artifact") or "").strip()
        selected_fix_keys = sorted({str(item).strip() for item in params.get("fix_keys", []) if str(item).strip()})
        status = str(params.get("status") or "").strip().lower()
        issue_id = str(params.get("issue_id") or "").strip()
        issue_type = str(params.get("issue_type") or "").strip()
        issue_field = str(params.get("issue_field") or "").strip()
        issue_threshold = str(params.get("issue_threshold") or "").strip()
        issue_note = str(params.get("issue_note") or "").strip()
        owner_question = str(params.get("owner_question") or "").strip()
        user = str(params.get("user") or "").strip()
        if status not in {"suspected", "confirmed"}:
            raise SystemExit("Invalid outlier status")
        if not target_artifact:
            raise SystemExit("Missing target artifact")
        if not selected_fix_keys:
            raise SystemExit("No fixes were selected")

        source = None
        for artifact in spec.get("input_artifacts", []):
            if artifact.get("logical_name") == target_artifact:
                source = artifact
                break
        output = None
        for artifact in spec.get("output_artifacts", []):
            if artifact.get("logical_name") == target_artifact:
                output = artifact
                break
        if source is None or output is None:
            raise SystemExit("Missing input or output artifact")

        fieldnames, _, rows, valid_records = load_rows_with_context(source["path"])
        selected_set = set(selected_fix_keys)
        matched = set()
        context_by_row = {record["row_index"]: record for record in valid_records}
        output_fieldnames = list(fieldnames)
        for name in REVIEW_COLUMNS:
            if name not in output_fieldnames:
                output_fieldnames.append(name)

        output_path = Path(output["path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        reviewed_at = now_iso()
        new_issue = clean_issue_payload({
            "status": status,
            "issue_id": issue_id,
            "issue_type": issue_type,
            "issue_field": issue_field,
            "issue_threshold": issue_threshold,
            "issue_note": issue_note,
            "owner_question": owner_question,
            "review_user": user,
            "reviewed_at": reviewed_at,
        })
        with output_path.open("w", newline="", encoding="utf-8") as output_handle:
            writer = csv.DictWriter(output_handle, fieldnames=output_fieldnames)
            writer.writeheader()
            for row_index, raw in enumerate(rows, start=1):
                row = dict(raw)
                context = context_by_row.get(row_index)
                if context and context["fix_key"] in selected_set:
                    matched.add(context["fix_key"])
                    issues = parse_issue_refs(raw)
                    issues.append(new_issue)
                    row["vc_outlier_status"] = aggregate_issue_status(issues)
                    row["vc_issue_id"] = issue_id
                    row["vc_issue_type"] = issue_type
                    row["vc_issue_field"] = issue_field
                    row["vc_issue_threshold"] = issue_threshold
                    row["vc_issue_refs"] = json.dumps(issues, sort_keys=True)
                    row["vc_issue_note"] = issue_note
                    row["vc_owner_question"] = owner_question
                    row["vc_review_user"] = user
                    row["vc_reviewed_at"] = reviewed_at
                writer.writerow({name: row.get(name, "") for name in output_fieldnames})

        if not matched:
            raise SystemExit("None of the selected fixes were found")

        write_json(summary_path, {
            "app": "movement",
            "action": "annotate_fixes",
            "target_artifact": target_artifact,
            "status": status,
            "issue_id": issue_id,
            "issue_type": issue_type,
            "issue_field": issue_field,
            "issue_threshold": issue_threshold,
            "issue_note": issue_note,
            "owner_question": owner_question,
            "selected_fix_keys": selected_fix_keys,
            "matched_fix_keys": sorted(matched),
            "missing_fix_keys": sorted(selected_set - matched),
            "annotated_fix_count": len(matched),
            "reviewed_at": reviewed_at,
            "review_user": user,
        })


if __name__ == "__main__":
    main()
    """
).strip() + "\n"


REMOVE_CONFIRMED_FIXES_SCRIPT = SCRIPT_SHARED + textwrap.dedent(
    """

    def main():
        spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
        summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
        spec = json.loads(spec_path.read_text())
        params = dict(spec["step"].get("parameters") or {})
        target_artifact = str(params.get("target_artifact") or "").strip()
        selected_fix_keys = sorted({str(item).strip() for item in params.get("fix_keys", []) if str(item).strip()})
        reason = str(params.get("reason") or "").strip()
        if not target_artifact:
            raise SystemExit("Missing target artifact")
        if not selected_fix_keys:
            raise SystemExit("No confirmed fixes were selected")

        source = None
        for artifact in spec.get("input_artifacts", []):
            if artifact.get("logical_name") == target_artifact:
                source = artifact
                break
        output = None
        for artifact in spec.get("output_artifacts", []):
            if artifact.get("logical_name") == target_artifact:
                output = artifact
                break
        if source is None or output is None:
            raise SystemExit("Missing input or output artifact")

        fieldnames, _, rows, valid_records = load_rows_with_context(source["path"])
        selected_set = set(selected_fix_keys)
        contexts = selected_contexts(valid_records, selected_fix_keys=selected_fix_keys)
        matched = {record["fix_key"] for record in contexts}
        missing = sorted(selected_set - matched)
        blocked = []
        for record in contexts:
            if record["review"].get("vc_outlier_status", "").strip().lower() != "confirmed":
                blocked.append(record["fix_key"])
        if missing:
            raise SystemExit("Some selected fixes were not found in the current dataset")
        if blocked:
            raise SystemExit("Only confirmed fixes can be removed")

        remove_keys = {record["fix_key"] for record in contexts}
        remove_row_indexes = {record["row_index"] for record in contexts}
        removed_issue_ids = sorted({
            str(item.get("issue_id", "")).strip()
            for record in contexts
            for item in record["review"].get("issues", [])
            if str(item.get("issue_id", "")).strip()
        })
        affected_individuals = sorted({record["individual"] for record in contexts})

        output_path = Path(output["path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        retained_rows = 0
        with output_path.open("w", newline="", encoding="utf-8") as output_handle:
            writer = csv.DictWriter(output_handle, fieldnames=fieldnames)
            writer.writeheader()
            for row_index, raw in enumerate(rows, start=1):
                if row_index in remove_row_indexes:
                    continue
                writer.writerow({name: raw.get(name, "") for name in fieldnames})
                retained_rows += 1

        write_json(summary_path, {
            "app": "movement",
            "action": "remove_confirmed_fixes",
            "target_artifact": target_artifact,
            "reason": reason,
            "selected_fix_keys": selected_fix_keys,
            "removed_fix_keys": sorted(remove_keys),
            "removed_fix_count": len(remove_keys),
            "retained_rows": retained_rows,
            "issue_ids": removed_issue_ids,
            "affected_individuals": affected_individuals,
        })


    if __name__ == "__main__":
        main()
    """
).strip() + "\n"


GENERATE_REPORT_SCRIPT = SCRIPT_SHARED + textwrap.dedent(
    """

    def build_markdown_report(target_artifact, user, screenshot_mode, issue_sections, snapshots_by_key, selected_count):
        lines = [
            "# Movement Outlier Review Report",
            "",
            f"- Artifact: `{target_artifact}`",
            f"- Generated by: {user}",
            f"- Screenshot mode: {screenshot_mode}",
            f"- Selected fixes: {selected_count}",
            f"- Generated at: {now_iso()}",
            "",
        ]
        for issue_type, windows in issue_sections:
            lines.extend([
                f"## Issue Type: {issue_type}",
                "",
            ])
            for window, records in windows:
                first = records[0]
                status_counts = {}
                issue_ids = sorted({record["review"].get("vc_issue_id", "").strip() for record in records if record["review"].get("vc_issue_id", "").strip()})
                max_step = max((record["step_length_m"] or 0.0) for record in records)
                max_speed = max((record["speed_mps"] or 0.0) for record in records)
                quality_fields = []
                for name in extract_quality_fields(list(first["raw"].keys()), detect_columns(list(first["raw"].keys()))):
                    if any(str(record["raw"].get(name, "")).strip() for record in records):
                        quality_fields.append(name)
                for record in records:
                    status = record["review"].get("vc_outlier_status", "").strip().lower() or "unreviewed"
                    status_counts[status] = status_counts.get(status, 0) + 1
                lines.extend([
                    f"### {window['individual']} | {window['start_time_text']} to {window['end_time_text']}",
                    "",
                    f"- Track: {window['set_name']}",
                    f"- Window fixes on map: {window['window_fix_count']}",
                    f"- Suspicious fixes in this section: {len(records)}",
                    f"- Issue ids: {', '.join(issue_ids) if issue_ids else 'none yet'}",
                    f"- Status counts: {', '.join(f'{key}={value}' for key, value in sorted(status_counts.items()))}",
                    f"- Max step length: {max_step:.2f} m",
                    f"- Max speed: {max_speed:.3f} m/s",
                    "",
                    "#### Description",
                    first["review"].get("vc_issue_note", "").strip() or "Potential location error requiring owner review.",
                    "",
                    "#### Owner Question",
                    first["review"].get("vc_owner_question", "").strip() or "Could you confirm whether these locations should be treated as outliers?",
                    "",
                    "#### Snapshot",
                    "",
                ])
                window_snapshot = snapshots_by_key.get(window["snapshot_key"])
                if screenshot_mode == "auto" and window_snapshot:
                    caption = window_snapshot.get("caption", "").strip() or window_snapshot["artifact_name"]
                    lines.append(f"![{caption}]({window_snapshot['artifact_name']})")
                    lines.append("")
                else:
                    lines.extend([
                        "[Add snapshot of the relevant 50-fix context window here.]",
                        "",
                    ])
                lines.extend([
                    "#### Evidence",
                ])
                for name in quality_fields[:6]:
                    values = sorted({str(record["raw"].get(name, "")).strip() for record in records if str(record["raw"].get(name, "")).strip()})
                    if values:
                        lines.append(f"- {name}: {', '.join(values[:8])}")
                lines.extend([
                    "",
                    "#### Suspicious Fixes",
                    "",
                    "| Fix Key | Individual | Timestamp | Longitude | Latitude | Step (m) | Speed (m/s) | Status | Issue Id |",
                    "| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
                ])
                for record in records:
                    lines.append(
                        f"| {record['fix_key']} | {record['individual']} | {record['time_text']} | {record['lon']:.6f} | {record['lat']:.6f} | "
                        f"{(record['step_length_m'] or 0.0):.2f} | {(record['speed_mps'] or 0.0):.3f} | "
                        f"{record['review'].get('vc_outlier_status', '').strip() or 'unreviewed'} | "
                        f"{record['review'].get('vc_issue_id', '').strip() or 'n/a'} |"
                    )
                lines.append("")
        return "\\n".join(lines).rstrip() + "\\n"


def build_html_report(target_artifact, user, screenshot_mode, issue_sections, snapshots_by_key, selected_count):
        parts = [
            "<!DOCTYPE html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            "<title>Movement Outlier Review Report</title>",
            "<style>",
            "body { font-family: Arial, sans-serif; line-height: 1.5; color: #1f2933; margin: 0; background: #f6f8fb; }",
            "main { max-width: 1120px; margin: 0 auto; padding: 32px 24px 48px; }",
            "header { margin-bottom: 28px; }",
            "h1, h2, h3, h4 { color: #102a43; margin-bottom: 0.5rem; }",
            "h2 { margin-top: 2rem; padding-bottom: 0.35rem; border-bottom: 2px solid #d9e2ec; }",
            "section.window { background: #ffffff; border: 1px solid #d9e2ec; border-radius: 12px; padding: 20px; margin: 20px 0; box-shadow: 0 4px 16px rgba(15, 23, 42, 0.06); }",
            "ul.meta, ul.evidence { margin: 0.5rem 0 1rem 1.25rem; padding: 0; }",
            "figure { margin: 1rem 0; }",
            "figure img { max-width: 100%; height: auto; border: 1px solid #bcccdc; border-radius: 8px; background: #ffffff; }",
            "figcaption { color: #52606d; font-size: 0.95rem; margin-top: 0.4rem; }",
            "table { width: 100%; border-collapse: collapse; margin-top: 0.75rem; font-size: 0.95rem; background: #ffffff; }",
            "th, td { border: 1px solid #d9e2ec; padding: 0.55rem 0.65rem; text-align: left; vertical-align: top; }",
            "th { background: #eef2f7; color: #243b53; }",
            "td.numeric { text-align: right; font-variant-numeric: tabular-nums; }",
            "p.placeholder { font-style: italic; color: #52606d; }",
            "code { font-family: SFMono-Regular, Consolas, 'Liberation Mono', Menlo, monospace; font-size: 0.95em; }",
            "</style>",
            "</head>",
            "<body>",
            "<main>",
            "<header>",
            "<h1>Movement Outlier Review Report</h1>",
            '<ul class="meta">',
            f"<li><strong>Artifact:</strong> <code>{html_escape(target_artifact)}</code></li>",
            f"<li><strong>Generated by:</strong> {html_escape(user)}</li>",
            f"<li><strong>Screenshot mode:</strong> {html_escape(screenshot_mode)}</li>",
            f"<li><strong>Selected fixes:</strong> {selected_count}</li>",
            f"<li><strong>Generated at:</strong> {html_escape(now_iso())}</li>",
            "</ul>",
            "</header>",
        ]
        for issue_type, windows in issue_sections:
            parts.extend([
                "<section>",
                f"<h2>Issue Type: {html_escape(issue_type)}</h2>",
            ])
            for window, records in windows:
                first = records[0]
                status_counts = {}
                issue_ids = sorted({record["review"].get("vc_issue_id", "").strip() for record in records if record["review"].get("vc_issue_id", "").strip()})
                max_step = max((record["step_length_m"] or 0.0) for record in records)
                max_speed = max((record["speed_mps"] or 0.0) for record in records)
                quality_fields = []
                for name in extract_quality_fields(list(first["raw"].keys()), detect_columns(list(first["raw"].keys()))):
                    if any(str(record["raw"].get(name, "")).strip() for record in records):
                        quality_fields.append(name)
                for record in records:
                    status = record["review"].get("vc_outlier_status", "").strip().lower() or "unreviewed"
                    status_counts[status] = status_counts.get(status, 0) + 1

                window_snapshot = snapshots_by_key.get(window["snapshot_key"])
                if screenshot_mode == "auto" and window_snapshot:
                    caption = window_snapshot.get("caption", "").strip() or window_snapshot["artifact_name"]
                    snapshot_markup = (
                        "<figure>"
                        f'<img src="{html_escape(window_snapshot["artifact_name"])}" alt="{html_escape(caption)}">'
                        f"<figcaption>{html_escape(caption)}</figcaption>"
                        "</figure>"
                    )
                else:
                    snapshot_markup = '<p class="placeholder">[Add snapshot of the relevant 50-fix context window here.]</p>'

                evidence_items = []
                for name in quality_fields[:6]:
                    values = sorted({str(record["raw"].get(name, "")).strip() for record in records if str(record["raw"].get(name, "")).strip()})
                    if values:
                        evidence_items.append(
                            f"<li><strong>{html_escape(name)}:</strong> {html_escape(', '.join(values[:8]))}</li>"
                        )
                if evidence_items:
                    evidence_markup = '<ul class="evidence">' + "".join(evidence_items) + "</ul>"
                else:
                    evidence_markup = "<p>No additional quality fields were populated for these fixes.</p>"

                rows = []
                for record in records:
                    rows.append(
                        "<tr>"
                        f"<td><code>{html_escape(record['fix_key'])}</code></td>"
                        f"<td>{html_escape(record['individual'])}</td>"
                        f"<td>{html_escape(record['time_text'])}</td>"
                        f'<td class="numeric">{record["lon"]:.6f}</td>'
                        f'<td class="numeric">{record["lat"]:.6f}</td>'
                        f'<td class="numeric">{(record["step_length_m"] or 0.0):.2f}</td>'
                        f'<td class="numeric">{(record["speed_mps"] or 0.0):.3f}</td>'
                        f"<td>{html_escape(record['review'].get('vc_outlier_status', '').strip() or 'unreviewed')}</td>"
                        f"<td>{html_escape(record['review'].get('vc_issue_id', '').strip() or 'n/a')}</td>"
                        "</tr>"
                    )

                parts.extend([
                    '<section class="window">',
                    f"<h3>{html_escape(window['individual'])} | {html_escape(window['start_time_text'])} to {html_escape(window['end_time_text'])}</h3>",
                    '<ul class="meta">',
                    f"<li><strong>Track:</strong> {html_escape(window['set_name'])}</li>",
                    f"<li><strong>Window fixes on map:</strong> {window['window_fix_count']}</li>",
                    f"<li><strong>Suspicious fixes in this section:</strong> {len(records)}</li>",
                    f"<li><strong>Issue ids:</strong> {html_escape(', '.join(issue_ids) if issue_ids else 'none yet')}</li>",
                    f"<li><strong>Status counts:</strong> {html_escape(', '.join(f'{key}={value}' for key, value in sorted(status_counts.items())))}</li>",
                    f"<li><strong>Max step length:</strong> {max_step:.2f} m</li>",
                    f"<li><strong>Max speed:</strong> {max_speed:.3f} m/s</li>",
                    "</ul>",
                    "<h4>Description</h4>",
                    f"<p>{html_escape(first['review'].get('vc_issue_note', '').strip() or 'Potential location error requiring owner review.')}</p>",
                    "<h4>Owner Question</h4>",
                    f"<p>{html_escape(first['review'].get('vc_owner_question', '').strip() or 'Could you confirm whether these locations should be treated as outliers?')}</p>",
                    "<h4>Snapshot</h4>",
                    snapshot_markup,
                    "<h4>Evidence</h4>",
                    evidence_markup,
                    "<h4>Suspicious Fixes</h4>",
                    "<table>",
                    "<thead>",
                    "<tr><th>Fix Key</th><th>Individual</th><th>Timestamp</th><th>Longitude</th><th>Latitude</th><th>Step (m)</th><th>Speed (m/s)</th><th>Status</th><th>Issue Id</th></tr>",
                    "</thead>",
                    "<tbody>",
                    "".join(rows),
                    "</tbody>",
                    "</table>",
                    "</section>",
                ])
            parts.append("</section>")
        parts.extend([
            "</main>",
            "</body>",
            "</html>",
        ])
        return "\\n".join(parts).rstrip() + "\\n"


    def normalize_report_records(items):
        normalized = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            review_in = dict(item.get("review") or {})
            attributes_in = dict(item.get("attributes") or {})
            time_ms = try_float(item.get("time_ms"))
            lon = try_float(item.get("lon"))
            lat = try_float(item.get("lat"))
            if time_ms is None or lon is None or lat is None:
                continue
            review = {
                "vc_outlier_status": str(review_in.get("status", "")).strip(),
                "vc_issue_id": str(review_in.get("issue_id", "")).strip(),
                "vc_issue_type": str(review_in.get("issue_type", "")).strip(),
                "vc_issue_note": str(review_in.get("issue_note", "")).strip(),
                "vc_owner_question": str(review_in.get("owner_question", "")).strip(),
                "vc_review_user": str(review_in.get("review_user", "")).strip(),
                "vc_reviewed_at": str(review_in.get("reviewed_at", "")).strip(),
            }
            raw = {}
            for key, value in attributes_in.items():
                name = str(key).strip()
                if not name:
                    continue
                raw[name] = "" if value is None else str(value)
            raw.update(review)
            normalized.append(
                {
                    "fix_key": str(item.get("fix_key", "")).strip(),
                    "individual": str(item.get("individual", "")).strip(),
                    "set_name": str(item.get("set_name", "")).strip() or "train",
                    "time_ms": int(time_ms),
                    "time_text": str(item.get("time_text") or format_timestamp(time_ms)).strip(),
                    "lon": float(lon),
                    "lat": float(lat),
                    "step_length_m": try_float(item.get("step_length_m")),
                    "speed_mps": try_float(item.get("speed_mps")),
                    "time_delta_s": try_float(item.get("time_delta_s")),
                    "review": review,
                    "raw": raw,
                }
            )
        return normalized


    def normalize_snapshot_windows(items):
        normalized = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            snapshot_key = str(item.get("snapshot_key", "")).strip()
            if not snapshot_key:
                continue
            normalized.append(
                {
                    "snapshot_key": snapshot_key,
                    "caption": str(item.get("caption", "")).strip(),
                    "individual": str(item.get("individual", "")).strip(),
                    "set_name": str(item.get("set_name", "")).strip() or "train",
                    "issue_type": str(item.get("issue_type", "")).strip() or "Unspecified issue",
                    "issue_types": sorted({str(value).strip() for value in item.get("issue_types", []) if str(value).strip()}),
                    "anchor_fix_keys": sorted({str(value).strip() for value in item.get("anchor_fix_keys", []) if str(value).strip()}),
                    "report_fix_keys": sorted({str(value).strip() for value in item.get("report_fix_keys", []) if str(value).strip()}),
                    "start_fix_key": str(item.get("start_fix_key", "")).strip(),
                    "end_fix_key": str(item.get("end_fix_key", "")).strip(),
                    "start_time_ms": int(try_float(item.get("start_time_ms")) or 0),
                    "end_time_ms": int(try_float(item.get("end_time_ms")) or 0),
                    "start_time_text": str(item.get("start_time_text", "")).strip(),
                    "end_time_text": str(item.get("end_time_text", "")).strip(),
                    "window_fix_count": int(try_float(item.get("window_fix_count")) or 0),
                }
            )
        return normalized


    def main():
        spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
        summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
        spec = json.loads(spec_path.read_text())
        params = dict(spec["analysis"].get("parameters") or {})
        target_artifact = str(params.get("target_artifact") or "").strip()
        selected_fix_keys = sorted({str(item).strip() for item in params.get("fix_keys", []) if str(item).strip()})
        selected_issue_ids = sorted({str(item).strip() for item in params.get("issue_ids", []) if str(item).strip()})
        report_fixes = normalize_report_records(params.get("report_fixes") or [])
        snapshot_windows = normalize_snapshot_windows(params.get("snapshot_windows") or [])
        screenshot_mode = str(params.get("screenshot_mode") or "manual").strip().lower()
        snapshots = list(params.get("snapshots") or [])
        user = str(params.get("user") or "").strip()
        if screenshot_mode not in {"manual", "auto"}:
            raise SystemExit("Invalid screenshot mode")
        if not target_artifact:
            raise SystemExit("Missing target artifact")
        if not selected_fix_keys and not selected_issue_ids and not report_fixes:
            raise SystemExit("Select at least one issue or fix before generating a report")

        output_by_name = {artifact["logical_name"]: artifact for artifact in spec.get("output_artifacts", [])}
        required_outputs = {
            "movement_outlier_report.md",
            "movement_outlier_report.html",
            "movement_outlier_fixes.csv",
        }
        if any(name not in output_by_name for name in required_outputs):
            raise SystemExit("Report outputs were not declared")

        if report_fixes:
            matched_records = report_fixes
            fieldnames = sorted({key for record in matched_records for key in record["raw"].keys()})
            columns = detect_columns(fieldnames) if fieldnames else {}
        else:
            source = None
            for artifact in spec.get("input_artifacts", []):
                if artifact.get("logical_name") == target_artifact:
                    source = artifact
                    break
            if source is None:
                raise SystemExit("Target artifact was not provided as an input")
            fieldnames, columns, _, valid_records = load_rows_with_context(source["path"])
            matched_records = selected_contexts(
                valid_records,
                selected_fix_keys=selected_fix_keys,
                selected_issue_ids=selected_issue_ids,
            )
        if not matched_records:
            raise SystemExit("None of the selected fixes or issues were found")
        matched_records.sort(key=lambda record: (record["individual"], record["time_ms"], record["fix_key"]))

        record_by_fix_key = {record["fix_key"]: record for record in matched_records}
        if not snapshot_windows:
            snapshot_windows = [
                {
                    "snapshot_key": f"snapshot_{index + 1:02d}",
                    "caption": f"{record['review'].get('vc_issue_type', '').strip() or 'Unspecified issue'} | {record['individual']} | {record['time_text']}",
                    "individual": record["individual"],
                    "set_name": record.get("set_name", "train"),
                    "issue_type": record["review"].get("vc_issue_type", "").strip() or "Unspecified issue",
                    "issue_types": [record["review"].get("vc_issue_type", "").strip() or "Unspecified issue"],
                    "anchor_fix_keys": [record["fix_key"]],
                    "report_fix_keys": [record["fix_key"]],
                    "start_fix_key": record["fix_key"],
                    "end_fix_key": record["fix_key"],
                    "start_time_ms": record["time_ms"],
                    "end_time_ms": record["time_ms"],
                    "start_time_text": record["time_text"],
                    "end_time_text": record["time_text"],
                    "window_fix_count": 1,
                }
                for index, record in enumerate(matched_records)
            ]

        snapshots_by_key = {}
        realized_snapshots = []
        for snapshot in snapshots:
            artifact_name = str(snapshot.get("artifact_name") or "").strip()
            if not artifact_name or artifact_name not in output_by_name:
                continue
            raw_bytes = decode_data_url(snapshot.get("data_url"))
            output_path = Path(output_by_name[artifact_name]["path"])
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(raw_bytes)
            snapshot_key = str(snapshot.get("snapshot_key") or "").strip()
            snapshots_by_key[snapshot_key] = snapshot
            realized_snapshots.append(artifact_name)

        windows_by_issue_type = {}
        for window in snapshot_windows:
            window_records = [
                record_by_fix_key[fix_key]
                for fix_key in window.get("report_fix_keys", [])
                if fix_key in record_by_fix_key
            ]
            if not window_records:
                continue
            issue_types = sorted({
                record["review"].get("vc_issue_type", "").strip() or "Unspecified issue"
                for record in window_records
            })
            for issue_type in issue_types:
                typed_records = [
                    record
                    for record in window_records
                    if (record["review"].get("vc_issue_type", "").strip() or "Unspecified issue") == issue_type
                ]
                if not typed_records:
                    continue
                windows_by_issue_type.setdefault(issue_type, []).append((window, typed_records))

        ordered_issue_sections = [
            (
                issue_type,
                sorted(
                    windows,
                    key=lambda item: (
                        item[0]["individual"],
                        item[0]["start_time_ms"],
                        item[0]["snapshot_key"],
                    ),
                ),
            )
            for issue_type, windows in sorted(windows_by_issue_type.items(), key=lambda item: item[0])
        ]
        report_text = build_markdown_report(
            target_artifact,
            user,
            screenshot_mode,
            ordered_issue_sections,
            snapshots_by_key,
            len(matched_records),
        )
        report_path = Path(output_by_name["movement_outlier_report.md"]["path"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report_text, encoding="utf-8")
        html_report_text = build_html_report(
            target_artifact,
            user,
            screenshot_mode,
            ordered_issue_sections,
            snapshots_by_key,
            len(matched_records),
        )
        html_report_path = Path(output_by_name["movement_outlier_report.html"]["path"])
        html_report_path.parent.mkdir(parents=True, exist_ok=True)
        html_report_path.write_text(html_report_text, encoding="utf-8")

        appendix_fields = [
            "issue_id",
            "issue_type",
            "fix_key",
            "individual",
            "timestamp",
            "longitude",
            "latitude",
            "step_length_m",
            "speed_mps",
            "time_delta_s",
            "status",
            "owner_question",
            "issue_note",
        ]
        quality_fields = extract_quality_fields(fieldnames, columns)
        appendix_fields.extend(quality_fields)
        appendix_path = Path(output_by_name["movement_outlier_fixes.csv"]["path"])
        appendix_path.parent.mkdir(parents=True, exist_ok=True)
        with appendix_path.open("w", newline="", encoding="utf-8") as output_handle:
            writer = csv.DictWriter(output_handle, fieldnames=appendix_fields)
            writer.writeheader()
            for record in matched_records:
                row = {
                    "issue_id": record["review"].get("vc_issue_id", "").strip(),
                    "issue_type": record["review"].get("vc_issue_type", "").strip(),
                    "fix_key": record["fix_key"],
                    "individual": record["individual"],
                    "timestamp": record["time_text"],
                    "longitude": f"{record['lon']:.6f}",
                    "latitude": f"{record['lat']:.6f}",
                    "step_length_m": "" if record["step_length_m"] is None else f"{record['step_length_m']:.6f}",
                    "speed_mps": "" if record["speed_mps"] is None else f"{record['speed_mps']:.6f}",
                    "time_delta_s": "" if record["time_delta_s"] is None else f"{record['time_delta_s']:.6f}",
                    "status": record["review"].get("vc_outlier_status", "").strip(),
                    "owner_question": record["review"].get("vc_owner_question", "").strip(),
                    "issue_note": record["review"].get("vc_issue_note", "").strip(),
                }
                for name in quality_fields:
                    row[name] = str(record["raw"].get(name, "")).strip()
                writer.writerow(row)

        write_json(summary_path, {
            "app": "movement",
            "action": "generate_report",
            "target_artifact": target_artifact,
            "selected_fix_keys": selected_fix_keys,
            "selected_issue_ids": selected_issue_ids,
            "selected_report_fix_count": len(report_fixes),
            "matched_fix_count": len(matched_records),
            "matched_issue_types": [issue_type for issue_type, _ in ordered_issue_sections],
            "screenshot_mode": screenshot_mode,
            "snapshot_window_count": len(snapshot_windows),
            "realized_snapshots": realized_snapshots,
        })


    if __name__ == "__main__":
        main()
    """
).strip() + "\n"

REPORT_ANALYSIS_TEMPLATE_PATH = Path(__file__).with_name("report_analysis_template.py")
GENERATE_REPORT_SCRIPT = REPORT_ANALYSIS_TEMPLATE_PATH.read_text(encoding="utf-8").strip() + "\n"
compile(GENERATE_REPORT_SCRIPT, str(REPORT_ANALYSIS_TEMPLATE_PATH), "exec")


def _validate_fix_keys(value: object, *, allow_empty: bool = False) -> list[str]:
    if value is None and allow_empty:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid fix list")
    cleaned = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("Invalid fix list")
        key = " ".join(item.strip().split())
        if not key:
            raise ValueError("Invalid fix list")
        cleaned.append(key)
    unique = sorted(set(cleaned))
    if not unique and not allow_empty:
        raise ValueError("Select at least one fix")
    return unique


def _validate_issue_ids(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid issue list")
    cleaned = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("Invalid issue list")
        issue_id = " ".join(item.strip().split())
        if not issue_id:
            raise ValueError("Invalid issue list")
        cleaned.append(issue_id)
    return sorted(set(cleaned))


def _validate_status(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Status is required")
    status = value.strip().lower()
    if status not in {"suspected", "confirmed"}:
        raise ValueError("Status must be suspected or confirmed")
    return status


def _validate_required_text(value: object, *, label: str, max_length: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} is required")
    normalized = " ".join(value.strip().split())
    if not normalized:
        raise ValueError(f"{label} is required")
    if len(normalized) > max_length:
        raise ValueError(f"{label} is too long")
    return normalized


def _validate_optional_text(value: object, *, label: str, max_length: int) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"Invalid {label.lower()}")
    normalized = " ".join(value.strip().split())
    if len(normalized) > max_length:
        raise ValueError(f"{label} is too long")
    return normalized


def _validate_screenshot_mode(value: object) -> str:
    if value is None:
        return "manual"
    if not isinstance(value, str):
        raise ValueError("Invalid screenshot mode")
    mode = value.strip().lower()
    if mode not in {"manual", "auto"}:
        raise ValueError("Invalid screenshot mode")
    return mode


def _validate_snapshots(value: object) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid snapshots payload")
    snapshots = []
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            raise ValueError("Invalid snapshots payload")
        data_url = item.get("data_url")
        if not isinstance(data_url, str) or not data_url.strip():
            raise ValueError("Each snapshot must include image data")
        caption = _validate_optional_text(item.get("caption"), label="Snapshot caption", max_length=240)
        snapshot_key = _validate_required_text(item.get("snapshot_key"), label="Snapshot key", max_length=120)
        snapshots.append(
            {
                "artifact_name": f"movement_snapshot_{index:02d}.png",
                "caption": caption,
                "data_url": data_url.strip(),
                "snapshot_key": snapshot_key,
            }
        )
    return snapshots


def _validate_snapshot_windows(value: object) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid snapshot windows payload")
    cleaned = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("Invalid snapshot windows payload")
        cleaned.append(
            {
                "snapshot_key": _validate_required_text(item.get("snapshot_key"), label="Snapshot key", max_length=120),
                "caption": _validate_optional_text(item.get("caption"), label="Snapshot caption", max_length=240),
                "individual": _validate_optional_text(item.get("individual"), label="Individual", max_length=200),
                "set_name": _validate_optional_text(item.get("set_name"), label="Track", max_length=40),
                "issue_type": _validate_optional_text(item.get("issue_type"), label="Issue type", max_length=120),
                "issue_types": _validate_issue_ids(item.get("issue_types")),
                "anchor_fix_keys": _validate_fix_keys(item.get("anchor_fix_keys"), allow_empty=True),
                "report_fix_keys": _validate_fix_keys(item.get("report_fix_keys"), allow_empty=True),
                "start_fix_key": _validate_optional_text(item.get("start_fix_key"), label="Start fix key", max_length=240),
                "end_fix_key": _validate_optional_text(item.get("end_fix_key"), label="End fix key", max_length=240),
                "start_time_ms": item.get("start_time_ms"),
                "end_time_ms": item.get("end_time_ms"),
                "start_time_text": _validate_optional_text(item.get("start_time_text"), label="Start time", max_length=120),
                "end_time_text": _validate_optional_text(item.get("end_time_text"), label="End time", max_length=120),
                "window_fix_count": item.get("window_fix_count"),
            }
        )
    return cleaned


def _validate_report_fixes(value: object) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid report fixes payload")
    cleaned = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("Invalid report fixes payload")
        review = item.get("review") or {}
        attributes = item.get("attributes") or {}
        if not isinstance(review, dict) or not isinstance(attributes, dict):
            raise ValueError("Invalid report fixes payload")
        raw_issues = review.get("issues") or []
        if raw_issues is None:
            raw_issues = []
        if not isinstance(raw_issues, list):
            raise ValueError("Invalid report fixes payload")
        cleaned_attributes = {}
        for key, raw_value in attributes.items():
            name = _validate_optional_text(key, label="Attribute name", max_length=120)
            if not name:
                continue
            if raw_value is None or isinstance(raw_value, (str, int, float, bool)):
                cleaned_attributes[name] = raw_value
            else:
                raise ValueError("Invalid report fixes payload")
        cleaned.append(
            {
                "fix_key": _validate_optional_text(item.get("fix_key"), label="Fix key", max_length=240),
                "individual": _validate_optional_text(item.get("individual"), label="Individual", max_length=200),
                "set_name": _validate_optional_text(item.get("set_name"), label="Track", max_length=40),
                "time_ms": item.get("time_ms"),
                "time_text": _validate_optional_text(item.get("time_text"), label="Timestamp", max_length=120),
                "lon": item.get("lon"),
                "lat": item.get("lat"),
                "step_length_m": item.get("step_length_m"),
                "speed_mps": item.get("speed_mps"),
                "time_delta_s": item.get("time_delta_s"),
                "attributes": cleaned_attributes,
                "review": {
                    "status": _validate_optional_text(review.get("status"), label="Status", max_length=40),
                    "issue_id": _validate_optional_text(review.get("issue_id"), label="Issue id", max_length=120),
                    "issue_type": _validate_optional_text(review.get("issue_type"), label="Issue type", max_length=120),
                    "issue_field": _validate_optional_text(review.get("issue_field"), label="Issue field", max_length=120),
                    "issue_threshold": _validate_optional_text(review.get("issue_threshold"), label="Issue threshold", max_length=120),
                    "issues": [
                        {
                            "status": _validate_optional_text(issue.get("status"), label="Status", max_length=40),
                            "issue_id": _validate_optional_text(issue.get("issue_id"), label="Issue id", max_length=120),
                            "issue_type": _validate_optional_text(issue.get("issue_type"), label="Issue type", max_length=120),
                            "issue_field": _validate_optional_text(issue.get("issue_field"), label="Issue field", max_length=120),
                            "issue_threshold": _validate_optional_text(issue.get("issue_threshold"), label="Issue threshold", max_length=120),
                            "issue_note": _validate_optional_text(issue.get("issue_note"), label="Issue note", max_length=1200),
                            "owner_question": _validate_optional_text(issue.get("owner_question"), label="Owner question", max_length=600),
                            "review_user": _validate_optional_text(issue.get("review_user"), label="Review user", max_length=120),
                            "reviewed_at": _validate_optional_text(issue.get("reviewed_at"), label="Reviewed at", max_length=120),
                        }
                        for issue in raw_issues
                        if isinstance(issue, dict)
                    ],
                    "issue_note": _validate_optional_text(review.get("issue_note"), label="Issue note", max_length=1200),
                    "owner_question": _validate_optional_text(review.get("owner_question"), label="Owner question", max_length=600),
                    "review_user": _validate_optional_text(review.get("review_user"), label="Review user", max_length=120),
                    "reviewed_at": _validate_optional_text(review.get("reviewed_at"), label="Reviewed at", max_length=120),
                },
            }
        )
    return cleaned


def _validate_report_type(value: object) -> str:
    if value is None:
        return "issue_first"
    if not isinstance(value, str):
        raise ValueError("Invalid report type")
    report_type = value.strip().lower()
    if report_type not in {"issue_first", "individual_profile"}:
        raise ValueError("Invalid report type")
    return report_type


def _validate_output_mode(value: object) -> str:
    if value is None:
        return "separate"
    if not isinstance(value, str):
        raise ValueError("Invalid output mode")
    output_mode = value.strip().lower()
    if output_mode not in {"combined", "separate"}:
        raise ValueError("Invalid output mode")
    return output_mode


def _normalize_individual_name(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Invalid individual")
    normalized = " ".join(value.strip().split())
    if not normalized or any(ord(char) < 32 for char in normalized):
        raise ValueError("Invalid individual")
    return normalized


def _validate_report_individuals(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Invalid individuals")
    cleaned = []
    seen = set()
    for item in value:
        individual = _normalize_individual_name(item)
        if individual in seen:
            continue
        cleaned.append(individual)
        seen.add(individual)
    return sorted(cleaned)


def _slugify_individual_name(value: str) -> str:
    chars = []
    last_sep = False
    for char in str(value or "").strip().lower():
        if char.isalnum():
            chars.append(char)
            last_sep = False
            continue
        if not last_sep:
            chars.append("_")
            last_sep = True
    slug = "".join(chars).strip("_")
    return slug or "individual"


def _build_individual_report_artifacts(individuals: list[str]) -> list[dict]:
    artifacts = []
    used = {}
    for index, individual in enumerate(individuals, start=1):
        slug = _slugify_individual_name(individual)
        occurrence = used.get(slug, 0) + 1
        used[slug] = occurrence
        suffix = slug if occurrence == 1 else f"{slug}_{occurrence}"
        stem = f"movement_individual_report_{index:02d}_{suffix}"
        artifacts.append(
            {
                "individual": individual,
                "markdown_name": f"{stem}.md",
                "html_name": f"{stem}.html",
            }
        )
    return artifacts


def register_movement_routes(app: FastAPI, *, data_root: Path):
    data_root = data_root.resolve()

    def parse_optional_int(raw_value: object, *, label: str) -> int | None:
        if raw_value in (None, ""):
            return None
        if isinstance(raw_value, bool):
            raise ValueError(f"Invalid {label}")
        try:
            return int(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid {label}") from exc

    def parse_optional_limit(raw_value: object) -> int | None:
        if raw_value in (None, ""):
            return None
        value = parse_optional_int(raw_value, label="limit")
        if value is None or value <= 0:
            raise ValueError("Invalid limit")
        return value

    def parse_optional_individual(raw_value: object) -> str:
        if raw_value in (None, ""):
            return ""
        return _normalize_individual_name(raw_value)

    def parse_optional_individuals(raw_values: list[str] | tuple[str, ...]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_value in raw_values:
            value = parse_optional_individual(raw_value)
            if not value or value in seen:
                continue
            normalized.append(value)
            seen.add(value)
        return normalized

    @app.get("/api/apps/movement/families")
    async def get_movement_families():
        return JSONResponse({"families": list_families(data_root)})

    @app.get("/api/apps/movement/family/{family_name}/studies")
    async def get_movement_studies(family_name: str):
        try:
            return JSONResponse(
                {
                    "family": validate_path_part(family_name, label="family"),
                    "studies": list_studies(data_root, family_name),
                }
            )
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/state")
    async def get_movement_study_state(family_name: str, study_name: str):
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            return JSONResponse(project_state_payload(study_dir))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/graph")
    async def get_movement_study_graph(family_name: str, study_name: str):
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            return JSONResponse(graph_payload(study_dir))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/load")
    async def get_movement_study_load(family_name: str, study_name: str):
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            return JSONResponse(await run_in_threadpool(_build_initial_study_payload, study_dir))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/dataset/{dataset_id}")
    async def get_movement_study_dataset(family_name: str, study_name: str, dataset_id: str):
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            return JSONResponse(load_dataset(study_dir, dataset_id))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/dataset/{dataset_id}/overview")
    async def get_movement_study_overview(family_name: str, study_name: str, dataset_id: str, logical_name: str):
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            _, artifact_path = get_dataset_artifact(study_dir, dataset_id, logical_name)
            return JSONResponse(await run_in_threadpool(build_movement_overview, artifact_path))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/dataset/{dataset_id}/fixes")
    async def get_movement_study_fixes(
        family_name: str,
        study_name: str,
        dataset_id: str,
        request: Request,
        logical_name: str,
        individual: str = "",
        start_ms: int | None = None,
        end_ms: int | None = None,
        review_status: str = "",
        limit: int | None = None,
    ):
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            _, artifact_path = get_dataset_artifact(study_dir, dataset_id, logical_name)
            individuals = parse_optional_individuals(request.query_params.getlist("individuals"))
            if not individuals:
                single_individual = parse_optional_individual(individual)
                if single_individual:
                    individuals = [single_individual]
            payload = await run_in_threadpool(
                build_movement_fixes,
                artifact_path,
                individuals=individuals or None,
                start_ms=parse_optional_int(start_ms, label="start_ms"),
                end_ms=parse_optional_int(end_ms, label="end_ms"),
                review_status=str(review_status or "").strip().lower(),
                limit=parse_optional_limit(limit) if limit not in (None, "") else DEFAULT_FIX_LIMIT,
            )
            return JSONResponse(payload)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/dataset/{dataset_id}/summary")
    async def get_movement_study_summary(family_name: str, study_name: str, dataset_id: str, logical_name: str):
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            _, artifact_path = get_dataset_artifact(study_dir, dataset_id, logical_name)
            return JSONResponse(await run_in_threadpool(build_movement_summary, artifact_path))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/apps/movement/family/{family_name}/study/{study_name}/analysis/{analysis_id}/artifact/{logical_name}")
    async def get_movement_analysis_artifact(
        family_name: str,
        study_name: str,
        analysis_id: str,
        logical_name: str,
    ):
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            analysis_dir = project_paths(study_dir)["analyses"] / validate_path_part(analysis_id, label="analysis")
            artifact_name = validate_path_part(logical_name, label="artifact")
            artifact_path = (analysis_dir / "outputs" / artifact_name).resolve()
            if study_dir.resolve() not in artifact_path.parents:
                raise ProjectStateError("Invalid artifact path")
            if not artifact_path.exists() or not artifact_path.is_file():
                raise ProjectStateError("Unknown artifact")
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)
        return FileResponse(artifact_path, media_type=media_type_for_path(artifact_path))

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/undo")
    async def post_movement_study_undo(family_name: str, study_name: str):
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            return JSONResponse(undo_to_parent(study_dir))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/actions/annotate-fixes")
    async def post_movement_annotate_fixes(family_name: str, study_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            fix_keys = _validate_fix_keys(body.get("fix_keys"))
            status = _validate_status(body.get("status"))
            issue_type = _validate_required_text(body.get("issue_type"), label="Issue type", max_length=120)
            issue_field = _validate_optional_text(body.get("issue_field"), label="Issue field", max_length=120)
            issue_threshold = _validate_optional_text(body.get("issue_threshold"), label="Issue threshold", max_length=120)
            issue_note = _validate_required_text(body.get("issue_note"), label="Issue note", max_length=1200)
            owner_question = _validate_required_text(body.get("owner_question"), label="Owner question", max_length=600)
            user = body.get("user")
            issue_id = f"issue_{uuid.uuid4().hex[:12]}"

            payload = {
                "user": user,
                "title": f"Mark {len(fix_keys)} fix(es) as {status} in {logical_name}",
                "kind": "python",
                "script": ANNOTATE_FIXES_SCRIPT,
                "parameters": {
                    "app": "movement",
                    "action": "annotate_fixes",
                    "target_artifact": logical_name,
                    "fix_keys": fix_keys,
                    "status": status,
                    "issue_id": issue_id,
                    "issue_type": issue_type,
                    "issue_field": issue_field,
                    "issue_threshold": issue_threshold,
                    "issue_note": issue_note,
                    "owner_question": owner_question,
                    "user": user,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": [logical_name],
                "output_artifacts": [logical_name],
                "set_as_head": True,
            }
            return JSONResponse(create_step(study_dir, payload))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/actions/generate-report")
    async def post_movement_generate_report(family_name: str, study_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            fix_keys = _validate_fix_keys(body.get("fix_keys"), allow_empty=True)
            issue_ids = _validate_issue_ids(body.get("issue_ids"))
            report_fixes = _validate_report_fixes(body.get("report_fixes"))
            report_type = _validate_report_type(body.get("report_type"))
            individuals = _validate_report_individuals(body.get("individuals"))
            output_mode = _validate_output_mode(body.get("output_mode"))
            snapshot_windows = _validate_snapshot_windows(body.get("snapshot_windows"))
            if report_type == "issue_first" and not fix_keys and not issue_ids and not report_fixes:
                raise ValueError("Select at least one issue or fix")
            if report_type == "individual_profile" and not individuals:
                raise ValueError("Select at least one individual")
            screenshot_mode = _validate_screenshot_mode(body.get("screenshot_mode"))
            snapshots = _validate_snapshots(body.get("snapshots"))
            user = body.get("user")
            individual_report_artifacts = _build_individual_report_artifacts(individuals)
            effective_output_mode = output_mode if len(individuals) > 1 else "combined"
            if report_type == "issue_first":
                output_artifacts = [
                    "movement_outlier_report.md",
                    "movement_outlier_report.html",
                    "movement_outlier_fixes.csv",
                ]
            elif effective_output_mode == "combined":
                output_artifacts = [
                    "movement_individual_reports.md",
                    "movement_individual_reports.html",
                ]
            else:
                output_artifacts = [
                    "movement_individual_report_index.md",
                    "movement_individual_report_index.html",
                ]
                for item in individual_report_artifacts:
                    output_artifacts.extend([item["markdown_name"], item["html_name"]])
            output_artifacts.extend(snapshot["artifact_name"] for snapshot in snapshots)

            payload = {
                "user": user,
                "title": (
                    f"Generate outlier report for {logical_name}"
                    if report_type == "issue_first"
                    else f"Generate individual profile report for {logical_name}"
                ),
                "kind": "python",
                "script": GENERATE_REPORT_SCRIPT,
                "dataset_id": dataset_id,
                "input_artifacts": [logical_name],
                "output_artifacts": output_artifacts,
                "parameters": {
                    "app": "movement",
                    "action": "generate_report",
                    "report_type": report_type,
                    "output_mode": effective_output_mode,
                    "target_artifact": logical_name,
                    "fix_keys": fix_keys,
                    "issue_ids": issue_ids,
                    "individuals": individuals,
                    "report_fixes": report_fixes,
                    "snapshot_windows": snapshot_windows,
                    "screenshot_mode": screenshot_mode,
                    "snapshots": snapshots,
                    "individual_report_artifacts": individual_report_artifacts,
                    "user": user,
                },
            }
            return JSONResponse(create_analysis(study_dir, payload))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/apps/movement/family/{family_name}/study/{study_name}/actions/remove-confirmed-fixes")
    async def post_movement_remove_confirmed_fixes(family_name: str, study_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            study_dir = get_study_dir(data_root, family_name, study_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            fix_keys = _validate_fix_keys(body.get("fix_keys"))
            reason = _validate_required_text(body.get("reason"), label="Reason", max_length=240)
            user = body.get("user")

            payload = {
                "user": user,
                "title": f"Remove {len(fix_keys)} confirmed fix(es) from {logical_name}",
                "kind": "python",
                "script": REMOVE_CONFIRMED_FIXES_SCRIPT,
                "parameters": {
                    "app": "movement",
                    "action": "remove_confirmed_fixes",
                    "target_artifact": logical_name,
                    "fix_keys": fix_keys,
                    "reason": reason,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": [logical_name],
                "output_artifacts": [logical_name],
                "set_as_head": True,
            }
            return JSONResponse(create_step(study_dir, payload))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.get("/api/project/{project_name}/apps/movement/dataset/{dataset_id}/summary")
    async def get_movement_summary(project_name: str, dataset_id: str, logical_name: str):
        try:
            project_dir = get_project_dir(data_root, project_name)
            _, artifact_path = get_dataset_artifact(project_dir, dataset_id, logical_name)
            return JSONResponse(await run_in_threadpool(build_movement_summary, artifact_path))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.post("/api/project/{project_name}/apps/movement/actions/annotate-fixes")
    async def post_annotate_fixes(project_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            project_dir = get_project_dir(data_root, project_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            fix_keys = _validate_fix_keys(body.get("fix_keys"))
            status = _validate_status(body.get("status"))
            issue_type = _validate_required_text(body.get("issue_type"), label="Issue type", max_length=120)
            issue_field = _validate_optional_text(body.get("issue_field"), label="Issue field", max_length=120)
            issue_threshold = _validate_optional_text(body.get("issue_threshold"), label="Issue threshold", max_length=120)
            issue_note = _validate_required_text(body.get("issue_note"), label="Issue note", max_length=1200)
            owner_question = _validate_required_text(body.get("owner_question"), label="Owner question", max_length=600)
            user = body.get("user")
            issue_id = f"issue_{uuid.uuid4().hex[:12]}"

            payload = {
                "user": user,
                "title": f"Mark {len(fix_keys)} fix(es) as {status} in {logical_name}",
                "kind": "python",
                "script": ANNOTATE_FIXES_SCRIPT,
                "parameters": {
                    "app": "movement",
                    "action": "annotate_fixes",
                    "target_artifact": logical_name,
                    "fix_keys": fix_keys,
                    "status": status,
                    "issue_id": issue_id,
                    "issue_type": issue_type,
                    "issue_field": issue_field,
                    "issue_threshold": issue_threshold,
                    "issue_note": issue_note,
                    "owner_question": owner_question,
                    "user": user,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": [logical_name],
                "output_artifacts": [logical_name],
                "set_as_head": True,
            }
            return JSONResponse(create_step(project_dir, payload))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/project/{project_name}/apps/movement/actions/generate-report")
    async def post_generate_report(project_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            project_dir = get_project_dir(data_root, project_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            fix_keys = _validate_fix_keys(body.get("fix_keys"), allow_empty=True)
            issue_ids = _validate_issue_ids(body.get("issue_ids"))
            report_fixes = _validate_report_fixes(body.get("report_fixes"))
            report_type = _validate_report_type(body.get("report_type"))
            individuals = _validate_report_individuals(body.get("individuals"))
            output_mode = _validate_output_mode(body.get("output_mode"))
            snapshot_windows = _validate_snapshot_windows(body.get("snapshot_windows"))
            if report_type == "issue_first" and not fix_keys and not issue_ids and not report_fixes:
                raise ValueError("Select at least one issue or fix")
            if report_type == "individual_profile" and not individuals:
                raise ValueError("Select at least one individual")
            screenshot_mode = _validate_screenshot_mode(body.get("screenshot_mode"))
            snapshots = _validate_snapshots(body.get("snapshots"))
            user = body.get("user")
            individual_report_artifacts = _build_individual_report_artifacts(individuals)
            effective_output_mode = output_mode if len(individuals) > 1 else "combined"
            if report_type == "issue_first":
                output_artifacts = [
                    "movement_outlier_report.md",
                    "movement_outlier_report.html",
                    "movement_outlier_fixes.csv",
                ]
            elif effective_output_mode == "combined":
                output_artifacts = [
                    "movement_individual_reports.md",
                    "movement_individual_reports.html",
                ]
            else:
                output_artifacts = [
                    "movement_individual_report_index.md",
                    "movement_individual_report_index.html",
                ]
                for item in individual_report_artifacts:
                    output_artifacts.extend([item["markdown_name"], item["html_name"]])
            output_artifacts.extend(snapshot["artifact_name"] for snapshot in snapshots)

            payload = {
                "user": user,
                "title": (
                    f"Generate outlier report for {logical_name}"
                    if report_type == "issue_first"
                    else f"Generate individual profile report for {logical_name}"
                ),
                "kind": "python",
                "script": GENERATE_REPORT_SCRIPT,
                "dataset_id": dataset_id,
                "input_artifacts": [logical_name],
                "output_artifacts": output_artifacts,
                "parameters": {
                    "app": "movement",
                    "action": "generate_report",
                    "report_type": report_type,
                    "output_mode": effective_output_mode,
                    "target_artifact": logical_name,
                    "fix_keys": fix_keys,
                    "issue_ids": issue_ids,
                    "individuals": individuals,
                    "report_fixes": report_fixes,
                    "snapshot_windows": snapshot_windows,
                    "screenshot_mode": screenshot_mode,
                    "snapshots": snapshots,
                    "individual_report_artifacts": individual_report_artifacts,
                    "user": user,
                },
            }
            return JSONResponse(create_analysis(project_dir, payload))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/project/{project_name}/apps/movement/actions/remove-confirmed-fixes")
    async def post_remove_confirmed_fixes(project_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            project_dir = get_project_dir(data_root, project_name)
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            logical_name = validate_path_part(body.get("logical_name"), label="artifact")
            fix_keys = _validate_fix_keys(body.get("fix_keys"))
            reason = _validate_required_text(body.get("reason"), label="Reason", max_length=240)
            user = body.get("user")

            payload = {
                "user": user,
                "title": f"Remove {len(fix_keys)} confirmed fix(es) from {logical_name}",
                "kind": "python",
                "script": REMOVE_CONFIRMED_FIXES_SCRIPT,
                "parameters": {
                    "app": "movement",
                    "action": "remove_confirmed_fixes",
                    "target_artifact": logical_name,
                    "fix_keys": fix_keys,
                    "reason": reason,
                },
                "parent_dataset_id": dataset_id,
                "input_artifacts": [logical_name],
                "output_artifacts": [logical_name],
                "set_as_head": True,
            }
            return JSONResponse(create_step(project_dir, payload))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)
