import csv
import json
from pathlib import Path

from .summary import (
    ALL_REVIEW_COLUMNS,
    _make_fix_key,
    _normalize_review_status,
    _review_issues,
    detect_columns,
    is_valid_coordinate,
    parse_time_ms,
    try_float,
)


REVIEW_SIDECAR_NAME = "movement_review_annotations.json"
EXPORT_COLUMNS = [
    "visible",
    "manually-marked-outlier",
    "algorithm-marked-outlier",
    "outlier_status",
    "outlier_issue_type",
    "outlier_comments",
    "outlier_flag_step_ids",
]
DEPRECATED_EXPORT_COLUMNS = {
    "manually_marked_outliers",
    "algorithm_marked_outliers",
    "outlier_annotation_ids",
}
VALID_ORIGINS = {"manual", "threshold", "algorithm"}


def load_review_annotations(path: Path | None) -> list[dict]:
    if path is None or not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Movement review sidecar is invalid") from exc
    raw_items = payload.get("annotations", []) if isinstance(payload, dict) else payload
    if not isinstance(raw_items, list):
        raise ValueError("Movement review sidecar is invalid")
    return [normalize_annotation(item) for item in raw_items if isinstance(item, dict)]


def normalize_annotation(raw: dict) -> dict:
    scope = dict(raw.get("scope") or {})
    origin = str(raw.get("origin") or "manual").strip().lower()
    if origin not in VALID_ORIGINS:
        origin = "manual"
    status = _normalize_review_status(raw.get("status"))
    fix_keys = sorted({str(item).strip() for item in scope.get("fix_keys", []) if str(item).strip()})
    try:
        resolved_fix_count = max(0, int(raw.get("resolved_fix_count") or len(fix_keys)))
    except (TypeError, ValueError):
        resolved_fix_count = len(fix_keys)
    return {
        "annotation_id": str(raw.get("annotation_id") or "").strip(),
        "step_id": str(raw.get("step_id") or "").strip(),
        "parent_annotation_id": str(raw.get("parent_annotation_id") or "").strip(),
        "annotation_kind": str(raw.get("annotation_kind") or "issue").strip().lower(),
        "source_artifact": str(raw.get("source_artifact") or "").strip(),
        "source_dataset_id": str(raw.get("source_dataset_id") or "").strip(),
        "status": status,
        "origin": origin,
        "issue_type": str(raw.get("issue_type") or "").strip(),
        "comment": str(raw.get("comment") or raw.get("issue_note") or "").strip(),
        "owner_question": str(raw.get("owner_question") or "").strip(),
        "user": str(raw.get("user") or raw.get("review_user") or "").strip(),
        "created_at": str(raw.get("created_at") or raw.get("reviewed_at") or "").strip(),
        "source_analysis_id": str(raw.get("source_analysis_id") or "").strip(),
        "resolved_fix_count": resolved_fix_count,
        "scope": {
            "kind": str(scope.get("kind") or "fix").strip().lower(),
            "fix_keys": fix_keys,
            "burst_id": str(scope.get("burst_id") or "").strip(),
            "individual": str(scope.get("individual") or "").strip(),
            "set_name": str(scope.get("set_name") or "").strip(),
            "start_fix_key": str(scope.get("start_fix_key") or "").strip(),
            "end_fix_key": str(scope.get("end_fix_key") or "").strip(),
            "burst_gap": dict(scope.get("burst_gap") or {}),
        },
    }


def _legacy_annotations(raw: dict, *, fix_key: str, source_artifact: str) -> list[dict]:
    annotations = []
    for index, issue in enumerate(_review_issues(raw), start=1):
        status = _normalize_review_status(issue.get("status"))
        if not status:
            continue
        issue_field = str(issue.get("issue_field") or "").strip()
        issue_threshold = str(issue.get("issue_threshold") or "").strip()
        annotations.append(
            normalize_annotation(
                {
                    "annotation_id": str(issue.get("issue_id") or f"legacy:{fix_key}:{index}"),
                    "source_artifact": source_artifact,
                    "status": status,
                    "origin": "threshold" if issue_field or issue_threshold else "manual",
                    "issue_type": issue.get("issue_type"),
                    "comment": issue.get("issue_note"),
                    "owner_question": issue.get("owner_question"),
                    "user": issue.get("review_user"),
                    "created_at": issue.get("reviewed_at"),
                    "scope": {"kind": "fix", "fix_keys": [fix_key]},
                }
            )
        )
    return annotations


def annotation_applies(annotation: dict, *, fix_key: str, individual: str, set_name: str) -> bool:
    scope = annotation.get("scope") or {}
    kind = str(scope.get("kind") or "fix")
    if kind == "individual":
        if str(scope.get("individual") or "") != individual:
            return False
        scoped_set = str(scope.get("set_name") or "")
        return not scoped_set or scoped_set == set_name
    return fix_key in set(scope.get("fix_keys") or [])


def confirmed_exclusion_scopes(
    annotations: list[dict],
    *,
    source_artifact: str,
) -> tuple[set[str], set[tuple[str, str]]]:
    fix_keys: set[str] = set()
    individual_tracks: set[tuple[str, str]] = set()
    for annotation in annotations:
        if annotation.get("status") != "confirmed":
            continue
        if annotation.get("source_artifact") and annotation["source_artifact"] != source_artifact:
            continue
        scope = annotation.get("scope") or {}
        fix_keys.update(str(item) for item in scope.get("fix_keys") or [] if str(item))
        if str(scope.get("kind") or "") == "individual":
            individual = str(scope.get("individual") or "").strip()
            if individual:
                individual_tracks.add((individual, str(scope.get("set_name") or "").strip()))
    return fix_keys, individual_tracks


def apply_review_annotations(summary: dict, annotations: list[dict], *, source_artifact: str) -> dict:
    relevant = [
        item
        for item in annotations
        if item["status"] and (not item["source_artifact"] or item["source_artifact"] == source_artifact)
    ]
    if not relevant:
        return summary
    result = dict(summary)
    fixes = list(summary.get("fixes") or [])
    for fix_index, fix in enumerate(fixes):
        fix_key = str(fix.get("fix_key") or "")
        individual = str(fix.get("individual") or "")
        set_name = str(fix.get("set") or "train")
        matches = [
            item
            for item in relevant
            if annotation_applies(item, fix_key=fix_key, individual=individual, set_name=set_name)
        ]
        if not matches:
            continue
        fix = dict(fix)
        review = dict(fix.get("review") or {})
        issues = list(review.get("issues") or [])
        for item in matches:
            issues.append(
                {
                    "status": item["status"],
                    "issue_id": item["annotation_id"],
                    "issue_type": item["issue_type"],
                    "issue_note": item["comment"],
                    "owner_question": item["owner_question"],
                    "review_user": item["user"],
                    "reviewed_at": item["created_at"],
                    "origin": item["origin"],
                    "step_id": item["step_id"],
                    "source_analysis_id": item["source_analysis_id"],
                    "scope_kind": item["scope"].get("kind"),
                    "parent_annotation_id": item["parent_annotation_id"],
                    "annotation_kind": item["annotation_kind"],
                }
            )
        latest = matches[-1]
        review.update(
            {
                "status": "confirmed" if any(item.get("status") == "confirmed" for item in issues) else "suspected",
                "issue_id": latest["annotation_id"],
                "issue_type": latest["issue_type"],
                "issue_note": latest["comment"],
                "owner_question": latest["owner_question"],
                "review_user": latest["user"],
                "reviewed_at": latest["created_at"],
                "issues": issues,
            }
        )
        fix["review"] = review
        fixes[fix_index] = fix
    result["fixes"] = fixes
    fix_by_key = {
        str(fix.get("fix_key") or ""): fix
        for fix in fixes
        if str(fix.get("fix_key") or "")
    }
    segments = list(result.get("segments") or [])
    existing_segment_ids = {str(item.get("segment_id") or "") for item in segments}
    for item in relevant:
        scope = item.get("scope") or {}
        if scope.get("kind") != "segment" or item["annotation_id"] in existing_segment_ids:
            continue
        segment_fixes = [fix_by_key[key] for key in scope.get("fix_keys") or [] if key in fix_by_key]
        segment_fixes.sort(key=lambda fix: (int(fix.get("time_ms") or 0), str(fix.get("fix_key") or "")))
        if not segment_fixes:
            continue
        segments.append(
            {
                "segment_id": item["annotation_id"],
                "individual": str(segment_fixes[0].get("individual") or ""),
                "set_name": str(segment_fixes[0].get("set") or "train"),
                "start_fix_key": str(scope.get("start_fix_key") or segment_fixes[0].get("fix_key") or ""),
                "end_fix_key": str(scope.get("end_fix_key") or segment_fixes[-1].get("fix_key") or ""),
                "start_time_ms": int(segment_fixes[0].get("time_ms") or 0),
                "end_time_ms": int(segment_fixes[-1].get("time_ms") or 0),
                "fix_count": len(segment_fixes),
                "status": item["status"],
                "issue_type": item["issue_type"],
                "issue_note": item["comment"],
                "owner_question": item["owner_question"],
                "review_user": item["user"],
                "reviewed_at": item["created_at"],
                "fix_keys": [str(fix.get("fix_key") or "") for fix in segment_fixes],
                "path": [
                    [float(fix.get("lon") or 0), float(fix.get("lat") or 0)]
                    for fix in segment_fixes
                ],
            }
        )
    result["segments"] = segments
    result["review_annotations"] = relevant
    if not result.get("overview_truncated") and not result.get("truncated"):
        counts = {"suspected": 0, "confirmed": 0}
        counts_by_individual: dict[str, dict[str, int]] = {}
        for fix in fixes:
            status = str((fix.get("review") or {}).get("status") or "")
            if status not in counts:
                continue
            counts[status] += 1
            individual_counts = counts_by_individual.setdefault(
                str(fix.get("individual") or ""),
                {"suspected": 0, "confirmed": 0},
            )
            individual_counts[status] += 1
        result["review_counts"] = counts
        stats_by_individual = {
            individual: dict(stats)
            for individual, stats in (result.get("stats") or {}).items()
        }
        for individual, stats in stats_by_individual.items():
            individual_counts = counts_by_individual.get(individual, {})
            stats["suspected_count"] = int(individual_counts.get("suspected", 0))
            stats["confirmed_count"] = int(individual_counts.get("confirmed", 0))
        result["stats"] = stats_by_individual
    return result


def apply_annotations_to_report_records(
    records: list[dict],
    annotations: list[dict],
    *,
    source_artifact: str,
) -> list[dict]:
    relevant = [
        item
        for item in annotations
        if item["status"] and (not item["source_artifact"] or item["source_artifact"] == source_artifact)
    ]
    for record in records:
        matches = [
            item
            for item in relevant
            if annotation_applies(
                item,
                fix_key=str(record.get("fix_key") or ""),
                individual=str(record.get("individual") or ""),
                set_name=str(record.get("set_name") or "train"),
            )
        ]
        if not matches:
            continue
        review = dict(record.get("review") or {})
        issues = list(review.get("issues") or [])
        for item in matches:
            issues.append(
                {
                    "status": item["status"],
                    "issue_id": item["annotation_id"],
                    "issue_type": item["issue_type"],
                    "issue_note": item["comment"],
                    "owner_question": item["owner_question"],
                    "review_user": item["user"],
                    "reviewed_at": item["created_at"],
                    "origin": item["origin"],
                    "step_id": item["step_id"],
                    "parent_annotation_id": item["parent_annotation_id"],
                    "annotation_kind": item["annotation_kind"],
                }
            )
        latest = matches[-1]
        review.update(
            {
                "vc_outlier_status": "confirmed"
                if any(item.get("status") == "confirmed" for item in issues)
                else "suspected",
                "vc_issue_id": latest["annotation_id"],
                "vc_issue_type": latest["issue_type"],
                "vc_issue_note": latest["comment"],
                "vc_owner_question": latest["owner_question"],
                "vc_review_user": latest["user"],
                "vc_reviewed_at": latest["created_at"],
                "issues": issues,
            }
        )
        record["review"] = review
    return records


def _original_visible(raw_value: object) -> bool:
    normalized = str(raw_value or "").strip().lower()
    return normalized not in {"false", "f", "no", "n", "0"}


def _flag_is_true(raw_value: object) -> bool:
    return str(raw_value or "").strip().lower() in {"true", "t", "yes", "y", "1"}


def export_reviewed_csv(
    source_path: Path,
    output_path: Path,
    *,
    source_artifact: str,
    sidecar_path: Path | None = None,
    annotation_step_ids: dict[str, str] | None = None,
) -> dict:
    annotations = load_review_annotations(sidecar_path)
    step_id_by_annotation = dict(annotation_step_ids or {})
    with source_path.open("r", newline="", encoding="utf-8") as input_handle:
        reader = csv.DictReader(input_handle)
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            raise ValueError("CSV did not contain a header row")
        columns = detect_columns(fieldnames)
        if not columns["individual"] or not columns["time"] or not columns["lon"] or not columns["lat"]:
            raise ValueError("CSV is missing required columns for movement visualization")
        output_fields = [
            name
            for name in fieldnames
            if name not in ALL_REVIEW_COLUMNS
            and not str(name).startswith("vc_")
            and name not in EXPORT_COLUMNS
            and name not in DEPRECATED_EXPORT_COLUMNS
        ] + EXPORT_COLUMNS
        rows = list(reader)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    exported_rows = 0
    flagged_rows = 0
    manual_rows = 0
    algorithm_rows = 0
    with output_path.open("w", newline="", encoding="utf-8") as output_handle:
        writer = csv.DictWriter(output_handle, fieldnames=output_fields)
        writer.writeheader()
        for row_index, raw in enumerate(rows, start=1):
            individual = str(raw.get(columns["individual"], "")).strip()
            time_ms = parse_time_ms(raw.get(columns["time"]))
            lon = try_float(raw.get(columns["lon"]))
            lat = try_float(raw.get(columns["lat"]))
            fix_id = str(raw.get(columns["fix_id"], "")).strip() if columns["fix_id"] else ""
            set_name = str(raw.get(columns["set"], "")).strip().lower() if columns["set"] else "train"
            if set_name != "test":
                set_name = "train"
            valid = bool(individual and time_ms is not None and is_valid_coordinate(lon, lat))
            fix_key = _make_fix_key(row_index, fix_id, individual, time_ms) if valid else ""
            active = []
            if valid:
                active.extend(_legacy_annotations(raw, fix_key=fix_key, source_artifact=source_artifact))
                active.extend(
                    item
                    for item in annotations
                    if (not item["source_artifact"] or item["source_artifact"] == source_artifact)
                    and item["status"]
                    and annotation_applies(item, fix_key=fix_key, individual=individual, set_name=set_name)
                )
            source_manual = _flag_is_true(raw.get("manually-marked-outlier"))
            source_algorithm = _flag_is_true(raw.get("algorithm-marked-outlier"))
            deprecated_manual = _flag_is_true(raw.get("manually_marked_outliers"))
            deprecated_algorithm = _flag_is_true(raw.get("algorithm_marked_outliers"))
            source_manual = source_manual or deprecated_manual
            source_algorithm = source_algorithm or deprecated_algorithm
            manual = source_manual or any(item["origin"] == "manual" for item in active)
            algorithm = source_algorithm or any(item["origin"] in {"threshold", "algorithm"} for item in active)
            source_status = _normalize_review_status(raw.get("outlier_status"))
            confirmed = source_status == "confirmed" or any(item["status"] == "confirmed" for item in active)
            flagged = manual or algorithm or bool(active)
            suspected = source_status == "suspected" or any(item["status"] == "suspected" for item in active)
            status = "confirmed" if confirmed else "suspected" if suspected else ""
            issue_types = [
                item.strip()
                for item in str(raw.get("outlier_issue_type") or "").split(";")
                if item.strip()
            ]
            for item in active:
                issue_type = str(item.get("issue_type") or "").strip()
                if issue_type and issue_type not in issue_types:
                    issue_types.append(issue_type)
            comments = []
            existing_comment = str(raw.get("outlier_comments") or "").strip()
            for comment in existing_comment.split(";"):
                comment = comment.strip()
                if comment.startswith("Already flagged in source:") and comment not in comments:
                    comments.append(comment)
            if source_manual:
                marker = "Already flagged in source: manually-marked-outlier=true"
                if marker not in existing_comment:
                    comments.append(marker)
            if source_algorithm:
                marker = "Already flagged in source: algorithm-marked-outlier=true"
                if marker not in existing_comment:
                    comments.append(marker)
            flag_step_ids = [
                item.strip()
                for item in str(raw.get("outlier_flag_step_ids") or "").split(";")
                if item.strip()
            ]
            for item in active:
                annotation_id = str(item.get("annotation_id") or "").strip()
                step_id = str(item.get("step_id") or step_id_by_annotation.get(annotation_id) or "").strip()
                if not step_id and annotation_id.startswith("step_"):
                    step_id = annotation_id
                if step_id and step_id not in flag_step_ids:
                    flag_step_ids.append(step_id)
            output_row = {name: raw.get(name, "") for name in output_fields}
            output_row["visible"] = "true" if _original_visible(raw.get("visible")) and not confirmed else "false"
            output_row["manually-marked-outlier"] = "true" if manual else "false"
            output_row["algorithm-marked-outlier"] = "true" if algorithm else "false"
            output_row["outlier_status"] = status
            output_row["outlier_issue_type"] = "; ".join(issue_types)
            output_row["outlier_comments"] = "; ".join(comments)
            output_row["outlier_flag_step_ids"] = ";".join(flag_step_ids)
            writer.writerow(output_row)
            exported_rows += 1
            flagged_rows += int(flagged)
            manual_rows += int(manual)
            algorithm_rows += int(algorithm)
    return {
        "run_status": "completed",
        "source_artifact": source_artifact,
        "output_artifact": output_path.name,
        "exported_row_count": exported_rows,
        "flagged_row_count": flagged_rows,
        "manually_marked_row_count": manual_rows,
        "algorithm_marked_row_count": algorithm_rows,
        "sidecar_annotation_count": len(annotations),
    }
