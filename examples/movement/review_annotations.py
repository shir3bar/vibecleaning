import csv
import json
from pathlib import Path

from .summary import (
    _make_fix_key,
    _normalize_review_status,
    _row_is_analytically_excluded,
    _valid_movement_row,
    detect_columns,
    is_valid_coordinate,
    parse_bool,
    parse_time_ms,
    try_float,
)
from .movement_features import haversine_meters


REVIEW_SIDECAR_NAME = "movement_review_annotations.json"
EXPORT_COLUMNS = [
    "visible",
    "manually-marked-outlier",
    "algorithm-marked-outlier",
    "individual-reviewed",
    "individual-review-ok",
    "individual-review-decision",
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
DERIVED_FILTER_FIELDS = {"step_length_m", "speed_mps", "time_delta_s"}


def fix_key_row_number(fix_key: object) -> int:
    value = str(fix_key or "").strip()
    if "#row:" in value:
        raw_number = value.rsplit("#row:", 1)[1]
    elif value.startswith("row:"):
        raw_number = value[4:].split("|", 1)[0]
    else:
        raise ValueError("Invalid movement fix key")
    try:
        row_number = int(raw_number)
    except ValueError as exc:
        raise ValueError("Invalid movement fix key") from exc
    if row_number < 1:
        raise ValueError("Invalid movement fix key")
    return row_number


def normalize_row_ranges(raw_ranges: object) -> list[list[int]]:
    if not isinstance(raw_ranges, list):
        return []
    ranges = []
    for item in raw_ranges:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError("Invalid movement row ranges")
        try:
            start, end = int(item[0]), int(item[1])
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid movement row ranges") from exc
        if start < 1 or end < start:
            raise ValueError("Invalid movement row ranges")
        ranges.append((start, end))
    merged: list[list[int]] = []
    for start, end in sorted(ranges):
        if merged and start <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def compress_fix_keys(fix_keys: list[str] | tuple[str, ...] | set[str]) -> list[list[int]]:
    return normalize_row_ranges([[fix_key_row_number(key), fix_key_row_number(key)] for key in fix_keys])


def row_number_in_ranges(row_number: int, row_ranges: list[list[int]]) -> bool:
    low = 0
    high = len(row_ranges)
    while low < high:
        middle = (low + high) // 2
        start, end = row_ranges[middle]
        if row_number < start:
            high = middle
        elif row_number > end:
            low = middle + 1
        else:
            return True
    return False


def _compress_row_numbers(row_numbers: list[int]) -> list[list[int]]:
    ranges: list[list[int]] = []
    for row_number in sorted(set(row_numbers)):
        if ranges and row_number == ranges[-1][1] + 1:
            ranges[-1][1] = row_number
        else:
            ranges.append([row_number, row_number])
    return ranges


def _filter_value_matches(value: object, filter_spec: dict) -> bool:
    field_kind = str(filter_spec.get("field_kind") or "").strip().lower()
    if field_kind == "numeric":
        numeric = try_float(value)
        threshold = try_float(filter_spec.get("threshold_value"))
        if numeric is None or threshold is None:
            return False
        operator = str(filter_spec.get("operator") or "gt").strip().lower()
        return numeric < threshold if operator == "lt" else numeric > threshold

    selected_levels = {
        str(item).strip()
        for item in filter_spec.get("selected_levels") or []
        if str(item).strip()
    }
    if not selected_levels:
        return False
    if field_kind == "boolean":
        parsed = parse_bool(value)
        label = "True" if parsed is True else "False" if parsed is False else "Missing"
    else:
        label = str(value or "").strip() or "Missing"
    return label in selected_levels


def _derived_filter_value(previous: tuple | None, current: tuple, field_key: str):
    if previous is None or current[1] <= previous[1]:
        return None
    time_delta_s = (current[1] - previous[1]) / 1000.0
    if field_key == "time_delta_s":
        return time_delta_s
    step_length_m = haversine_meters(previous[2], previous[3], current[2], current[3])
    if field_key == "step_length_m":
        return step_length_m
    return step_length_m / time_delta_s if time_delta_s > 0 else None


def resolve_filter_row_ranges(
    path: Path,
    filter_spec: dict,
    *,
    confirmed_fix_keys: set[str] | None = None,
    confirmed_individual_tracks: set[tuple[str, str]] | None = None,
) -> tuple[list[list[int]], int]:
    """Evaluate a persisted issue filter over every valid movement row exactly once.

    Derived step fields use a streaming pass when each track is already in time
    order. A second, lean grouped pass is used only for files with track-order
    regressions so the result retains the app's chronological step semantics.
    """
    field_key = str(filter_spec.get("field_key") or "").strip()
    if not field_key:
        raise ValueError("Filter field is required")
    confirmed_fix_key_set = set(confirmed_fix_keys or set())
    confirmed_individual_track_set = set(confirmed_individual_tracks or set())

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        columns = detect_columns(fieldnames)
        if not columns["individual"] or not columns["time"] or not columns["lon"] or not columns["lat"]:
            raise ValueError("CSV is missing required columns for movement filtering")
        if field_key not in DERIVED_FILTER_FIELDS and field_key not in fieldnames:
            raise ValueError("Filter field is not present in the movement CSV")

        matched_rows: list[int] = []
        if field_key not in DERIVED_FILTER_FIELDS:
            for row_index, raw in enumerate(reader, start=1):
                valid = _valid_movement_row(raw, columns)
                if valid is None:
                    continue
                fix_key = _make_fix_key(
                    row_index,
                    valid["fix_id"],
                    valid["individual"],
                    valid["time_ms"],
                )
                if _row_is_analytically_excluded(
                    raw,
                    fix_key=fix_key,
                    individual=valid["individual"],
                    set_name=valid["set_name"],
                    confirmed_fix_keys=confirmed_fix_key_set,
                    confirmed_individual_tracks=confirmed_individual_track_set,
                ):
                    continue
                if _filter_value_matches(raw.get(field_key), filter_spec):
                    matched_rows.append(row_index)
            return _compress_row_numbers(matched_rows), len(matched_rows)

        previous_by_track: dict[tuple[str, str], tuple] = {}
        file_order_regressed = False
        for row_index, raw in enumerate(reader, start=1):
            valid = _valid_movement_row(raw, columns)
            if valid is None:
                continue
            fix_key = _make_fix_key(
                row_index,
                valid["fix_id"],
                valid["individual"],
                valid["time_ms"],
            )
            if _row_is_analytically_excluded(
                raw,
                fix_key=fix_key,
                individual=valid["individual"],
                set_name=valid["set_name"],
                confirmed_fix_keys=confirmed_fix_key_set,
                confirmed_individual_tracks=confirmed_individual_track_set,
            ):
                continue
            track_key = (valid["individual"], valid["set_name"])
            current = (row_index, valid["time_ms"], valid["lon"], valid["lat"])
            previous = previous_by_track.get(track_key)
            if previous is not None and current[1] < previous[1]:
                file_order_regressed = True
            value = _derived_filter_value(previous, current, field_key)
            if _filter_value_matches(value, filter_spec):
                matched_rows.append(row_index)
            previous_by_track[track_key] = current

    if not file_order_regressed:
        return _compress_row_numbers(matched_rows), len(matched_rows)

    records_by_track: dict[tuple[str, str], list[tuple]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = detect_columns(list(reader.fieldnames or []))
        for row_index, raw in enumerate(reader, start=1):
            valid = _valid_movement_row(raw, columns)
            if valid is None:
                continue
            fix_key = _make_fix_key(
                row_index,
                valid["fix_id"],
                valid["individual"],
                valid["time_ms"],
            )
            if _row_is_analytically_excluded(
                raw,
                fix_key=fix_key,
                individual=valid["individual"],
                set_name=valid["set_name"],
                confirmed_fix_keys=confirmed_fix_key_set,
                confirmed_individual_tracks=confirmed_individual_track_set,
            ):
                continue
            records_by_track.setdefault(
                (valid["individual"], valid["set_name"]),
                [],
            ).append((row_index, valid["time_ms"], valid["lon"], valid["lat"]))

    matched_rows = []
    for records in records_by_track.values():
        previous = None
        for current in sorted(records, key=lambda item: (item[1], item[0])):
            value = _derived_filter_value(previous, current, field_key)
            if _filter_value_matches(value, filter_spec):
                matched_rows.append(current[0])
            previous = current
    return _compress_row_numbers(matched_rows), len(matched_rows)


def row_tokens_for_scope(scope: dict) -> set[str]:
    tokens = set()
    for start, end in normalize_row_ranges(scope.get("row_ranges") or []):
        tokens.update(f"row:{row_number}" for row_number in range(start, end + 1))
    return tokens


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
    annotation_kind = str(raw.get("annotation_kind") or "issue").strip().lower()
    raw_status = str(raw.get("status") or "").strip().lower()
    status = "dismissed" if annotation_kind == "dismissal" or raw_status == "dismissed" else _normalize_review_status(raw_status)
    fix_keys = sorted({str(item).strip() for item in scope.get("fix_keys", []) if str(item).strip()})
    row_ranges = normalize_row_ranges(scope.get("row_ranges") or [])
    if not row_ranges and fix_keys:
        row_ranges = compress_fix_keys(fix_keys)
    try:
        resolved_fix_count = max(
            0,
            int(
                raw.get("resolved_fix_count")
                or sum(end - start + 1 for start, end in row_ranges)
            ),
        )
    except (TypeError, ValueError):
        resolved_fix_count = sum(end - start + 1 for start, end in row_ranges)
    reviewed = raw.get("reviewed") is True or _flag_is_true(raw.get("reviewed"))
    raw_review_ok = raw.get("review_ok")
    review_ok = (
        raw_review_ok
        if isinstance(raw_review_ok, bool)
        else _flag_is_true(raw_review_ok)
        if raw_review_ok not in (None, "")
        else None
    )
    raw_decision = str(raw.get("review_decision") or "").strip().lower()
    if raw_decision not in {"ok", "not_ok", "second_opinion"}:
        raw_decision = "ok" if review_ok is True else "not_ok" if reviewed else ""
    return {
        "annotation_id": str(raw.get("annotation_id") or "").strip(),
        "step_id": str(raw.get("step_id") or "").strip(),
        "parent_annotation_id": str(raw.get("parent_annotation_id") or "").strip(),
        "annotation_kind": annotation_kind,
        "reviewed": reviewed,
        "review_ok": review_ok if reviewed else None,
        "review_decision": raw_decision if reviewed else "",
        "review_id": str(raw.get("review_id") or "").strip(),
        "actor": dict(raw.get("actor") or {}) if isinstance(raw.get("actor"), dict) else {},
        "source_artifact": str(raw.get("source_artifact") or "").strip(),
        "source_dataset_id": str(raw.get("source_dataset_id") or "").strip(),
        "status": status,
        "origin": origin,
        "issue_type": str(raw.get("issue_type") or "").strip(),
        "issue_field": str(raw.get("issue_field") or "").strip(),
        "issue_threshold": str(raw.get("issue_threshold") or "").strip(),
        "comment": str(raw.get("comment") or raw.get("issue_note") or "").strip(),
        "owner_question": str(raw.get("owner_question") or "").strip(),
        "user": str(raw.get("user") or raw.get("review_user") or "").strip(),
        "created_at": str(raw.get("created_at") or raw.get("reviewed_at") or "").strip(),
        "source_analysis_id": str(raw.get("source_analysis_id") or "").strip(),
        "resolved_fix_count": resolved_fix_count,
        "scope": {
            "kind": str(scope.get("kind") or "fix").strip().lower(),
            "row_ranges": row_ranges,
            "filter": dict(scope.get("filter") or {}) if isinstance(scope.get("filter"), dict) else {},
            "burst_id": str(scope.get("burst_id") or "").strip(),
            "individual": str(scope.get("individual") or "").strip(),
            "set_name": str(scope.get("set_name") or "").strip(),
            "start_fix_key": str(scope.get("start_fix_key") or "").strip(),
            "end_fix_key": str(scope.get("end_fix_key") or "").strip(),
            "burst_gap": dict(scope.get("burst_gap") or {}),
        },
    }


def individual_review_decisions(
    annotations: list[dict],
    *,
    source_artifact: str,
) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    for annotation in annotations:
        if annotation.get("annotation_kind") != "individual_review":
            continue
        if not annotation.get("reviewed"):
            continue
        if annotation.get("source_artifact") and annotation["source_artifact"] != source_artifact:
            continue
        scope = annotation.get("scope") or {}
        if scope.get("kind") != "individual":
            continue
        individual = str(scope.get("individual") or "").strip()
        if individual:
            latest[individual] = annotation
    return latest


def source_row_annotation(raw: dict, *, fix_key: str, source_artifact: str) -> dict | None:
    """Return an explicit portable review annotation embedded in a source CSV row."""
    status = _normalize_review_status(raw.get("outlier_status"))
    if not status:
        return None
    manual = _flag_is_true(raw.get("manually-marked-outlier"))
    algorithm = _flag_is_true(raw.get("algorithm-marked-outlier"))
    origin = "algorithm" if algorithm and not manual else "manual"
    return normalize_annotation(
        {
            "annotation_id": f"source:{fix_key}",
            "source_artifact": source_artifact,
            "status": status,
            "origin": origin,
            "issue_type": raw.get("outlier_issue_type"),
            "comment": raw.get("outlier_comments"),
            "scope": {"kind": "fix", "row_ranges": compress_fix_keys([fix_key])},
        }
    )


def annotation_applies(annotation: dict, *, fix_key: str, individual: str, set_name: str) -> bool:
    scope = annotation.get("scope") or {}
    kind = str(scope.get("kind") or "fix")
    if kind == "individual":
        if str(scope.get("individual") or "") != individual:
            return False
        scoped_set = str(scope.get("set_name") or "")
        return not scoped_set or scoped_set == set_name
    row_ranges = scope.get("row_ranges") or []
    if row_ranges:
        return row_number_in_ranges(fix_key_row_number(fix_key), row_ranges)
    return fix_key in set(scope.get("fix_keys") or [])


def _issue_payload(item: dict) -> dict:
    scope = item.get("scope") or {}
    return {
        "status": str(item.get("status") or ""),
        "issue_id": str(item.get("annotation_id") or item.get("issue_id") or ""),
        "issue_type": str(item.get("issue_type") or ""),
        "issue_field": str(item.get("issue_field") or ""),
        "issue_threshold": str(item.get("issue_threshold") or ""),
        "issue_note": str(item.get("comment") or item.get("issue_note") or ""),
        "owner_question": str(item.get("owner_question") or ""),
        "review_user": str(item.get("user") or item.get("review_user") or ""),
        "reviewed_at": str(item.get("created_at") or item.get("reviewed_at") or ""),
        "origin": str(item.get("origin") or "manual"),
        "step_id": str(item.get("step_id") or ""),
        "source_analysis_id": str(item.get("source_analysis_id") or ""),
        "scope_kind": str(scope.get("kind") or item.get("scope_kind") or "fix"),
        "scope_burst_id": str(scope.get("burst_id") or item.get("scope_burst_id") or ""),
        "parent_annotation_id": str(item.get("parent_annotation_id") or ""),
        "annotation_kind": str(item.get("annotation_kind") or "issue"),
    }


def effective_issues_for_fix(
    annotations: list[dict],
    *,
    fix_key: str,
    individual: str,
    set_name: str,
    existing_issues: list[dict] | None = None,
) -> list[dict]:
    """Return one effective record per parent suspicion for a movement fix."""
    matching = [
        item
        for item in annotations
        if item.get("status")
        and annotation_applies(
            item,
            fix_key=fix_key,
            individual=individual,
            set_name=set_name,
        )
    ]
    parents: dict[str, dict] = {}
    resolutions: dict[str, list[dict]] = {}
    for raw_issue in existing_issues or []:
        issue = _issue_payload(raw_issue)
        issue_id = issue["issue_id"]
        parent_id = issue["parent_annotation_id"]
        if issue["status"] == "suspected" and issue_id and not parent_id:
            parents.setdefault(issue_id, issue)
        elif issue["status"] == "confirmed" and not parent_id and issue_id:
            parents.setdefault(issue_id, issue)
    for item in matching:
        issue = _issue_payload(item)
        issue_id = issue["issue_id"]
        parent_id = issue["parent_annotation_id"]
        if parent_id:
            resolutions.setdefault(parent_id, []).append(issue)
        elif issue["status"] in {"suspected", "confirmed"} and issue_id:
            parents.setdefault(issue_id, issue)

    effective = []
    for parent_id, parent in parents.items():
        child_records = resolutions.get(parent_id, [])
        confirmation = next(
            (item for item in reversed(child_records) if item["status"] == "confirmed"),
            None,
        )
        dismissal = next(
            (item for item in reversed(child_records) if item["status"] == "dismissed"),
            None,
        )
        resolution = confirmation or dismissal
        status = (
            "confirmed"
            if confirmation or parent.get("status") == "confirmed"
            else "dismissed"
            if dismissal
            else "suspected"
        )
        record = dict(parent)
        record.update(
            {
                "status": status,
                "parent_issue_id": parent_id,
                "resolution_issue_id": str((resolution or {}).get("issue_id") or ""),
                "resolution_step_id": str((resolution or {}).get("step_id") or ""),
                "resolution_user": str((resolution or {}).get("review_user") or ""),
                "resolution_note": str((resolution or {}).get("issue_note") or ""),
                "resolved_at": str((resolution or {}).get("reviewed_at") or ""),
            }
        )
        effective.append(record)
    return effective


def effective_review_status(effective_issues: list[dict]) -> str:
    if any(item.get("status") == "confirmed" for item in effective_issues):
        return "confirmed"
    if any(item.get("status") == "suspected" for item in effective_issues):
        return "suspected"
    return ""


def apply_review_annotation_counts(
    summary: dict,
    source_path: Path,
    annotations: list[dict],
    *,
    source_artifact: str,
) -> dict:
    """Attach reliable effective review counts even when overview fixes are capped."""
    relevant = [
        item
        for item in annotations
        if item.get("status")
        and (not item.get("source_artifact") or item.get("source_artifact") == source_artifact)
    ]
    counts = {"suspected": 0, "confirmed": 0}
    by_individual: dict[str, dict] = {}
    with source_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = detect_columns(list(reader.fieldnames or []))
        for row_index, raw in enumerate(reader, start=1):
            valid = _valid_movement_row(raw, columns)
            if valid is None:
                continue
            fix_key = _make_fix_key(
                row_index,
                valid["fix_id"],
                valid["individual"],
                valid["time_ms"],
            )
            source_issue = source_row_annotation(
                raw,
                fix_key=fix_key,
                source_artifact=source_artifact,
            )
            effective = effective_issues_for_fix(
                relevant,
                fix_key=fix_key,
                individual=valid["individual"],
                set_name=valid["set_name"],
                existing_issues=[source_issue] if source_issue else [],
            )
            status = effective_review_status(effective)
            if status not in counts:
                continue
            counts[status] += 1
            individual_counts = by_individual.setdefault(
                valid["individual"],
                {
                    "suspected": 0,
                    "confirmed": 0,
                    "issue_types": set(),
                    "origins": set(),
                },
            )
            individual_counts[status] += 1
            for issue in effective:
                if issue.get("status") != "suspected":
                    continue
                if issue.get("issue_type"):
                    individual_counts["issue_types"].add(str(issue["issue_type"]))
                if issue.get("origin"):
                    individual_counts["origins"].add(str(issue["origin"]))

    result = dict(summary)
    result["review_counts"] = counts
    result["unresolved_suspected_count"] = counts["suspected"]
    stats_by_individual = {
        individual: dict(stats)
        for individual, stats in (summary.get("stats") or {}).items()
    }
    for individual in sorted(set(stats_by_individual) | set(by_individual)):
        stats = stats_by_individual.setdefault(individual, {})
        individual_counts = by_individual.get(individual, {})
        stats["suspected_count"] = int(individual_counts.get("suspected", 0))
        stats["unresolved_suspected_count"] = stats["suspected_count"]
        stats["confirmed_count"] = int(individual_counts.get("confirmed", 0))
        stats["unresolved_issue_types"] = sorted(individual_counts.get("issue_types", set()))
        stats["unresolved_issue_origins"] = sorted(individual_counts.get("origins", set()))
    result["stats"] = stats_by_individual
    return result


def unresolved_suspicion_pairs(annotations: list[dict]) -> set[tuple[str, str]]:
    """Return unresolved (parent id, row token) pairs for guarded resolutions."""
    parents: set[tuple[str, str]] = set()
    resolved: set[tuple[str, str]] = set()
    for item in annotations:
        parent_id = str(item.get("parent_annotation_id") or "")
        tokens = row_tokens_for_scope(item.get("scope") or {})
        if item.get("status") == "suspected" and not parent_id:
            issue_id = str(item.get("annotation_id") or "")
            parents.update((issue_id, token) for token in tokens if issue_id)
        elif parent_id and item.get("status") in {"confirmed", "dismissed"}:
            resolved.update((parent_id, token) for token in tokens)
    return parents - resolved


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
        fix_keys.update(row_tokens_for_scope(scope))
        if str(scope.get("kind") or "") == "individual":
            individual = str(scope.get("individual") or "").strip()
            if individual:
                individual_tracks.add((individual, str(scope.get("set_name") or "").strip()))
    return fix_keys, individual_tracks


def apply_review_annotations(summary: dict, annotations: list[dict], *, source_artifact: str) -> dict:
    relevant_issues = [
        item
        for item in annotations
        if item["status"] and (not item["source_artifact"] or item["source_artifact"] == source_artifact)
    ]
    review_decisions = individual_review_decisions(
        annotations,
        source_artifact=source_artifact,
    )
    if not relevant_issues and not review_decisions:
        return summary
    result = dict(summary)
    fixes = list(summary.get("fixes") or [])
    for fix_index, fix in enumerate(fixes):
        fix_key = str(fix.get("fix_key") or "")
        individual = str(fix.get("individual") or "")
        set_name = str(fix.get("set") or "train")
        matches = [
            item
            for item in relevant_issues
            if annotation_applies(item, fix_key=fix_key, individual=individual, set_name=set_name)
        ]
        existing_review = dict(fix.get("review") or {})
        existing_issues = list(existing_review.get("issues") or [])
        if not matches and not existing_issues:
            continue
        fix = dict(fix)
        review = existing_review
        issues = existing_issues
        for item in matches:
            issues.append(_issue_payload(item))
        effective_issues = effective_issues_for_fix(
            relevant_issues,
            fix_key=fix_key,
            individual=individual,
            set_name=set_name,
            existing_issues=existing_issues,
        )
        status = effective_review_status(effective_issues)
        visible_issues = [item for item in effective_issues if item.get("status") != "dismissed"]
        latest = (visible_issues or effective_issues or issues)[-1]
        review.update(
            {
                "status": status,
                "issue_id": latest.get("parent_issue_id") or latest.get("issue_id") or "",
                "issue_type": latest.get("issue_type") or "",
                "issue_field": latest.get("issue_field") or "",
                "issue_threshold": latest.get("issue_threshold") or "",
                "issue_note": latest.get("issue_note") or "",
                "owner_question": latest.get("owner_question") or "",
                "review_user": latest.get("review_user") or "",
                "reviewed_at": latest.get("reviewed_at") or "",
                "issues": issues,
                "effective_issues": effective_issues,
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
    for item in relevant_issues:
        scope = item.get("scope") or {}
        if scope.get("kind") != "segment" or item["annotation_id"] in existing_segment_ids:
            continue
        segment_fixes = [
            fix
            for fix in fixes
            if annotation_applies(
                item,
                fix_key=str(fix.get("fix_key") or ""),
                individual=str(fix.get("individual") or ""),
                set_name=str(fix.get("set") or "train"),
            )
        ]
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
                "status": str((fix_by_key.get(str(segment_fixes[0].get("fix_key") or ""), {}).get("review") or {}).get("status") or item["status"]),
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
    result["review_annotations"] = [
        item
        for item in annotations
        if not item.get("source_artifact") or item["source_artifact"] == source_artifact
    ]
    result["individual_reviews"] = {
        individual: {
            "reviewed": True,
            "review_ok": bool(item.get("review_ok")),
            "review_decision": str(item.get("review_decision") or ""),
            "review_user": item.get("user") or "",
            "reviewed_at": item.get("created_at") or "",
            "review_comment": item.get("comment") or "",
            "step_id": item.get("step_id") or "",
        }
        for individual, item in review_decisions.items()
    }
    stats_by_individual = {
        individual: dict(stats)
        for individual, stats in (result.get("stats") or {}).items()
    }
    for individual, stats in stats_by_individual.items():
        decision = review_decisions.get(individual)
        stats.update(
            {
                "reviewed": bool(decision),
                "review_ok": bool(decision.get("review_ok")) if decision else False,
                "review_decision": str(decision.get("review_decision") or "") if decision else "",
                "review_user": str(decision.get("user") or "") if decision else "",
                "reviewed_at": str(decision.get("created_at") or "") if decision else "",
                "review_comment": str(decision.get("comment") or "") if decision else "",
            }
        )
    result["stats"] = stats_by_individual
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
        review = dict(record.get("review") or {})
        existing_issues = list(review.get("issues") or [])
        if not matches and not existing_issues:
            continue
        issues = existing_issues
        for item in matches:
            issues.append(_issue_payload(item))
        fix_key = str(record.get("fix_key") or "")
        effective_issues = effective_issues_for_fix(
            relevant,
            fix_key=fix_key,
            individual=str(record.get("individual") or ""),
            set_name=str(record.get("set_name") or "train"),
            existing_issues=existing_issues,
        )
        status = effective_review_status(effective_issues)
        visible_issues = [item for item in effective_issues if item.get("status") != "dismissed"]
        latest = (visible_issues or effective_issues or issues)[-1]
        review.update(
            {
                "status": status,
                "issue_id": latest.get("parent_issue_id") or latest.get("issue_id") or "",
                "issue_type": latest.get("issue_type") or "",
                "issue_field": latest.get("issue_field") or "",
                "issue_threshold": latest.get("issue_threshold") or "",
                "issue_note": latest.get("issue_note") or "",
                "owner_question": latest.get("owner_question") or "",
                "review_user": latest.get("review_user") or "",
                "reviewed_at": latest.get("reviewed_at") or "",
                "issues": issues,
                "effective_issues": effective_issues,
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
    allowed_individual_review_annotation_ids: set[str] | None = None,
) -> dict:
    annotations = load_review_annotations(sidecar_path)
    if allowed_individual_review_annotation_ids is not None:
        annotations = [
            item
            for item in annotations
            if item.get("annotation_kind") != "individual_review"
            or item.get("annotation_id") in allowed_individual_review_annotation_ids
        ]
    reviews_by_individual = individual_review_decisions(
        annotations,
        source_artifact=source_artifact,
    )
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
            if not str(name).startswith("vc_")
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
            source_annotation = None
            matching_annotations = []
            if valid:
                source_annotation = source_row_annotation(
                    raw,
                    fix_key=fix_key,
                    source_artifact=source_artifact,
                )
                matching_annotations.extend(
                    item
                    for item in annotations
                    if (not item["source_artifact"] or item["source_artifact"] == source_artifact)
                    and item["status"]
                    and annotation_applies(item, fix_key=fix_key, individual=individual, set_name=set_name)
                )
            effective_issues = effective_issues_for_fix(
                matching_annotations,
                fix_key=fix_key,
                individual=individual,
                set_name=set_name,
                existing_issues=[source_annotation] if source_annotation else [],
            ) if valid else []
            active = [item for item in effective_issues if item.get("status") != "dismissed"]
            source_dismissed = bool(source_annotation) and not any(
                item.get("parent_issue_id") == source_annotation.get("annotation_id")
                and item.get("status") != "dismissed"
                for item in effective_issues
            )
            source_manual = _flag_is_true(raw.get("manually-marked-outlier")) and not source_dismissed
            source_algorithm = _flag_is_true(raw.get("algorithm-marked-outlier")) and not source_dismissed
            manual = source_manual or any(item["origin"] == "manual" for item in active)
            algorithm = source_algorithm or any(item["origin"] in {"threshold", "algorithm"} for item in active)
            confirmed = any(item["status"] == "confirmed" for item in active)
            flagged = manual or algorithm or bool(active)
            suspected = any(item["status"] == "suspected" for item in active)
            status = "confirmed" if confirmed else "suspected" if suspected else ""
            issue_types = [
                item.strip()
                for item in str(raw.get("outlier_issue_type") or "").split(";")
                if item.strip() and not source_dismissed
            ]
            for item in active:
                issue_type = str(item.get("issue_type") or "").strip()
                if issue_type and issue_type not in issue_types:
                    issue_types.append(issue_type)
            comments = []
            existing_comment = str(raw.get("outlier_comments") or "").strip() if not source_dismissed else ""
            for comment in existing_comment.split(";"):
                comment = comment.strip()
                if comment and comment not in comments:
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
                annotation_id = str(item.get("annotation_id") or item.get("issue_id") or "").strip()
                step_id = str(item.get("step_id") or step_id_by_annotation.get(annotation_id) or "").strip()
                if not step_id and annotation_id.startswith("step_"):
                    step_id = annotation_id
                if step_id and step_id not in flag_step_ids:
                    flag_step_ids.append(step_id)
                resolution_step_id = str(item.get("resolution_step_id") or "").strip()
                if resolution_step_id and resolution_step_id not in flag_step_ids:
                    flag_step_ids.append(resolution_step_id)
            output_row = {name: raw.get(name, "") for name in output_fields}
            output_row["visible"] = "true" if _original_visible(raw.get("visible")) and not confirmed else "false"
            output_row["manually-marked-outlier"] = "true" if manual else "false"
            output_row["algorithm-marked-outlier"] = "true" if algorithm else "false"
            individual_review = reviews_by_individual.get(individual)
            output_row["individual-reviewed"] = "true" if individual_review else "false"
            output_row["individual-review-ok"] = (
                "true"
                if individual_review and individual_review.get("review_ok")
                else "false"
            )
            output_row["individual-review-decision"] = (
                str(individual_review.get("review_decision") or "")
                if individual_review
                else ""
            )
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
