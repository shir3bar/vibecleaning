"""Stationarity input checks and explicit, history-preserving reruns."""

from functools import lru_cache
from hashlib import sha256
import json
from pathlib import Path

from app.state import get_dataset_artifact, ProjectStateError
from .review_annotations import load_review_annotations, row_number_in_ranges
from .stationarity import (
    ALGORITHM, compressed_rows, evaluate_stationarity, stationarity_scope_matcher,
    validate_stationarity_filter,
)


@lru_cache(maxsize=4)
def _records(path, modified, size, rds, logical_name, individual, sets):
    spec = {"individuals": [individual], "set_names": list(sets)}
    if rds:
        from .rds_index import rds_stationarity_records
        return rds_stationarity_records(Path(path), spec)
    from .review_annotations import csv_stationarity_records
    return csv_stationarity_records(Path(path), spec, source_artifact=logical_name)


def input_records(path, *, rds, logical_name, individual, spec):
    stat = path.stat()
    return _records(str(path), stat.st_mtime_ns, stat.st_size, rds, logical_name,
                    individual, tuple(sorted(spec.get("set_names") or [])))


def _parents(annotations, logical_name, rds):
    return [item for item in annotations if not item.get("parent_annotation_id")
            and ((item.get("scope") or {}).get("filter") or {}).get("kind") == "stationarity"
            and (rds or item.get("source_artifact") in (None, "", logical_name))]


def _root(item):
    return ((item.get("scope") or {}).get("stationarity_rerun") or {}).get("root_annotation_id") or item["annotation_id"]


def _scoped_inputs(item, individual):
    return [window for window in (item.get("scope") or {}).get("stationarity_inputs", [])
            if window.get("individual") == individual]


def _window_records(records, windows):
    if not windows:
        return records
    return [record for record in records if any(
        record["source_artifact"] == window["source_artifact"]
        and record["set_name"] == window["set_name"]
        and window["start_ms"] <= record["time_ms"] <= window["end_ms"]
        and row_number_in_ranges(record["row_index"], window["input_rows"])
        for window in windows)]


def run_contexts(study_dir, annotations, *, path, rds, logical_name, individual, source_signature):
    parents = _parents(annotations, logical_name, rds)
    roots = {}
    for item in parents:
        scope = item.get("scope") or {}
        selected = (scope.get("filter") or {}).get("individuals") or []
        if selected and individual not in selected:
            continue
        if scope.get("stationarity_inputs") and not _scoped_inputs(item, individual):
            continue
        roots[_root(item)] = item
    result = []
    for root_id, latest in roots.items():
        family = [item for item in parents if _root(item) == root_id]
        family_ids = {item["annotation_id"] for item in family}
        # A run's own stationarity decisions must not invalidate its inputs or
        # make it erase its candidates on rerun. GPS and other upstream edits do.
        upstream = [item for item in annotations if item.get("annotation_id") not in family_ids
                    and item.get("parent_annotation_id") not in family_ids]
        original_spec = (latest.get("scope") or {})["filter"]
        spec = {**original_spec, **validate_stationarity_filter({**original_spec, "algorithm": ALGORITHM}),
                "individuals": [individual]}
        records = input_records(path, rds=rds, logical_name=logical_name, individual=individual, spec=spec)
        windows = _scoped_inputs(latest, individual)
        records = _window_records(records, windows)
        if not records:
            continue
        if not windows:
            # Legacy flags have a parent version but no explicit input snapshot.
            try:
                _, sidecar = get_dataset_artifact(study_dir, latest["source_dataset_id"],
                                                 "movement_review_annotations.json")
                original_annotations = load_review_annotations(sidecar)
            except ProjectStateError:
                original_annotations = []
            _, windows = evaluate_stationarity(records, original_spec, original_annotations, calculate=False)
        _, current_inputs = evaluate_stationarity(records, spec, upstream, calculate=False)
        changed = windows != current_inputs
        source_changed = bool(latest.get("source_id") and latest["source_id"] != source_signature)
        rule_changed = original_spec.get("algorithm") != ALGORITHM and any(
            window["skipped_rows"] for window in current_inputs)
        result.append({
            "run_id": root_id, "annotation_id": latest["annotation_id"], "individual": individual,
            "issue_type": latest.get("issue_type") or "Stationarity", "stale": changed or source_changed or rule_changed,
            "filter": spec, "previous_algorithm": original_spec.get("algorithm", "anchor-radius-v1"),
            "input_windows": windows, "current_inputs": current_inputs,
            "_records": records, "_annotations": upstream, "_family": family, "_latest": latest,
        })
    return result


def public_run(context):
    return {key: context[key] for key in ("run_id", "annotation_id", "individual", "issue_type", "stale",
                                         "filter", "previous_algorithm")}


def _scope(keys, *, rds, logical_name):
    if rds:
        from .rds_index import source_rows_from_fix_keys
        return {"kind": "fix", "source_rows": source_rows_from_fix_keys(keys)}
    return {"kind": "fix", "row_ranges": compressed_rows(int(key.split(":")[-1]) for key in keys)}


def preview_rerun(context, annotations, *, rds, logical_name, dataset_id, source_signature):
    matches, inputs = evaluate_stationarity(context["_records"], context["filter"], context["_annotations"])
    candidates = set(matches)
    matches_scope = stationarity_scope_matcher(context["_records"])

    def keys_for(item):
        return {context["_records"][i]["fix_key"] for i in matches_scope(item)}

    family_ids = {item["annotation_id"] for item in context["_family"]}
    resolutions = [item for item in annotations if item.get("parent_annotation_id") in family_ids
                   and item.get("status") in {"confirmed", "dismissed"}]
    human_resolutions = [item for item in resolutions
                         if not (item["scope"].get("stationarity_rerun") or {}).get("superseded")]
    resolved = set().union(*(keys_for(item) for item in human_resolutions))
    pending_by_parent = {}
    for parent in context["_family"]:
        keys = keys_for(parent)
        for child in resolutions:
            if child["parent_annotation_id"] == parent["annotation_id"]:
                keys -= keys_for(child)
        pending_by_parent[parent["annotation_id"]] = keys
    pending = set().union(*pending_by_parent.values())
    added = candidates - pending - resolved
    removed = pending - candidates
    records = []
    for parent in context["_family"]:
        obsolete = pending_by_parent[parent["annotation_id"]] & removed
        if obsolete:
            records.append({
                "annotation_kind": "dismissal", "status": "dismissed", "origin": "algorithm",
                "parent_annotation_id": parent["annotation_id"], "issue_type": context["issue_type"],
                "source_artifact": "" if rds else logical_name,
                "scope": {**_scope(obsolete, rds=rds, logical_name=logical_name),
                          "stationarity_rerun": {"superseded": True}},
                "comment": "Superseded by an explicit stationarity rerun; no longer selected by the filter.",
                "resolved_fix_count": len(obsolete),
            })
    new_scope = {**_scope(added, rds=rds, logical_name=logical_name), "kind": "filter",
                 "filter": context["filter"], "stationarity_inputs": inputs,
                 "stationarity_rerun": {"root_annotation_id": context["run_id"],
                                        "previous_annotation_id": context["annotation_id"],
                                        "candidate_scope": _scope(candidates, rds=rds, logical_name=logical_name)}}
    records.append({
        "annotation_kind": "issue", "status": "suspected", "origin": "threshold",
        "issue_type": context["issue_type"], "source_artifact": "" if rds else logical_name,
        "scope": new_scope, "resolved_fix_count": len(added),
        "comment": "Stationarity rerun with saved settings after input changes. Existing review decisions retained.",
    })
    confirmed = set().union(*(keys_for(item) for item in human_resolutions
                              if item["status"] == "confirmed"))
    dismissed = resolved - confirmed
    result = {
        **public_run(context), "match_count": len(candidates), "added_count": len(added),
        "removed_count": len(removed), "reviewed_difference_count": len((confirmed - candidates) | (dismissed & candidates)),
        "records": records,
    }
    result["preview_token"] = sha256(json.dumps({"dataset": dataset_id, "source": source_signature,
        "run": context["run_id"], "records": records}, sort_keys=True).encode()).hexdigest()
    return result
