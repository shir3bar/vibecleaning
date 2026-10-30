"""Optional, study-wide filter runs. Automatic suspicions only feed forward.

Calculations are previews; only an explicit guarded step changes review state.
Ranges always address original source rows, never disposable index ordinals.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import closing
import hashlib
import json
from pathlib import Path
import os
import tempfile
import zipfile

import numpy as np

from .review_annotations import load_review_annotations, normalize_row_ranges, row_number_in_ranges
from .stationarity import evaluate_stationarity, validate_stationarity_filter
from .movement_features import compute_track_movement, speed_threshold_matches

STAGES = ("stationarity", "speed", "gps_spike")
LABELS = {"stationarity": "Stationarity", "speed": "Speed", "gps_spike": "GPS Spike"}
PROFILE = "movetraits-review-v1"
IMPLEMENTATION_VERSION = "study-filters-1"
STAGE_IMPLEMENTATION_VERSIONS = {"speed": "study-filters-speed-2"}
RULES = {
    "stationarity": {"radius_m": 100, "minimum_duration_s": 43200, "duration_comparison": "gt",
                     "maximum_gap_s": 259200, "endpoint_rule": "any-first-or-last-three"},
    "speed": {"method": "Q3 + 100 * IQR", "comparison": "gt",
              "speed_basis": "both-adjacent", "ownership": "central-fix"},
    "gps_spike": {"probability": .99, "minimum_abs_turn_deg": 166, "comparison": "gt"},
}


def artifact_name(stage):
    if stage not in STAGES:
        raise ValueError("Unknown study filter")
    return f"movement_filter_{stage}.json"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def merge(*maps):
    combined = defaultdict(list)
    for mapping in maps:
        for name, ranges in mapping.items():
            combined[name].extend(ranges)
    return {name: normalize_row_ranges(ranges) for name, ranges in sorted(combined.items()) if ranges}


def subtract(left, right):
    result = {}
    for name, ranges in left.items():
        remaining = []
        cuts = right.get(name, [])
        j = 0
        for start, end in ranges:
            while j < len(cuts) and cuts[j][1] < start:
                j += 1
            k, cursor = j, start
            while k < len(cuts) and cuts[k][0] <= end:
                low, high = cuts[k]
                if low > cursor:
                    remaining.append([cursor, min(end, low - 1)])
                cursor = max(cursor, high + 1)
                if cursor > end:
                    break
                k += 1
            if cursor <= end:
                remaining.append([cursor, end])
        if remaining:
            result[name] = remaining
    return result


def count(mapping):
    return sum(end - start + 1 for ranges in mapping.values() for start, end in ranges)


def scope(mapping):
    return {"kind": "fix", "source_rows": [{"logical_name": name, "row_ranges": ranges}
                                             for name, ranges in sorted(mapping.items())]}


def stage_of(annotation):
    return (annotation.get("scope", {}).get("study_filter") or {}).get("stage")


def gps_candidate(incoming, outgoing, angle, threshold):
    return (all(value is not None and np.isfinite(value) for value in (incoming, outgoing, angle, threshold))
            and incoming > threshold and outgoing > threshold and abs(angle) > 166)


class StudyFilters:
    def __init__(self, study_dir, dataset_id, *, rds=True, logical_name="", cache_root=None):
        from app.state import ProjectStateError, get_dataset_artifact
        self.study_dir, self.dataset_id = Path(study_dir), dataset_id
        self.rds, self.logical_name = rds, logical_name
        self.catalog, self.source_annotations = {}, []
        if rds:
            from .rds_index import ensure_rds_index, _connect, _artifact_content_signature
            bundle, self.path = ensure_rds_index(self.study_dir, dataset_id, cache_root=cache_root)
            self.source_signature = bundle.signature
            self.source_checksums = {a["logical_name"]: _artifact_content_signature(p) for a, p in zip(bundle.artifacts, bundle.paths)}
            with closing(_connect(self.path)) as connection:
                for row in connection.execute("SELECT a.logical_name, a.row_count, i.identifier FROM artifacts a JOIN individuals i ON i.artifact_id=a.artifact_id"):
                    self.catalog[row["logical_name"]] = {"individual": row["identifier"], "row_count": row["row_count"]}
                for row in connection.execute("SELECT a.logical_name, f.source_row, f.fix_key, f.source_outlier_status, f.source_outlier_issue_type FROM fixes f JOIN artifacts a ON a.artifact_id=f.artifact_id WHERE f.source_outlier_status != ''"):
                    self.source_annotations.append({"annotation_id": f"source:{row['fix_key']}",
                        "status": row["source_outlier_status"], "issue_type": row["source_outlier_issue_type"],
                        "scope": scope({row["logical_name"]: [[row["source_row"], row["source_row"]]]})})
        else:
            from .review_annotations import csv_stationarity_records
            _, self.path = get_dataset_artifact(self.study_dir, dataset_id, logical_name)
            self.source_signature = hashlib.sha256(self.path.read_bytes()).hexdigest()
            self.source_checksums = {logical_name: self.source_signature}
            self.csv_records = csv_stationarity_records(self.path, {}, source_artifact=logical_name)
            self.catalog[logical_name] = {"individual": "", "row_count": max((r["row_index"] for r in self.csv_records), default=0)}
            self.source_annotations = [r["source_annotation"] for r in self.csv_records if r.get("source_annotation")]
        try:
            _, annotations_path = get_dataset_artifact(self.study_dir, dataset_id, "movement_review_annotations.json")
        except ProjectStateError:
            annotations_path = None
        self.annotations = load_review_annotations(annotations_path)
        self.all_annotations = [*self.source_annotations, *self.annotations]
        self.children = defaultdict(list)
        for annotation in self.all_annotations:
            self.children[annotation.get("parent_annotation_id", "")].append(annotation)

    def mapping(self, annotation):
        spec = annotation.get("scope") or {}
        sources = spec.get("source_rows") or []
        result = merge(*[{s["logical_name"]: s.get("row_ranges", [])} for s in sources])
        if not sources and spec.get("row_ranges"):
            result = {annotation.get("source_artifact") or self.logical_name: spec["row_ranges"]}
        if spec.get("kind") == "individual" and not result:
            individual = spec.get("individual")
            if self.rds:
                result = {name: [[1, info["row_count"]]] for name, info in self.catalog.items()
                          if info["individual"] == individual and info["row_count"]}
            else:
                result = {self.logical_name: normalize_row_ranges([[r["row_index"], r["row_index"]]
                    for r in self.csv_records if r["individual"] == individual])}
        return {name: ranges for name, ranges in result.items() if name in self.catalog and ranges}

    def pending(self, annotation):
        result = self.mapping(annotation)
        for child in self.children.get(annotation.get("annotation_id"), []):
            if child.get("status") in {"confirmed", "dismissed"}:
                result = subtract(result, self.mapping(child))
        return result

    def excluded(self, stage):
        rank = STAGES.index(stage)
        maps = []
        for annotation in self.all_annotations:
            if annotation.get("status") == "confirmed":
                maps.append(self.mapping(annotation))
            elif annotation.get("status") == "suspected" and not annotation.get("parent_annotation_id"):
                owner = stage_of(annotation)
                if owner not in STAGES or STAGES.index(owner) < rank:
                    maps.append(self.pending(annotation))
        return merge(*maps)

    def signature(self, stage, excluded):
        version = STAGE_IMPLEMENTATION_VERSIONS.get(stage, IMPLEMENTATION_VERSION)
        return digest([PROFILE, version, stage, RULES[stage], self.source_signature, excluded])

    def saved(self, stage):
        from app.state import ProjectStateError, get_dataset_artifact
        try:
            _, path = get_dataset_artifact(self.study_dir, self.dataset_id, artifact_name(stage))
        except ProjectStateError:
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def status(self):
        stages = []
        for stage in STAGES:
            saved, excluded = self.saved(stage), self.excluded(stage)
            old = (saved or {}).get("excluded_source_rows", {})
            changed = merge(subtract(old, excluded), subtract(excluded, old))
            people = {self.catalog[name]["individual"] for name in changed if self.catalog[name]["individual"]}
            public_saved = {k: v for k, v in saved.items() if k not in {"candidate_scope", "excluded_source_rows"}} if saved else None
            stages.append({"stage": stage, "label": LABELS[stage], "rules": RULES[stage], "saved": public_saved,
                "status": "not_run" if not saved else "outdated" if saved["input_signature"] != self.signature(stage, excluded) else "current",
                "changed_fix_count": count(changed), "changed_individual_count": len(people)})
        for i, item in enumerate(stages):
            item["pending_upstream"] = [s["label"] for s in stages[:i] if s["status"] != "current"]
        return {"profile": PROFILE, "dataset_id": self.dataset_id, "source_signature": self.source_signature,
                "optional": True, "stages": stages}

    def tracks(self):
        if self.rds:
            from .rds_index import _rds_stationarity_tracks
            yield from _rds_stationarity_tracks(self.path, {})
        else:
            groups = defaultdict(list)
            for row in self.csv_records:
                groups[(row["individual"], row["set_name"])].append(row)
            yield from groups.values()

    def metrics(self, excluded):
        from app.filesystem import atomic_replace, exclusive_file_lock
        from app.runtime import resolve_cache_root
        cache_root = (self.path.parent if self.rds else resolve_cache_root()) / "study-filter-metrics"
        cache_root.mkdir(parents=True, exist_ok=True)
        fields = ("step_length_m", "speed_mps", "time_delta_s", "turn_angle_deg")
        for records in self.tracks():
            retained = [{**r, "lon": float("nan"), "lat": float("nan")} if r.get("invalid") else r
                        for r in records if r.get("invalid") or not row_number_in_ranges(r["row_index"], excluded.get(r["source_artifact"], []))]
            if not retained:
                continue
            retained.sort(key=lambda r: (r["time_ms"], r["row_index"]))
            first = retained[0]
            name = first["source_artifact"]
            key = digest(["retained-metrics-v1", name, self.source_checksums[name],
                          first["individual"], first["set_name"], excluded.get(name, [])])
            target = cache_root / f"{key}.npz"
            rows = np.array([r["row_index"] for r in retained], dtype=np.int64)
            with exclusive_file_lock(target.with_suffix(".lock")):
                try:
                    with np.load(target, allow_pickle=False) as cached:
                        if not np.array_equal(cached["rows"], rows):
                            raise ValueError("Cached row order differs")
                        values = cached["values"]
                        if values.shape != (len(rows), len(fields)):
                            raise ValueError("Cached metric shape differs")
                except (OSError, ValueError, KeyError, EOFError, zipfile.BadZipFile):
                    movement, _ = compute_track_movement({(first["individual"], first["set_name"]): retained}, max_stat_samples=0)
                    values = np.array([[movement[r["fix_key"]][field] for field in fields] for r in retained], dtype=float)
                    fd, temporary = tempfile.mkstemp(prefix=".metrics-", suffix=".npz", dir=cache_root)
                    try:
                        with os.fdopen(fd, "wb") as stream:
                            np.savez_compressed(stream, rows=rows, values=values)
                        atomic_replace(Path(temporary), target)
                    finally:
                        Path(temporary).unlink(missing_ok=True)
            movement = {r["fix_key"]: {field: float(v) if np.isfinite(v) else None for field, v in zip(fields, vector)}
                        for r, vector in zip(retained, values)}
            yield retained, movement

    def calculate(self, stage, excluded):
        matches, statistics = defaultdict(list), {}
        if stage == "stationarity":
            # Integer millisecond times make this exactly duration > 12 h,
            # preserving v4's saved >= semantics for legacy filters.
            spec = validate_stationarity_filter({"radius_m": 100, "minimum_duration_s": 43200.001,
                "maximum_gap_s": 259200, "position": "ends"})
            tracks = self.tracks()
            if not self.rds:
                # Track sets do not introduce new individual endpoints.
                individuals = defaultdict(list)
                for row in self.csv_records:
                    individuals[row["individual"]].append(row)
                tracks = individuals.values()
            for track in tracks:
                frozen = [{**r, "excluded": row_number_in_ranges(r["row_index"], excluded.get(r["source_artifact"], [])),
                           "source_annotation": None} for r in track]
                keys, _ = evaluate_stationarity(frozen, spec, [])
                selected = set(keys)
                for row in track:
                    if row["fix_key"] in selected:
                        matches[row["source_artifact"]].append([row["row_index"], row["row_index"]])
        else:
            chunks = []
            for rows, movement in self.metrics(excluded):
                values = np.array([movement[r["fix_key"]]["speed_mps"] for r in rows], dtype=float)
                chunks.append(values[np.isfinite(values) & (values >= 0)])
            speeds = np.concatenate(chunks) if chunks else np.array([])
            statistics = {"sample_count": len(speeds), "quantile_method": "linear", "units": "m/s",
                          "q1": None, "q3": None, "p99": None, "threshold_mps": None}
            if speeds.size:
                q1, q3, p99 = np.quantile(speeds, [.25, .75, .99], method="linear").tolist()
                threshold = q3 + 100 * (q3 - q1) if stage == "speed" else p99
                statistics.update(q1=q1, q3=q3, p99=p99, threshold_mps=threshold)
                for rows, movement in self.metrics(excluded):
                    for i, row in enumerate(rows):
                        values = movement[row["fix_key"]]
                        speed = values["speed_mps"]
                        incoming = movement[rows[i - 1]["fix_key"]]["speed_mps"] if i else None
                        match = speed_threshold_matches(incoming, speed, threshold)
                        if stage == "gps_spike":
                            turn = values["turn_angle_deg"]
                            match = gps_candidate(incoming, speed, turn, threshold)
                        if match:
                            matches[row["source_artifact"]].append([row["row_index"], row["row_index"]])
            else:
                statistics["unavailable_reason"] = "No finite nonnegative speeds between eligible positions."
        return merge(matches), statistics

    def preview(self, stage):
        artifact_name(stage)
        excluded = self.excluded(stage)
        candidates, statistics = self.calculate(stage, excluded)
        family = [a for a in self.annotations if stage_of(a) == stage and not a.get("parent_annotation_id")]
        family_ids = {a["annotation_id"] for a in family}
        humans = [a for a in self.annotations if a.get("parent_annotation_id") in family_ids
                  and a.get("status") in {"confirmed", "dismissed"}
                  and not (a.get("scope", {}).get("study_filter") or {}).get("superseded")]
        protected = merge(*(self.mapping(a) for a in humans))
        pending = merge(*(self.pending(a) for a in family))
        added = subtract(subtract(candidates, pending), protected)
        removed = subtract(pending, candidates)
        records = []
        for parent in family:
            obsolete = subtract(self.pending(parent), candidates)
            if obsolete:
                records.append({"annotation_kind": "dismissal", "status": "dismissed", "origin": "algorithm",
                    "parent_annotation_id": parent["annotation_id"], "issue_type": parent["issue_type"],
                    "scope": {**scope(obsolete), "study_filter": {"stage": stage, "superseded": True}},
                    "resolved_fix_count": count(obsolete), "comment": "Superseded by an optional study-wide filter recalculation."})
        added_by_individual = defaultdict(dict)
        for name, ranges in added.items():
            info = self.catalog[name]
            if info["individual"]:
                added_by_individual[info["individual"]][name] = ranges
            else:
                for row in self.csv_records:
                    if row_number_in_ranges(row["row_index"], ranges):
                        added_by_individual[row["individual"]].setdefault(name, []).append([row["row_index"], row["row_index"]])
        records.append({"annotation_kind": "issue", "status": "suspected", "origin": "threshold",
            "issue_type": f"Filter {LABELS[stage]} (study protocol)",
            "scope": {**scope(added), "kind": "filter", "filter": {"kind": "study_filter", "stage": stage},
                "study_filter": {"stage": stage, "profile": PROFILE, "added_by_individual": dict(added_by_individual)}},
            "resolved_fix_count": count(added), "comment": "Study-wide filter; human decisions retained."})
        snapshot = {"schema_version": 1, "profile": PROFILE, "stage": stage, "rules": RULES[stage],
            "implementation_version": STAGE_IMPLEMENTATION_VERSIONS.get(stage, IMPLEMENTATION_VERSION),
            "source_checksums": self.source_checksums,
            "source_signature": self.source_signature, "input_signature": self.signature(stage, excluded),
            "excluded_source_rows": excluded, "candidate_scope": scope(candidates), "statistics": statistics,
            "candidate_count": count(candidates), "effective_count": count(subtract(candidates, protected))}
        changes = {}
        for kind, mapping in (("added", added), ("removed", removed)):
            for name, ranges in mapping.items():
                individual = self.catalog[name]["individual"]
                if individual:
                    item = changes.setdefault(individual, {"added": 0, "removed": 0})
                    item[kind] += count({name: ranges})
        result = {"stage": stage, "label": LABELS[stage], "dataset_id": self.dataset_id,
            "review_signature": digest(self.annotations),
            "snapshot": snapshot, "previous": self.saved(stage), "records": records,
            "added_count": count(added), "removed_count": count(removed),
            "affected_individuals": sorted(changes or added_by_individual), "changes_by_individual": changes,
            "reviewed_difference_count": count(merge(
                subtract(merge(*(self.mapping(a) for a in humans if a.get("status") == "confirmed")), candidates),
                subtract(protected, subtract(protected, candidates))))}
        result["preview_token"] = digest(result)
        return result


def pending_review_individuals(annotations):
    """New unresolved filter issues after an individual's last acknowledgement."""
    reviewed_at = {}
    children = defaultdict(list)
    for i, item in enumerate(annotations):
        if item.get("annotation_kind") == "individual_review" and item.get("reviewed"):
            reviewed_at[item.get("scope", {}).get("individual")] = i
        if item.get("parent_annotation_id") and item.get("status") in {"confirmed", "dismissed"}:
            children[item["parent_annotation_id"]].append(item)
    pending = set()
    for i, item in enumerate(annotations):
        if item.get("status") != "suspected" or item.get("parent_annotation_id"):
            continue
        for individual, mapping in (item.get("scope", {}).get("study_filter") or {}).get("added_by_individual", {}).items():
            if individual not in reviewed_at or reviewed_at[individual] >= i:
                continue
            for child in children[item["annotation_id"]]:
                resolved = {s["logical_name"]: s["row_ranges"] for s in child.get("scope", {}).get("source_rows", [])}
                if not resolved and child.get("scope", {}).get("row_ranges"):
                    resolved = {name: child["scope"]["row_ranges"] for name in mapping
                                if not child.get("source_artifact") or child["source_artifact"] == name}
                mapping = subtract(mapping, resolved)
            if mapping:
                pending.add(individual)
    return pending


def step_payload(preview, *, user, script, source_artifacts, logical_name=""):
    stage = preview["stage"]
    verb = "Recalculate" if preview.get("previous") else "Apply"
    return {"user": user, "title": f"{verb} {LABELS[stage]} across study", "kind": "python", "script": script,
        "parent_dataset_id": preview["dataset_id"], "set_as_head": True,
        "input_artifacts": source_artifacts,
        "output_artifacts": ["movement_review_annotations.json", artifact_name(stage)],
        "parameters": {"app": "movement", "action": "recalculate_study_filter", "user": user,
            "dataset_id": preview["dataset_id"], "target_artifact": logical_name,
            "source_bundle_signature": preview["snapshot"]["source_signature"], "records": preview["records"],
            "study_filter_snapshot": preview["snapshot"], "study_filter_summary": {k: preview[k] for k in
                ("stage", "added_count", "removed_count", "affected_individuals")}}}
