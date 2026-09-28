"""Conservative stationarity candidates, shared by CSV and RDS review filters."""

from hashlib import sha256
from math import isfinite
from bisect import bisect_left, bisect_right
from collections import defaultdict, OrderedDict
import json
from threading import Lock

from .movement_features import geodesic_distance_meters


ALGORITHM = "anchor-radius-v3"
SUPPORTED_ALGORITHMS = {"anchor-radius-v1", "anchor-radius-v2", ALGORITHM}
_SCAN_CACHE = OrderedDict()
_SCAN_CACHE_LOCK = Lock()
_SCAN_CACHE_MAX_FIXES = 250_000


def is_gps_spike_annotation(annotation: dict) -> bool:
    # Portable exports retain issue labels (sometimes joined with semicolons),
    # while the app's own filter flags also carry their exact filter definition.
    labels = {label.strip().lower().removeprefix("filter ")
              for label in str(annotation.get("issue_type") or "").split(";")}
    return (
        ((annotation.get("scope") or {}).get("filter") or {}).get("kind") == "gps_spike"
        or annotation.get("issue_field") == "gps_spike_step_turn"
        or bool(labels & {"gps spike", "gps spikes", "gps spike (step + turn)"})
    )


def compressed_rows(rows) -> list[list[int]]:
    result = []
    for row in sorted(set(rows)):
        if result and row == result[-1][1] + 1:
            result[-1][1] = row
        else:
            result.append([row, row])
    return result


def stationarity_scope_matcher(records: list[dict]):
    """Match saved scopes to just these records, without expanding study-wide ranges."""
    groups = defaultdict(list)
    for index, record in enumerate(records):
        groups[(record["source_artifact"], record["individual"], record["set_name"])].append(
            (record["row_index"], index))
    groups = {key: sorted(values) for key, values in groups.items()}
    row_numbers = {key: [row for row, _ in values] for key, values in groups.items()}

    def matches(annotation):
        scope = annotation.get("scope") or {}
        result = set()
        for key, values in groups.items():
            artifact, individual, set_name = key
            if annotation.get("source_artifact") not in (None, "", artifact):
                continue
            if scope.get("kind") == "individual":
                if scope.get("individual") == individual and scope.get("set_name") in (None, "", set_name):
                    result.update(index for _, index in values)
                continue
            ranges = scope.get("row_ranges") or []
            if scope.get("source_rows"):
                ranges = [pair for source in scope["source_rows"]
                          if source.get("logical_name") == artifact for pair in source.get("row_ranges", [])]
            for start, end in ranges:
                lo, hi = bisect_left(row_numbers[key], start), bisect_right(row_numbers[key], end)
                result.update(index for _, index in values[lo:hi])
        return result

    return matches


def stationarity_skips(records: list[dict], annotations: list[dict], *, skip_gps=True) -> set[int]:
    """Resolve only this input window; never allocate review state for a study.

    Confirmed exclusions always win. Dismissing one GPS allegation only restores
    its fixes if no other GPS allegation or confirmation still excludes them.
    """
    matches = stationarity_scope_matcher(records)
    # Source review fields are allegations too, with the same stable parent IDs
    # used by Confirm/Unflag. Generic owner flags are not assumed to be GPS spikes.
    source_annotations = [record["source_annotation"] for record in records if record.get("source_annotation")]
    relevant = [*source_annotations, *annotations]
    children = defaultdict(list)
    for annotation in relevant:
        if annotation.get("parent_annotation_id"):
            children[annotation["parent_annotation_id"]].append(annotation)
    skipped = {i for i, record in enumerate(records) if record.get("excluded")}
    for annotation in relevant:
        if annotation.get("status") == "confirmed":
            skipped.update(matches(annotation))
        elif (skip_gps and annotation.get("status") == "suspected"
              and not annotation.get("parent_annotation_id") and is_gps_spike_annotation(annotation)):
            pending = matches(annotation)
            for child in children.get(annotation.get("annotation_id"), []):
                if child.get("status") in {"dismissed", "confirmed"}:
                    pending.difference_update(matches(child))
            skipped.update(pending)
    return skipped


def stationarity_inputs(records: list[dict], skipped: set[int]) -> list[dict]:
    """Persist examined windows and the effective skipped rows, including zero matches."""
    groups = defaultdict(list)
    for index, record in enumerate(records):
        groups[(record["source_artifact"], record["individual"], record["set_name"])].append((index, record))
    return [{
        "source_artifact": artifact, "individual": individual, "set_name": set_name,
        "start_ms": min(record["time_ms"] for _, record in entries),
        "end_ms": max(record["time_ms"] for _, record in entries),
        "input_rows": compressed_rows(record["row_index"] for _, record in entries),
        "skipped_rows": compressed_rows(record["row_index"] for i, record in entries if i in skipped),
    } for (artifact, individual, set_name), entries in sorted(groups.items())]


def evaluate_stationarity(records: list[dict], spec: dict, annotations: list[dict], *, calculate=True):
    skipped = stationarity_skips(records, annotations, skip_gps=spec.get("algorithm", ALGORITHM) == ALGORITHM)
    inputs = stationarity_inputs(records, skipped)
    if not calculate:
        return [], inputs
    tracks = defaultdict(list)
    for i, record in enumerate(records):
        tracks[(record["source_artifact"], record["individual"], record["set_name"])].append(
            {**record, "excluded": i in skipped or record.get("invalid", False)})
    matches = []
    for track in tracks.values():
        track.sort(key=lambda item: (item["time_ms"], item["row_index"]))
        matches.extend(_cached_scan(track, spec))
    return matches, inputs


def _cached_scan(records, spec):
    # A new dataset or a confirmation of an already-skipped GPS flag does not
    # require another distance scan. Cache by actual track inputs and settings,
    # separately per individual/source/set, keeping only bounded result lists.
    criteria = {key: spec.get(key) for key in (
        "algorithm", "radius_m", "minimum_duration_s", "maximum_gap_s", "minimum_fixes", "position")}
    points = [[record.get(key) for key in (
        "fix_key", "time_ms", "lon", "lat", "burst", "segment", "excluded", "invalid")]
        for record in records]
    signature = sha256(json.dumps([criteria, points], separators=(",", ":")).encode()).digest()
    with _SCAN_CACHE_LOCK:
        cached = _SCAN_CACHE.get(signature)
        if cached is not None:
            _SCAN_CACHE.move_to_end(signature)
            return cached
    result = tuple(stationary_fix_keys(records, spec))
    if len(result) <= _SCAN_CACHE_MAX_FIXES:
        with _SCAN_CACHE_LOCK:
            _SCAN_CACHE[signature] = result
            while len(_SCAN_CACHE) > 32 or sum(map(len, _SCAN_CACHE.values())) > _SCAN_CACHE_MAX_FIXES:
                _SCAN_CACHE.popitem(last=False)
    return result


def validate_stationarity_filter(value: dict) -> dict:
    algorithm = value.get("algorithm", ALGORITHM)
    if not isinstance(algorithm, str) or algorithm not in SUPPORTED_ALGORITHMS:
        raise ValueError("Unsupported stationarity algorithm")
    result = {"kind": "stationarity", "algorithm": algorithm}
    for key in ("radius_m", "minimum_duration_s", "maximum_gap_s"):
        raw = value.get(key)
        try:
            number = float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Stationarity {key} must be a positive number") from exc
        if isinstance(raw, bool) or not isfinite(number) or number <= 0:
            raise ValueError(f"Stationarity {key} must be a positive number")
        result[key] = number
    points = value.get("minimum_fixes", 3)
    if isinstance(points, bool) or not isinstance(points, int) or points < 3:
        raise ValueError("Stationarity requires at least 3 fixes")
    position = value.get("position", "ends")
    if position not in {"ends", "anywhere"}:
        raise ValueError("Stationarity position must be ends or anywhere")
    result.update(
        minimum_fixes=points,
        position=position,
        implementation_sha256=sha256(__loader__.get_source(__name__).encode("utf-8")).hexdigest(),
    )
    if algorithm == ALGORITHM:
        result["skip_policy"] = "confirmed-and-saved-gps-spikes"
    return result


def stationary_fix_keys(records: list[dict], spec: dict) -> list[str]:
    """Scan one individual/source/track set in chronological order.

    Each non-overlapping run has a fixed centre: its first fix. A fix outside
    that radius starts the next run. This bounds spatial extent even under
    slow drift; it is deliberately not an exhaustive search over all possible
    centres. V3 skips excluded fixes and measures gaps between retained fixes.
    Invalid rows, non-increasing times and tag/time-fragment changes still break
    runs. Explicit v1/v2 filters retain their original exclusion rules.
    Ends means the first/last retained record of this track, not the edges of
    internal gaps or bursts. No coordinates are changed.
    """
    if not records:
        return []
    if spec.get("algorithm", ALGORITHM) == ALGORITHM:
        retained = []
        previous = None
        for record in records:
            if previous and (record.get("segment") != previous.get("segment")
                             or record["time_ms"] <= previous["time_ms"]):
                retained.append({"excluded": True})
            previous = record
            if record.get("invalid"):
                retained.append({"excluded": True})
            elif not record.get("excluded"):
                retained.append(record)
        records = retained
        if not records:
            return []
    matched = []
    run = []
    run_start = 0
    boundary_key = "burst" if spec.get("algorithm", ALGORITHM) == "anchor-radius-v1" else "segment"

    def finish(end: int) -> None:
        if len(run) < spec["minimum_fixes"]:
            return
        if run[-1]["time_ms"] - run[0]["time_ms"] < spec["minimum_duration_s"] * 1000:
            return
        if spec["position"] == "ends" and run_start != 0 and end != len(records):
            return
        matched.extend(item["fix_key"] for item in run)

    for index, record in enumerate(records):
        if record.get("excluded"):
            finish(index)
            run = []
            continue
        if run:
            gap_ms = record["time_ms"] - run[-1]["time_ms"]
            distance = geodesic_distance_meters(
                run[0]["lon"], run[0]["lat"], record["lon"], record["lat"]
            )
            if (
                gap_ms <= 0 or gap_ms > spec["maximum_gap_s"] * 1000
                or record.get(boundary_key) != run[-1].get(boundary_key)
                or distance is None or not isfinite(distance) or distance > spec["radius_m"]
            ):
                finish(index)
                run = []
        if not run:
            run_start = index
        run.append(record)
    finish(len(records))
    return matched
