"""Speed/turn spike rules shared by CSV, RDS, previews and saved steps."""
from collections import OrderedDict, defaultdict
from hashlib import sha256
from math import isfinite
from pathlib import Path
from threading import Lock
import csv

import numpy as np

from .movement_features import compute_track_movement
from .stationarity import compressed_rows, stationarity_skips

ALGORITHM = "speed-turn-v1"
METHODS = {"manual", "study_quantiles", "physiology"}
STAGES = ("moderate", "fast")
OVERRIDE_FIELDS = {"lower_speed_mps", "upper_speed_mps", "moderate_min_abs_turn_deg", "fast_min_abs_turn_deg"}
_DISTRIBUTIONS = OrderedDict()
_LOCK = Lock()


def is_speed_spike(spec):
    return spec.get("kind") == "gps_spike" and spec.get("metric") == "speed"


def finite_number(value, label, *, positive=False):
    if isinstance(value, bool):
        raise ValueError(f"{label} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be numeric") from exc
    if not isfinite(number) or number < 0 or (positive and number == 0):
        raise ValueError(f"{label} must be finite and {'positive' if positive else 'nonnegative'}")
    return number


def validate_speed_spike(value):
    method = value.get("method", "manual")
    if method not in METHODS or value.get("algorithm", ALGORITHM) != ALGORITHM:
        raise ValueError("Unknown speed spike rule")
    result = {"kind": "gps_spike", "metric": "speed", "algorithm": ALGORITHM,
              "method": method, "units": "m/s", "comparison": "both-adjacent-speeds-strictly-greater",
              "implementation_sha256": sha256(__loader__.get_source(__name__).encode()).hexdigest()}
    if value.get("study_key"):
        result["study_key"] = str(value["study_key"])
    if method == "manual":
        result["speed_threshold_mps"] = finite_number(value.get("speed_threshold_mps"), "Spike speed threshold")
        turn = finite_number(value.get("minimum_abs_turn_angle_deg"), "Spike turn threshold")
        if turn > 180:
            raise ValueError("Spike turn threshold must be between 0 and 180 degrees")
        result["minimum_abs_turn_angle_deg"] = turn
    else:
        result.update(lower_probability=0.75, upper_probability=0.999 if method == "study_quantiles" else None,
                      moderate_min_abs_turn_deg=166.0, fast_min_abs_turn_deg=157.0,
                      lower_cutoff_policy="study-Q3", population="all-study-retained-outbound-speeds",
                      quantile_method="linear")
        if method == "physiology":
            # Previously saved filters used the point estimate. Keep their
            # recorded meaning; new previews use the supplied lower CI bound.
            field = value.get("species_cap_field", "v_max_m_s" if "distribution" in value else "vmax_ci_lower")
            if field not in {"v_max_m_s", "vmax_ci_lower"}:
                raise ValueError("Unknown species speed-cap field")
            result["species_cap_field"] = field
        stages = value.get("selected_stages", list(STAGES))
        if not isinstance(stages, list) or not stages or any(s not in STAGES for s in stages):
            raise ValueError("Select one or both speed ranges")
        result["selected_stages"] = list(dict.fromkeys(stages))
        overrides = value.get("threshold_overrides", {})
        if not isinstance(overrides, dict) or set(overrides) - OVERRIDE_FIELDS:
            raise ValueError("Unknown spike threshold override")
        if overrides:
            result["threshold_overrides"] = {}
            for key, raw in overrides.items():
                number = finite_number(raw, key)
                if key.endswith("_deg") and number > 180:
                    raise ValueError("Spike turn thresholds must be between 0 and 180 degrees")
                result["threshold_overrides"][key] = number
        # Resolved cutoffs are frozen in the saved step. They are not recomputed
        # from another machine's cap table or a later distribution on replay.
        if "distribution" in value:
            distribution = value["distribution"]
            if not isinstance(distribution, dict):
                raise ValueError("Invalid speed distribution")
            count = distribution.get("sample_count")
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ValueError("Invalid speed sample count")
            result["distribution"] = {"sample_count": count, **{
                k: finite_number(distribution.get(k), k) if count else None for k in ("q1", "q3", "p999")}}
            if count and not (result["distribution"]["q1"] <= result["distribution"]["q3"] <= result["distribution"]["p999"]):
                raise ValueError("Speed quantiles must satisfy Q1 <= Q3 <= P99.9")
            result["lower_speed_mps"] = result["distribution"]["q3"]
            if method == "study_quantiles":
                result["upper_speed_mps"] = result["distribution"]["p999"]
            else:
                cap = value.get("species_cap")
                if not isinstance(cap, dict):
                    raise ValueError("Missing recorded species cap")
                cap = dict(cap)
                cap["v_max_m_s"] = finite_number(cap.get("v_max_m_s"), "Species cap", positive=True)
                field = result["species_cap_field"]
                cap[field] = finite_number(cap.get(field), field, positive=field == "v_max_m_s")
                result.update(species_cap=cap, upper_speed_mps=cap[field])
        apply_threshold_overrides(result)
    return result


def apply_threshold_overrides(spec):
    """Keep calculated defaults as provenance; record and use explicit edits."""
    overrides = spec.get("threshold_overrides", {})
    spec.update(overrides)
    if "lower_speed_mps" in overrides:
        spec["lower_cutoff_policy"] = "custom"


def track_metrics(records, annotations):
    """Rebuild speeds/turns on each retained track; unresolved flags stay in it."""
    skipped = stationarity_skips(records, annotations, skip_gps=False)
    tracks = defaultdict(list)
    for i, record in enumerate(records):
        # Retain invalid positions as barriers, including when also excluded.
        # Canonical movement metrics leave adjacent spatial features missing.
        if i not in skipped or record.get("invalid"):
            tracks[(record["individual"], record["set_name"])].append(record)
    for key, track in tracks.items():
        track.sort(key=lambda row: (row["time_ms"], row["row_index"], row["fix_key"]))
        movement, _ = compute_track_movement({key: track}, max_stat_samples=0)
        yield track, movement


def speed_histogram(values, bins=24):
    """Exact counts for a small preview; log bins keep zeros separate.

    This is display metadata only. Neither binning nor axis choice affects
    the quantiles or the spike classification.
    """
    if not values.size:
        return {"sample_count": 0, "zero_count": 0, "linear": {"edges": [], "counts": []},
                "log": {"edges": [], "counts": []}}
    maximum = float(values.max())
    counts, edges = np.histogram(values, bins=np.linspace(0, maximum or 1.0, bins + 1))
    positive = values[values > 0]
    if positive.size:
        lower, upper = float(positive.min()), float(positive.max())
        if lower == upper:
            lower, upper = lower / 2, upper * 2
        log_edges = np.geomspace(lower, upper, bins + 1)
        log_edges[0], log_edges[-1] = lower, upper
        log_counts, log_edges = np.histogram(positive, bins=log_edges)
    else:
        log_edges, log_counts = np.array([]), np.array([], dtype=int)
    return {"sample_count": int(values.size), "zero_count": int((values == 0).sum()),
            "linear": {"edges": edges.tolist(), "counts": counts.tolist()},
            "log": {"edges": log_edges.tolist(), "counts": log_counts.tolist()}}


def study_distribution(track_factory, annotations, cache_key=None):
    """Exact whole-study quantiles, counting each finite step speed once.

    NumPy's linear quantile uses position (n - 1) * p in the sorted speeds,
    interpolating between neighbouring values (equivalent to R type = 7).
    No angle condition or candidate/individual selection enters this function.
    """
    if cache_key:
        with _LOCK:
            cached = _DISTRIBUTIONS.get(cache_key)
            if cached is not None:
                _DISTRIBUTIONS.move_to_end(cache_key)
                return dict(cached)
    chunks = []
    for records in track_factory():
        for track, movement in track_metrics(records, annotations):
            values = np.fromiter((movement[r["fix_key"]]["speed_mps"]
                                 if movement[r["fix_key"]]["speed_mps"] is not None else np.nan
                                 for r in track), dtype=np.float64, count=len(track))
            chunks.append(values[np.isfinite(values) & (values >= 0)])
    values = np.concatenate(chunks) if chunks else np.array([], dtype=float)
    q = np.quantile(values, [0.25, 0.75, 0.999], method="linear").tolist() if values.size else [None] * 3
    result = dict(sample_count=int(values.size), q1=q[0], q3=q[1], p999=q[2])
    result["histogram"] = speed_histogram(values)
    if cache_key:
        with _LOCK:
            _DISTRIBUTIONS[cache_key] = result
            while len(_DISTRIBUTIONS) > 16:
                _DISTRIBUTIONS.popitem(last=False)
    return dict(result)


def species_cap(study_key, species, *, study_ids=()):
    """Read the point estimate and lower CI without substituting either value."""
    path = Path(__file__).with_name("species_speed_caps.csv")
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    matches = [r for r in rows if r["study"] == study_key]
    matched_by = "study_key"
    if not matches:
        ids = {str(value).strip() for value in study_ids if str(value).strip()}
        matches = [r for r in rows if r.get("movebank_study_id") in ids] if len(ids) == 1 else []
        matched_by = "movebank_study_id"
    if not matches:
        names = {str(name).strip().casefold() for name in species if str(name).strip()}
        matches = [r for r in rows if r["scientific_name"].casefold() in names or r["common_name"].casefold() in names]
        if len(names) != 1:
            matches = []
        matched_by = "species"
    if len(matches) != 1:
        raise ValueError("No unique physiology cap for this study/species in species_speed_caps.csv")
    row = dict(matches[0])
    row.pop("", None)
    row["v_max_m_s"] = finite_number(row["v_max_m_s"], "Species cap", positive=True)
    row["vmax_ci_lower"] = finite_number(row.get("vmax_ci_lower"), "Species cap lower confidence bound")
    return {**row, "matched_by": matched_by,
            "source_file": path.name, "source_sha256": sha256(path.read_bytes()).hexdigest()}


def candidate_stage(incoming, outgoing, turn, spec):
    """Classify the middle fix of three using both adjacent speeds in m/s.

    min(incoming, outgoing) > cutoff means BOTH speeds exceed that cutoff.
    Signed turns use the existing bearing-change convention: 166..194 degrees
    is |turn| >= 166; 157..203 is |turn| >= 157 by default. Explicit overrides
    change those cutoffs. The lower speed cutoff always remains the floor,
    even when the upper cutoff is lower than it.
    """
    if any(v is None or not isfinite(v) for v in (incoming, outgoing, turn)):
        return None
    speed = min(incoming, outgoing)
    if spec["method"] == "manual":
        return "moderate" if speed > spec["speed_threshold_mps"] and abs(turn) >= spec["minimum_abs_turn_angle_deg"] else None
    lower, upper = spec["lower_speed_mps"], spec["upper_speed_mps"]
    if lower is None or upper is None or speed <= lower:
        return None
    stage = "fast" if speed > upper else "moderate"
    angle = spec.get("fast_min_abs_turn_deg", 157) if stage == "fast" else spec.get("moderate_min_abs_turn_deg", 166)
    return stage if abs(turn) >= angle else None


def resolve_speed_spikes(track_factory, raw_spec, annotations, *, cap_loader=None, cache_key=None, candidate_factory=None):
    spec = {**raw_spec, **validate_speed_spike(raw_spec)}
    histogram = None
    if spec["method"] != "manual" and "distribution" not in spec:
        spec["distribution"] = study_distribution(track_factory, annotations, cache_key)
        histogram = spec["distribution"].pop("histogram")
        spec["lower_speed_mps"] = spec["distribution"]["q3"]
        if spec["method"] == "physiology":
            spec["species_cap"] = cap_loader()
            field = spec["species_cap_field"]
            spec["upper_speed_mps"] = finite_number(spec["species_cap"].get(field), field, positive=field == "v_max_m_s")
        else:
            spec["upper_speed_mps"] = spec["distribution"]["p999"]
    apply_threshold_overrides(spec)
    individuals, sets = set(spec.get("individuals") or []), set(spec.get("set_names") or [])
    stages = {stage: defaultdict(list) for stage in STAGES}
    for records in (candidate_factory or track_factory)():
        if not records or (individuals and not any(r["individual"] in individuals for r in records)):
            continue
        for track, movement in track_metrics(records, annotations):
            if not track or (individuals and track[0]["individual"] not in individuals) or (sets and track[0]["set_name"] not in sets):
                continue
            for i in range(1, len(track) - 1):
                row = track[i]
                values = movement[row["fix_key"]]
                stage = candidate_stage(movement[track[i - 1]["fix_key"]]["speed_mps"],
                                        values["speed_mps"], values["turn_angle_deg"], spec)
                if stage:
                    stages[stage][row["source_artifact"]].append(row["row_index"])
    selected = set(spec.get("selected_stages", STAGES))
    combined = defaultdict(list)
    groups = []
    for stage, sources in stages.items():
        groups.append({"stage": stage, "fix_count": sum(map(len, sources.values())),
                       "source_rows": [{"logical_name": name, "row_ranges": compressed_rows(rows)}
                                       for name, rows in sorted(sources.items())]})
        if stage in selected:
            for name, rows in sources.items():
                combined[name].extend(rows)
    scope = {"kind": "filter", "filter": spec,
             "source_rows": [{"logical_name": name, "row_ranges": compressed_rows(rows)}
                             for name, rows in sorted(combined.items())],
             "spike_groups": groups}
    if histogram is not None:
        scope["speed_histogram"] = histogram
    return scope, sum(map(len, combined.values()))
