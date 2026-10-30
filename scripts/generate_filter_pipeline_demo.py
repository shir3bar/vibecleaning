"""Generate small native-AMT studies and separate raw/pre-filtered review copies."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys

import numpy as np
import pandas as pd
import rdata
from pyproj import Geod, CRS
from rdata.conversion.to_r import build_r_object
from rdata.parser import RObjectType

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

PATTERN = [1., 2., 2., 3.]
GEOD = Geod(ellps="WGS84")
EPOCH = 1704067200


class DemoCrs:
    """A native sf crs attribute, serialized without requiring R."""


def crs_constructor(value, converter):
    values = [converter.convert_to_r_object(np.array([text]))
              for text in ("EPSG:4326", CRS.from_epsg(4326).to_wkt())]
    attributes = converter.convert_to_r_attributes({"names": np.array(["input", "wkt"]), "class": "crs"})
    return build_r_object(RObjectType.VEC, value=values, is_object=True, attributes=attributes)


def amt_constructor(frame, converter):
    values = []
    for key in frame.columns:
        if key in {"t_", "timestamp"}:
            value = build_r_object(RObjectType.REAL, value=frame[key].to_numpy(dtype=float), is_object=True,
                attributes=converter.convert_to_r_attributes({"class": np.array(["POSIXct", "POSIXt"]), "tzone": "UTC"}))
        else:
            data = frame[key].array
            value = converter.convert_to_r_object(data if isinstance(data, pd.Categorical) else frame[key].to_numpy())
        values.append(value)
    attributes = converter.convert_to_r_attributes({"names": np.array(frame.columns, dtype="U"),
        "class": np.array(["track_xyt", "track_xy", "tbl_df", "tbl", "data.frame"]),
        "row.names": np.arange(1, len(frame) + 1, dtype=np.int32),
        "crs_": DemoCrs(), "synthetic": True})
    return build_r_object(RObjectType.VEC, value=values, is_object=True, attributes=attributes)


def regular(n):
    return np.r_[0, np.cumsum([60 * PATTERN[i % 4] for i in range(n)])].tolist(), (np.arange(n + 1) * 60).tolist()


def scenarios():
    xs, times = regular(30)
    return {
        "910001": [
            ("Stationary_head", [0.] * 80 + [200 + x for x in xs],
             (np.arange(80) * 600).tolist() + (47400 + np.arange(1, 32) * 60).tolist()),
            ("Control", *regular(90)), ("Speed_candidate", [0, 10800, 0], [0, 60, 120])],
        "910002": [("Reference", *regular(100)), ("Borderline_GPS", [0, 4800, 0], [0, 60, 120]),
                   ("Extreme_speed", [0, 60000, 0], [0, 60, 120])],
        "910003": [("Exactly_12_hours", [0, 0, 0], [0, 21600, 43200]),
                   ("Over_12_hours", [0, 0, 0], [0, 21600, 43201]),
                   ("Radius_100m", [0, 100, 0], [0, 21600, 43201]),
                   ("Outside_radius", [0, 101, 0], [0, 21600, 43201]),
                   ("Invalid_gap", [0, 0, 0, 0, 0], [0, 21600, 21601, 21602, 43201]),
                   ("Unplottable", [0, 0, 0], [0, 60, 120]), ("Single_fix", [0], [0])],
    }


def generate(destination, *, prepare=True):
    destination = Path(destination)
    if destination.exists():
        raise ValueError("Choose a new output folder so existing review history is preserved")
    raw = destination / "raw" / "movement_rds"
    checksums = {}
    for study_id, tracks in scenarios().items():
        study = raw / study_id
        study.mkdir(parents=True)
        individuals = {}
        for individual_id, (name, xs, times) in enumerate(tracks, 1):
            lons = [GEOD.fwd(0, 0, 90, float(x))[0] for x in xs]
            if name == "Invalid_gap":
                lons[2] = 200.
            if name == "Unplottable":
                lons = [200.] * len(xs)
            frame = pd.DataFrame({"x_": lons, "y_": np.zeros(len(xs)), "t_": EPOCH + np.array(times, dtype=float),
                "timestamp": EPOCH + np.array(times, dtype=float), "study_id": np.full(len(xs), int(study_id), dtype=np.int32),
                "individual_id": np.full(len(xs), individual_id, dtype=np.int32),
                "individual_local_identifier": pd.Categorical([name] * len(xs), categories=[name, "unused-level"]),
                "tag_local_identifier": pd.Categorical(["demo-tag"] * len(xs)), "burst_": np.ones(len(xs))})
            filename = f"{study_id}_{individual_id}.rds"
            path = study / filename
            rdata.write_rds(path, frame, constructor_dict={**rdata.conversion.to_r.DEFAULT_CLASS_MAP,
                pd.DataFrame: amt_constructor, DemoCrs: crs_constructor})
            checksums[f"{study_id}/{filename}"] = hashlib.sha256(path.read_bytes()).hexdigest()
            individuals[str(individual_id)] = {"individual_id": str(individual_id), "source_file": filename,
                "individual_local_identifier": name, "species": "Vulpes vulpes", "sex": None,
                "deployments": [{"deployment_id": str(individual_id), "notes": "Synthetic demonstration; not observed animal movement."}]}
        (study / "metadata.json").write_text(json.dumps({"schema_version": 1, "study_id": study_id,
            "source": {"file_name": "synthetic", "sha256": "synthetic"}, "units": {}, "individuals": individuals}, indent=2) + "\n", encoding="utf-8")
    shutil.copytree(destination / "raw", destination / "prefiltered")
    expected = {"source_checksums": checksums, "910001": {"stationarity_flags": 80, "speed_flags": 1,
        "speed_cutoff_before": 152.75, "speed_cutoff_after_stationarity_unflag": 202., "gps_flags_after_speed_update": 1},
        "910002": {"speed_flags": 1, "gps_flags": 1, "gps_p99_before": 78.46, "gps_p99_after_speed_unflag": 972.4}}
    (destination / "expected.json").write_text(json.dumps(expected, indent=2) + "\n", encoding="utf-8")
    if prepare:
        from scripts.prefilter_movement_rds import prefilter_folder
        report = prefilter_folder(destination / "prefiltered" / "movement_rds", user="synthetic-demo")
        (destination / "preparation.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        if report["failures"]:
            raise RuntimeError("Demo pre-filtering failed; inspect preparation.json")
    walkthrough = Path(__file__).resolve().parents[1] / "docs" / "STUDY_FILTERS.md"
    if walkthrough.exists():
        shutil.copyfile(walkthrough, destination / "README.md")
    return expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--raw-only", action="store_true", help="Leave both copies without pre-filtering")
    args = parser.parse_args()
    generate(args.output.resolve(), prepare=not args.raw_only)


if __name__ == "__main__":
    main()
