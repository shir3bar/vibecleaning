"""Split a master MoveTraits metadata RDS into portable per-study JSON files."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import warnings

import numpy as np
import pandas as pd
import rdata

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from examples.movement.rds_index import _rds_constructors, _sf_constructor
from examples.movement.study_metadata import METADATA_UNITS, SOURCE_NAME


def study_folders(collection: Path) -> list[Path]:
    """Accept a full collection, one annotator root, or movement_rds itself."""
    roots = set(collection.glob("*/movement_rds"))
    if (collection / "movement_rds").is_dir():
        roots.add(collection / "movement_rds")
    if collection.name == "movement_rds" and collection.is_dir():
        roots.add(collection)
    studies = sorted(study for root in roots if root.is_dir()
                     for study in root.iterdir()
                     if study.is_dir() and not study.name.startswith(".") and any(study.glob("*.rds")))
    if not studies:
        raise ValueError(f"No study folders containing RDS tracks found under {collection}")
    return studies


def export_metadata(master: Path, collection: Path, *, replace: bool = False) -> dict:
    studies = study_folders(collection)
    constructors = _rds_constructors()
    constructors["data.frame"] = _sf_constructor
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Missing constructor for R class")
        frame = rdata.read_rds(master, constructor_dict=constructors)
    required = {"fileName", "MBid", "individual_id", "individual_local_identifier", "species"}
    if not isinstance(frame, pd.DataFrame) or not required <= set(frame.columns):
        raise ValueError("Master metadata is missing required columns")

    def scalar(value, name):
        if pd.isna(value):
            return None
        if name in {"individual_id", "MBid", "deployment_id"}:
            return str(int(value)) if isinstance(value, (int, float, np.number)) else str(value)
        if name in {"tracking_start_date", "tracking_end_date"}:
            return datetime.fromtimestamp(float(value), timezone.utc).isoformat().replace("+00:00", "Z")
        return value.item() if isinstance(value, np.generic) else value

    by_file = defaultdict(list)
    for raw in frame.to_dict("records"):
        row = {str(name): scalar(value, str(name)) for name, value in raw.items()}
        by_file[row["fileName"]].append(row)
    source = {"file_name": master.name, "sha256": hashlib.sha256(master.read_bytes()).hexdigest()}
    outputs, track_count, row_count = [], 0, 0
    for study in studies:
        individuals = {}
        for track in sorted(study.glob("*.rds")):
            match = SOURCE_NAME.fullmatch(track.name)
            if not match or match[1] != study.name:
                raise ValueError(f"Invalid source filename: {track}")
            rows = by_file.get(track.name)
            if not rows:
                raise ValueError(f"No metadata for {track}")
            study_id, individual_id = match.groups()
            if any(r["MBid"] != study_id or r["individual_id"] != individual_id for r in rows):
                raise ValueError(f"Metadata IDs disagree with {track}")
            def shared(key):
                values = {r.get(key) for r in rows if r.get(key) is not None}
                if key == "species" and len(values) > 1:
                    raise ValueError(f"Conflicting species for {track}")
                return next(iter(values)) if len(values) == 1 else None
            individuals[individual_id] = {
                "individual_id": individual_id, "source_file": track.name,
                "individual_local_identifier": shared("individual_local_identifier"),
                "species": shared("species"), "sex": shared("sex"), "deployments": rows,
            }
            track_count += 1
            row_count += len(rows)
        if individuals:
            payload = {"schema_version": 1, "study_id": study.name, "source": source,
                       "units": METADATA_UNITS, "individuals": individuals}
            contents = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
            target = study / "metadata.json"
            if target.exists() and target.read_text(encoding="utf-8") != contents and not replace:
                raise ValueError(f"Different metadata already exists: {target}; use --replace explicitly")
            outputs.append((target, contents))
    # Validate the complete collection before publishing any sidecar.
    for target, contents in outputs:
        if target.exists() and target.read_text(encoding="utf-8") == contents:
            continue
        temporary = target.with_suffix(".json.tmp")
        temporary.write_text(contents, encoding="utf-8")
        temporary.replace(target)
    return {"studies": len(outputs), "individuals": track_count, "metadata_rows": row_count,
            "source": source}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("master", type=Path)
    parser.add_argument("collection", type=Path)
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    print(json.dumps(export_metadata(args.master, args.collection, replace=args.replace), indent=2))
