"""Audit a metadata collection and smoke-test real studies on temporary copies."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import struct
import sys
import tempfile

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.state import ensure_project_state
from examples.movement.rds_index import (
    RDS_REVIEW_COLUMNS, build_rds_binary_columns, build_rds_overview, ensure_rds_index,
    read_movement_rds, rds_burst_feature_rows, validate_movement_rds,
)
from examples.movement.rds_export import _compare_original_columns, write_reviewed_rds_python
from examples.movement.study_metadata import read_study_metadata, resolve_index_metadata


def checksum(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def validate_collection(collection: Path, smoke_ids: set[str]) -> dict:
    studies = sorted(p for p in collection.glob("*/movement_rds/*") if p.is_dir())
    source_hashes = {}
    counts, species, smoke_results = Counter(), set(), []
    annotators = {}
    for study in studies:
        document, warnings, _signature = read_study_metadata(study)
        if document is None or warnings:
            raise ValueError(f"{study}: {warnings or 'missing metadata'}")
        tracks = sorted(study.glob("*.rds"))
        if len(document["individuals"]) != len(tracks):
            raise ValueError(f"{study}: track/metadata count differs")
        for track in tracks:
            record = next((row for row in document["individuals"].values() if row["source_file"] == track.name), None)
            if record is None:
                raise ValueError(f"{track}: no metadata")
            source_hashes[str(track.relative_to(collection))] = checksum(track)
            counts["individuals"] += 1
            counts["metadata_rows"] += len(record["deployments"])
            counts["multiple_deployments"] += len(record["deployments"]) > 1
            if record.get("species"):
                species.add(record["species"])
        counts["studies"] += 1
        annotator = study.parent.parent.name
        annotators.setdefault(annotator, Counter()).update(studies=1, individuals=len(tracks))

    # Each annotator contributes its smallest study, in addition to requested studies.
    selected = {min((p for p in studies if p.parent.parent.name == annotator),
                    key=lambda p: sum(x.stat().st_size for x in p.glob("*.rds")))
                for annotator in annotators}
    selected.update(p for p in studies if p.name in smoke_ids)
    with tempfile.TemporaryDirectory(prefix="study-metadata-validation-") as temporary:
        root = Path(temporary)
        for source_study in sorted(selected):
            study = root / source_study.parent.parent.name / "movement_rds" / source_study.name
            study.mkdir(parents=True)
            for source in source_study.iterdir():
                if source.suffix == ".rds" or source.name == "metadata.json":
                    shutil.copy2(source, study / source.name)
            state = ensure_project_state(study)
            bundle, index = ensure_rds_index(study, state["current_dataset_id"], cache_root=root / "cache")
            overview = build_rds_overview(index, overview_fix_limit=0)
            metadata = resolve_index_metadata(study, index)
            if len(metadata["metadata_by_individual"]) != len(bundle.paths):
                raise ValueError(f"{study}: metadata not resolved for every indexed individual")
            binary = build_rds_binary_columns(index, bundle_signature=bundle.signature)
            length = struct.unpack("<I", binary[4:8])[0]
            header = json.loads(binary[8:8+length]); offset = ((8+length+7)//8)*8
            def array(name):
                item = header["arrays"][name]
                return np.frombuffer(binary, dtype=item["dtype"], count=item["length"], offset=offset+item["offset"])
            valid = array("coordinate_valid").astype(bool)
            starts, ends = array("line_source_indexes"), array("line_target_indexes")
            invalid_prefix = np.cumsum(~valid)
            if not (valid[starts].all() and valid[ends].all() and np.all(invalid_prefix[ends] == invalid_prefix[starts])):
                raise ValueError(f"{study}: map lines cross invalid coordinates")
            exports = []
            with sqlite3.connect(index) as connection:
                invalid_files = [row[0] for row in connection.execute(
                    "SELECT DISTINCT a.logical_name FROM fixes f JOIN artifacts a USING(artifact_id) WHERE coordinate_valid=0")]
            for name in invalid_files:
                source = study / name
                frame = read_movement_rds(source)
                validate_movement_rds(source, frame)
                output = root / "exports" / name
                columns = {key: [None]*len(frame) for key in RDS_REVIEW_COLUMNS}
                write_reviewed_rds_python(source, output, columns)
                _compare_original_columns(source, output, columns)
                exported = read_movement_rds(output)
                bad_count = int(((exported.x_.abs() > 180) | (exported.y_.abs() > 90)).sum())
                exports.append({"file": name, "rows": len(exported), "invalid_positions_preserved": bad_count})
            unavailable = sum(bool(row.get("spatial_summary_unavailable")) for row in rds_burst_feature_rows(index))
            result = {"annotator": source_study.parent.parent.name, "study_id": source_study.name,
                "individuals": len(bundle.paths), "rows": overview["total_rows"],
                "invalid_coordinate_count": overview["invalid_coordinate_count"],
                "unavailable_burst_spatial_summaries": unavailable, "affected_track_exports": exports}
            smoke_results.append(result)
            print(json.dumps(result), flush=True)
    for relative, digest in source_hashes.items():
        if checksum(collection / relative) != digest:
            raise ValueError(f"Source changed during validation: {relative}")
    return {"validated_at": datetime.now(timezone.utc).isoformat(), "collection": str(collection.resolve()),
        **counts, "species": len(species), "annotators": annotators,
        "smoke_tests": smoke_results, "source_files_unchanged": True,
        "source_sha256": source_hashes}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collection", type=Path)
    parser.add_argument("--smoke-study", action="append", default=[])
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    result = validate_collection(args.collection, set(args.smoke_study))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items() if key != "source_sha256"}, indent=2))
