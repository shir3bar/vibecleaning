"""Optional, portable study descriptions, independent of fix/review identities."""

from __future__ import annotations

from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3


METADATA_NAME = "metadata.json"
METADATA_ATTACHMENT = "movement_metadata_snapshot.json"
METADATA_UNITS = {
    "animal_mass": "g", "tracking_duration_days": "d",
    "median_timelag_mins": "min", "min_timelag_mins": "min",
}
SOURCE_NAME = re.compile(r"^(\d+)_(\d+)(?:_cleaned)?\.rds$", re.IGNORECASE)


def _reject_json_constant(value):
    raise ValueError(f"invalid JSON number {value}")


def read_study_metadata(study_dir: Path) -> tuple[dict | None, list[str], str]:
    path = study_dir / METADATA_NAME
    if not path.exists():
        return None, [], ""
    try:
        raw = path.read_bytes()
        data = json.loads(raw, parse_constant=_reject_json_constant)
        if not isinstance(data, dict) or data.get("schema_version") != 1:
            raise ValueError("unsupported metadata schema")
        study_id = data.get("study_id")
        if not isinstance(study_id, str) or not study_id.isdecimal():
            raise ValueError("invalid study ID")
        if study_dir.name.isdecimal() and study_id != study_dir.name:
            raise ValueError("metadata belongs to a different study")
        records = data.get("individuals")
        if not isinstance(records, dict):
            raise ValueError("individuals must be an object keyed by numeric individual ID")
        for individual_id, record in records.items():
            if not isinstance(record, dict) or record.get("individual_id") != individual_id:
                raise ValueError("invalid individual record")
            match = SOURCE_NAME.fullmatch(str(record.get("source_file", "")))
            if not match or match.groups() != (study_id, individual_id):
                raise ValueError("source filename does not match metadata IDs")
            if not isinstance(record.get("deployments"), list) or not all(
                isinstance(row, dict) for row in record["deployments"]
            ):
                raise ValueError("invalid deployment records")
            for key in ("species", "sex", "individual_local_identifier"):
                if record.get(key) is not None and not isinstance(record[key], str):
                    raise ValueError(f"invalid {key}")
        return data, [], hashlib.sha256(raw).hexdigest()
    except (OSError, ValueError, TypeError, UnicodeError) as exc:
        return None, [f"Study metadata unavailable: {exc}"], ""


def metadata_catalog_entry(study_dir: Path) -> dict:
    data, warnings, signature = read_study_metadata(study_dir)
    species = sorted({r["species"] for r in (data or {}).get("individuals", {}).values() if r.get("species")})
    return {"species": species, "metadata_warnings": warnings, "metadata_signature": signature}


def resolve_index_metadata(study_dir: Path, index_path: Path) -> dict:
    data, warnings, signature = read_study_metadata(study_dir)
    resolved, species = {}, {}
    with closing(sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute("SELECT logical_name, study_id, individual_id, individual_identifier, report_metadata FROM artifacts")
        for row in rows:
            name = str(row["individual_identifier"])
            embedded = json.loads(row["report_metadata"])
            if embedded.get("species"):
                species[name] = embedded["species"]
            if data is None:
                continue
            record = data["individuals"].get(str(row["individual_id"]))
            if record is None:
                continue
            match = SOURCE_NAME.fullmatch(str(row["logical_name"]))
            if (str(row["study_id"]) != data["study_id"] or not match
                    or match.groups() != (data["study_id"], str(row["individual_id"]))):
                warnings.append(f"Metadata IDs do not match {row['logical_name']}")
                continue
            resolved[name] = record
            if record.get("species"):
                species[name] = record["species"]
    return {
        "metadata_by_individual": resolved, "species_by_individual": species,
        "study_metadata": {"study_id": (data or {}).get("study_id", study_dir.name),
                           "species": sorted(set(species.values())),
                           "units": (data or {}).get("units", METADATA_UNITS),
                           "source": (data or {}).get("source")},
        "metadata_warnings": warnings, "metadata_signature": signature,
    }


def metadata_export_attachment(study_dir: Path) -> list[dict]:
    data, _warnings, _signature = read_study_metadata(study_dir)
    return [] if data is None else [{
        "logical_name": METADATA_ATTACHMENT,
        "content": (json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8"),
        "content_type": "application/json",
    }]
