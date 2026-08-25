"""RDS-backed movement source and disposable SQLite working index.

The RDS files remain the lineage inputs.  This module only creates a cache that
can be removed and rebuilt at any time.  Rows retain their source artifact and
one-based source row so review scopes never depend on SQLite ordinals.
"""

from __future__ import annotations

from collections import defaultdict
from contextlib import closing
import copy
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import io
import json
from math import isclose
import os
from pathlib import Path
import shutil
import sqlite3
import struct
import tempfile
from typing import Iterable, Iterator, Sequence
import warnings

import numpy as np
import pandas as pd
import rdata
from rdata.conversion import DEFAULT_CLASS_MAP, dataframe_constructor

from app.state import ProjectStateError, load_dataset, resolve_artifact_path

from .movement_features import (
    compute_track_movement,
    geodesic_distance_meters,
    step_movement_metrics,
)
from .summary import DERIVED_FIELDS, quantile, span_to_zoom


RDS_INDEX_SCHEMA_VERSION = 3
RDS_SOURCE_FORMAT = "rds"
RDS_IMPLICIT_SET = "train"
RDS_REQUIRED_COLUMNS = {
    "x_",
    "y_",
    "t_",
    "individual_local_identifier",
    "individual_id",
    "study_id",
    "timestamp",
    "burst_",
    "geometry",
    "is_outlier",
}
RDS_REVIEW_COLUMNS = (
    "outlier_status",
    "outlier_issue_type",
    "outlier_comments",
    "outlier_flag_step_ids",
)
RDS_COLOR_FIELDS = [
    *DERIVED_FIELDS,
    {
        "key": "is_outlier",
        "label": "is_outlier",
        "kind": "boolean",
        "source": "raw",
        "column_name": "is_outlier",
    },
]


@dataclass(frozen=True)
class RdsBundle:
    study_dir: Path
    dataset_id: str
    artifacts: tuple[dict, ...]
    paths: tuple[Path, ...]
    signature: str


def is_rds_artifact(artifact: dict) -> bool:
    return str(artifact.get("logical_name") or "").lower().endswith(".rds")


def list_rds_individuals(index_path: Path) -> list[str]:
    with closing(_connect(index_path)) as connection:
        return [
            str(row["identifier"])
            for row in connection.execute(
                "SELECT identifier FROM individuals ORDER BY identifier"
            )
        ]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@lru_cache(maxsize=512)
def _sha256_file_stat(path_text: str, size: int, modified_ns: int) -> str:
    del size, modified_ns
    return _sha256_file(Path(path_text))


def _artifact_content_signature(path: Path) -> str:
    stat = path.stat()
    return _sha256_file_stat(str(path.resolve()), stat.st_size, stat.st_mtime_ns)


def load_rds_bundle(study_dir: Path, dataset_id: str) -> RdsBundle:
    dataset = load_dataset(study_dir, dataset_id)
    artifacts = tuple(
        sorted(
            (dict(item) for item in dataset.get("artifacts") or [] if is_rds_artifact(item)),
            key=lambda item: str(item.get("logical_name") or ""),
        )
    )
    if not artifacts:
        raise ProjectStateError("Selected dataset has no RDS movement artifacts")
    paths = tuple(resolve_artifact_path(study_dir, artifact) for artifact in artifacts)
    digest = hashlib.sha256()
    digest.update(f"rds-index-schema:{RDS_INDEX_SCHEMA_VERSION}\n".encode())
    for artifact, path in zip(artifacts, paths, strict=True):
        logical_name = str(artifact.get("logical_name") or "")
        digest.update(logical_name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(_artifact_content_signature(path).encode("ascii"))
        digest.update(b"\n")
    return RdsBundle(
        study_dir=study_dir.resolve(),
        dataset_id=dataset_id,
        artifacts=artifacts,
        paths=paths,
        signature=digest.hexdigest(),
    )


def _integer64_constructor(obj, _attrs):
    values = np.asarray(obj)
    if values.dtype != np.float64:
        raise ValueError("R integer64 column was not stored as a double vector")
    return values.view(np.int64)


def _sf_constructor(obj, attrs):
    # rdata's generic data-frame constructor coerces every integer ndarray to
    # pandas Int32.  That is correct for native R integer vectors, but not for
    # bit64::integer64 identifiers, whose constructor above deliberately
    # returns int64.  Wrapping only those arrays before the generic constructor
    # prevents the narrowing cast while retaining its factor/date handling.
    safe_obj = {
        key: pd.array(value, dtype="Int64")
        if isinstance(value, np.ndarray) and value.dtype == np.dtype("int64")
        else value
        for key, value in obj.items()
    }
    frame = dataframe_constructor(safe_obj, attrs)
    frame.attrs.update(attrs)
    return frame


def _rds_constructors() -> dict:
    constructors = dict(DEFAULT_CLASS_MAP)
    constructors["integer64"] = _integer64_constructor
    constructors["sf"] = _sf_constructor
    return constructors


def read_movement_rds(path: Path) -> pd.DataFrame:
    """Read the move2/sf frame while preserving top-level attributes."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Missing constructor for R class")
        frame = rdata.read_rds(path, constructor_dict=_rds_constructors())
    if not isinstance(frame, pd.DataFrame):
        raise ValueError(f"{path.name} does not contain an R data.frame")
    return frame


def _scalar_text(value: object) -> str:
    if value is None or value is pd.NA:
        return ""
    if isinstance(value, float) and np.isnan(value):
        return ""
    return str(value).strip()


def _single_string(values: pd.Series, *, label: str, filename: str) -> str:
    present = {_scalar_text(value) for value in values.tolist() if _scalar_text(value)}
    if len(present) != 1:
        raise ValueError(f"{filename} must contain exactly one {label}")
    return next(iter(present))


def _single_integer(values: pd.Series, *, label: str, filename: str) -> str:
    present: set[int] = set()
    for value in values.tolist():
        if value is None or value is pd.NA:
            continue
        try:
            present.add(int(value))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{filename} has an invalid {label}") from exc
    if len(present) != 1:
        raise ValueError(f"{filename} must contain exactly one {label}")
    return str(next(iter(present)))


def _attr_string(frame: pd.DataFrame, name: str) -> str:
    value = frame.attrs.get(name)
    if isinstance(value, np.ndarray):
        return _scalar_text(value[0]) if len(value) else ""
    return _scalar_text(value)


def _validate_crs(frame: pd.DataFrame, *, filename: str) -> None:
    crs = frame.attrs.get("crs_")
    crs_input = ""
    if isinstance(crs, dict):
        for key, value in crs.items():
            if str(key) == "input":
                if isinstance(value, np.ndarray) and len(value):
                    crs_input = _scalar_text(value[0])
                else:
                    crs_input = _scalar_text(value)
                break
    normalized = crs_input.upper().replace(" ", "")
    if normalized not in {"EPSG:4326", "WGS84"}:
        raise ValueError(f"{filename} must use EPSG:4326 coordinates")


def validate_movement_rds(path: Path, frame: pd.DataFrame) -> dict[str, str | int]:
    filename = path.name
    missing = sorted(RDS_REQUIRED_COLUMNS - set(map(str, frame.columns)))
    if missing:
        raise ValueError(f"{filename} is missing required columns: {', '.join(missing)}")
    if frame.empty:
        raise ValueError(f"{filename} contains no fixes")
    classes = {_scalar_text(value) for value in np.asarray(frame.attrs.get("class", []))}
    if not {"sf", "move2"}.issubset(classes):
        raise ValueError(f"{filename} is not an sf/move2 object")
    if _attr_string(frame, "sf_column") != "geometry":
        raise ValueError(f"{filename} does not identify geometry as its sf column")
    if _attr_string(frame, "time_column") != "t_":
        raise ValueError(f"{filename} does not identify t_ as its time column")
    if _attr_string(frame, "track_id_column") != "individual_local_identifier":
        raise ValueError(
            f"{filename} does not identify individual_local_identifier as its track column"
        )
    _validate_crs(frame, filename=filename)

    study_id = _single_integer(frame["study_id"], label="study_id", filename=filename)
    individual_id = _single_integer(
        frame["individual_id"], label="individual_id", filename=filename
    )
    individual = _single_string(
        frame["individual_local_identifier"],
        label="individual_local_identifier",
        filename=filename,
    )
    expected_stem = f"{study_id}_{individual_id}"
    if path.stem != expected_stem:
        raise ValueError(
            f"{filename} does not match embedded identifiers; expected {expected_stem}.rds"
        )

    x_values = pd.to_numeric(frame["x_"], errors="coerce").to_numpy(dtype=np.float64)
    y_values = pd.to_numeric(frame["y_"], errors="coerce").to_numpy(dtype=np.float64)
    t_values = pd.to_numeric(frame["t_"], errors="coerce").to_numpy(dtype=np.float64)
    timestamps = pd.to_numeric(frame["timestamp"], errors="coerce").to_numpy(dtype=np.float64)
    if not (
        np.isfinite(x_values).all()
        and np.isfinite(y_values).all()
        and np.isfinite(t_values).all()
        and np.isfinite(timestamps).all()
    ):
        raise ValueError(f"{filename} contains missing or non-finite coordinates/times")
    if ((x_values < -180) | (x_values > 180) | (y_values < -90) | (y_values > 90)).any():
        raise ValueError(f"{filename} contains invalid longitude/latitude values")
    if not np.allclose(t_values, timestamps, rtol=0.0, atol=1e-6):
        raise ValueError(f"{filename} has inconsistent t_ and timestamp values")

    geometry = frame["geometry"].tolist()
    for row_number, (lon, lat, point) in enumerate(
        zip(x_values, y_values, geometry, strict=True), start=1
    ):
        point_values = np.asarray(point, dtype=np.float64).reshape(-1)
        if len(point_values) < 2 or not (
            isclose(float(point_values[0]), float(lon), abs_tol=1e-9)
            and isclose(float(point_values[1]), float(lat), abs_tol=1e-9)
        ):
            raise ValueError(f"{filename} row {row_number} has inconsistent geometry")

    burst_values = pd.to_numeric(frame["burst_"], errors="coerce").to_numpy(dtype=np.float64)
    if not np.isfinite(burst_values).all() or not np.equal(burst_values, np.floor(burst_values)).all():
        raise ValueError(f"{filename} contains invalid burst_ values")
    outliers = frame["is_outlier"]
    if outliers.isna().any():
        raise ValueError(f"{filename} contains missing is_outlier values")
    normalized_outliers = {_scalar_text(value).lower() for value in outliers.tolist()}
    if not normalized_outliers.issubset({"true", "false", "1", "0"}):
        raise ValueError(f"{filename} contains non-boolean is_outlier values")

    return {
        "study_id": study_id,
        "individual_id": individual_id,
        "individual": individual,
        "row_count": int(len(frame)),
    }


def _schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE artifacts (
            artifact_id INTEGER PRIMARY KEY,
            logical_name TEXT NOT NULL UNIQUE,
            sha256 TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            study_id TEXT NOT NULL,
            individual_identifier TEXT NOT NULL,
            individual_id TEXT NOT NULL
        );
        CREATE TABLE individuals (
            individual_key INTEGER PRIMARY KEY,
            identifier TEXT NOT NULL UNIQUE,
            individual_id TEXT NOT NULL UNIQUE,
            study_id TEXT NOT NULL,
            artifact_id INTEGER NOT NULL UNIQUE REFERENCES artifacts(artifact_id),
            row_count INTEGER NOT NULL,
            start_ms INTEGER NOT NULL,
            end_ms INTEGER NOT NULL,
            min_lon REAL NOT NULL,
            max_lon REAL NOT NULL,
            min_lat REAL NOT NULL,
            max_lat REAL NOT NULL
        );
        CREATE TABLE fixes (
            ordinal INTEGER PRIMARY KEY,
            artifact_id INTEGER NOT NULL REFERENCES artifacts(artifact_id),
            source_row INTEGER NOT NULL,
            fix_key TEXT NOT NULL,
            individual_key INTEGER NOT NULL REFERENCES individuals(individual_key),
            time_ms INTEGER NOT NULL,
            lon REAL NOT NULL,
            lat REAL NOT NULL,
            burst_value INTEGER NOT NULL,
            is_outlier INTEGER NOT NULL CHECK (is_outlier IN (0, 1)),
            tag_identifier TEXT NOT NULL,
            step_length_m REAL,
            speed_mps REAL,
            time_delta_s REAL,
            turn_angle_deg REAL,
            source_outlier_status TEXT NOT NULL DEFAULT '',
            source_outlier_issue_type TEXT NOT NULL DEFAULT '',
            source_outlier_comments TEXT NOT NULL DEFAULT '',
            source_outlier_flag_step_ids TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX fixes_individual_time ON fixes(individual_key, time_ms, source_row);
        """
    )


def _source_review_value(frame: pd.DataFrame, column: str, index: int) -> str:
    if column not in frame.columns:
        return ""
    return _scalar_text(frame.iloc[index][column])


def _insert_frame(
    connection: sqlite3.Connection,
    *,
    logical_name: str,
    path: Path,
    frame: pd.DataFrame,
    info: dict[str, str | int],
    artifact_id: int,
    ordinal_start: int,
) -> int:
    row_count = int(info["row_count"])
    sha256 = _sha256_file(path)
    connection.execute(
        "INSERT INTO artifacts VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            artifact_id,
            logical_name,
            sha256,
            row_count,
            info["study_id"],
            info["individual"],
            info["individual_id"],
        ),
    )
    x_values = pd.to_numeric(frame["x_"], errors="raise").to_numpy(dtype=np.float64)
    y_values = pd.to_numeric(frame["y_"], errors="raise").to_numpy(dtype=np.float64)
    t_values = pd.to_numeric(frame["t_"], errors="raise").to_numpy(dtype=np.float64)
    time_ms_values = np.rint(t_values * 1000.0).astype(np.int64)
    burst_values = pd.to_numeric(frame["burst_"], errors="raise").to_numpy(dtype=np.int64)
    outlier_values = frame["is_outlier"].astype(bool).to_numpy(dtype=np.bool_)
    tags = (
        frame["tag_local_identifier"].tolist()
        if "tag_local_identifier" in frame.columns
        else [""] * row_count
    )
    individual_key = artifact_id
    connection.execute(
        "INSERT INTO individuals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            individual_key,
            info["individual"],
            info["individual_id"],
            info["study_id"],
            artifact_id,
            row_count,
            int(time_ms_values.min()),
            int(time_ms_values.max()),
            float(x_values.min()),
            float(x_values.max()),
            float(y_values.min()),
            float(y_values.max()),
        ),
    )
    rows = []
    for zero_index in range(row_count):
        source_row = zero_index + 1
        ordinal = ordinal_start + zero_index
        fix_key = f"file:{logical_name}#row:{source_row}"
        rows.append(
            (
                ordinal,
                artifact_id,
                source_row,
                fix_key,
                individual_key,
                int(time_ms_values[zero_index]),
                float(x_values[zero_index]),
                float(y_values[zero_index]),
                int(burst_values[zero_index]),
                int(bool(outlier_values[zero_index])),
                _scalar_text(tags[zero_index]),
                _source_review_value(frame, "outlier_status", zero_index),
                _source_review_value(frame, "outlier_issue_type", zero_index),
                _source_review_value(frame, "outlier_comments", zero_index),
                _source_review_value(frame, "outlier_flag_step_ids", zero_index),
            )
        )
    connection.executemany(
        """
        INSERT INTO fixes (
            ordinal, artifact_id, source_row, fix_key, individual_key,
            time_ms, lon, lat, burst_value, is_outlier, tag_identifier,
            source_outlier_status, source_outlier_issue_type,
            source_outlier_comments, source_outlier_flag_step_ids
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return ordinal_start + row_count


def _compute_derived_fields(connection: sqlite3.Connection) -> None:
    individual_rows = connection.execute(
        "SELECT individual_key, identifier FROM individuals ORDER BY individual_key"
    ).fetchall()
    updates = []
    for individual_key, identifier in individual_rows:
        raw_rows = connection.execute(
            """
            SELECT ordinal, source_row, fix_key, time_ms, lon, lat
            FROM fixes WHERE individual_key = ?
            ORDER BY time_ms, source_row
            """,
            (individual_key,),
        ).fetchall()
        records = [
            {
                "ordinal": row[0],
                "row_index": row[1],
                "fix_key": row[2],
                "individual": identifier,
                "set_name": RDS_IMPLICIT_SET,
                "time_ms": row[3],
                "lon": row[4],
                "lat": row[5],
            }
            for row in raw_rows
        ]
        movement, _ = compute_track_movement({(identifier, RDS_IMPLICIT_SET): records})
        for record in records:
            values = movement[record["fix_key"]]
            updates.append(
                (
                    values["step_length_m"],
                    values["speed_mps"],
                    values["time_delta_s"],
                    values["turn_angle_deg"],
                    record["ordinal"],
                )
            )
    connection.executemany(
        """
        UPDATE fixes SET step_length_m=?, speed_mps=?, time_delta_s=?, turn_angle_deg=?
        WHERE ordinal=?
        """,
        updates,
    )


def build_rds_index(bundle: RdsBundle, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent
    )
    os.close(fd)
    temporary_path = Path(temporary_name)
    try:
        with closing(sqlite3.connect(temporary_path)) as connection:
            _schema(connection)
            ordinal = 0
            study_ids: set[str] = set()
            individual_ids: set[str] = set()
            identifiers: set[str] = set()
            for artifact_id, (artifact, path) in enumerate(
                zip(bundle.artifacts, bundle.paths, strict=True), start=1
            ):
                logical_name = str(artifact.get("logical_name") or "")
                frame = read_movement_rds(path)
                info = validate_movement_rds(path, frame)
                study_ids.add(str(info["study_id"]))
                if str(info["individual_id"]) in individual_ids:
                    raise ValueError(f"Duplicate individual_id {info['individual_id']} in RDS bundle")
                if str(info["individual"]) in identifiers:
                    raise ValueError(
                        f"Duplicate individual_local_identifier {info['individual']} in RDS bundle"
                    )
                individual_ids.add(str(info["individual_id"]))
                identifiers.add(str(info["individual"]))
                ordinal = _insert_frame(
                    connection,
                    logical_name=logical_name,
                    path=path,
                    frame=frame,
                    info=info,
                    artifact_id=artifact_id,
                    ordinal_start=ordinal,
                )
            if len(study_ids) != 1:
                raise ValueError("RDS bundle must contain exactly one study_id")
            _compute_derived_fields(connection)
            meta = {
                "schema_version": str(RDS_INDEX_SCHEMA_VERSION),
                "bundle_signature": bundle.signature,
                "study_id": next(iter(study_ids)),
                "fix_count": str(ordinal),
                "artifact_count": str(len(bundle.artifacts)),
            }
            connection.executemany("INSERT INTO meta VALUES (?, ?)", meta.items())
            connection.commit()
            check = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if check != "ok":
                raise ValueError(f"SQLite integrity check failed: {check}")
        os.replace(temporary_path, output_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def rds_index_path(bundle: RdsBundle) -> Path:
    return (
        bundle.study_dir
        / ".vibecleaning"
        / "cache"
        / "movement"
        / f"{bundle.signature}.sqlite"
    )


def _index_matches(path: Path, signature: str) -> bool:
    if not path.exists():
        return False
    try:
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as connection:
            values = dict(connection.execute("SELECT key, value FROM meta"))
            return (
                values.get("bundle_signature") == signature
                and values.get("schema_version") == str(RDS_INDEX_SCHEMA_VERSION)
            )
    except sqlite3.Error:
        return False


def ensure_rds_index(study_dir: Path, dataset_id: str) -> tuple[RdsBundle, Path]:
    bundle = load_rds_bundle(study_dir, dataset_id)
    path = rds_index_path(bundle)
    if not _index_matches(path, bundle.signature):
        build_rds_index(bundle, path)
    for stale_path in path.parent.glob("*.sqlite"):
        if stale_path != path:
            stale_path.unlink(missing_ok=True)
    return bundle, path


def _row_review(row: sqlite3.Row) -> dict:
    status = str(row["source_outlier_status"] or "").strip().lower()
    if status not in {"suspected", "confirmed"}:
        return {}
    result = {
        "status": status,
        "issue_type": str(row["source_outlier_issue_type"] or "").strip(),
        "comments": str(row["source_outlier_comments"] or "").strip(),
    }
    return {key: value for key, value in result.items() if value}


def _fix_from_row(row: sqlite3.Row) -> dict:
    attributes = {
        "step_length_m": row["step_length_m"],
        "speed_mps": row["speed_mps"],
        "time_delta_s": row["time_delta_s"],
        "turn_angle_deg": row["turn_angle_deg"],
        "is_outlier": bool(row["is_outlier"]),
    }
    return {
        "fix_key": str(row["fix_key"]),
        "source_artifact": str(row["logical_name"]),
        "source_row": int(row["source_row"]),
        "individual": str(row["identifier"]),
        "individual_id": str(row["individual_id"]),
        "set": RDS_IMPLICIT_SET,
        "time_ms": int(row["time_ms"]),
        "lon": float(row["lon"]),
        "lat": float(row["lat"]),
        "attributes": attributes,
        "review": _row_review(row),
        "segments": [],
        "source_flags": [],
        "analytically_excluded": False,
        "source_burst": int(row["burst_value"]),
    }


FIX_SELECT = """
SELECT f.*, a.logical_name, i.identifier, i.individual_id, i.study_id
FROM fixes f
JOIN artifacts a ON a.artifact_id=f.artifact_id
JOIN individuals i ON i.individual_key=f.individual_key
"""


def _source_bursts(rows: Sequence[sqlite3.Row]) -> list[dict]:
    grouped: dict[tuple[str, int], list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["identifier"]), int(row["burst_value"]))].append(row)
    bursts = []
    for (individual, burst_value), group in grouped.items():
        ordered = sorted(group, key=lambda row: (int(row["time_ms"]), int(row["source_row"])))
        path = [[float(row["lon"]), float(row["lat"])] for row in ordered]
        step_lengths = [
            geodesic_distance_meters(*path[index - 1], *path[index])
            for index in range(1, len(path))
        ]
        bursts.append(
            {
                "burst_id": f"{individual}:{RDS_IMPLICIT_SET}:source_{burst_value}",
                "burst_idx": int(burst_value),
                "source_burst": int(burst_value),
                "individual": individual,
                "set_name": RDS_IMPLICIT_SET,
                "start_fix_key": str(ordered[0]["fix_key"]),
                "end_fix_key": str(ordered[-1]["fix_key"]),
                "start_time_ms": int(ordered[0]["time_ms"]),
                "end_time_ms": int(ordered[-1]["time_ms"]),
                "fix_count": len(ordered),
                "burst_gap_seconds": 0.0,
                "fix_keys": [str(row["fix_key"]) for row in ordered],
                "path": path,
                "path_length_m": float(sum(step_lengths)),
                "median_step_m": float(np.median(step_lengths)) if step_lengths else None,
                "is_outlier_count": sum(int(row["is_outlier"]) for row in ordered),
            }
        )
    return sorted(
        bursts,
        key=lambda item: (
            item["individual"], item["start_time_ms"], item["burst_idx"]
        ),
    )


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def build_rds_fixes(
    index_path: Path,
    *,
    individuals: Iterable[str] | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
    review_status: str = "",
    limit: int | None = None,
    confirmed_fix_keys: Iterable[str] | None = None,
    confirmed_individual_tracks: Iterable[tuple[str, str]] | None = None,
    annotations: list[dict] | None = None,
) -> dict:
    clauses = []
    values: list[object] = []
    selected = [str(item).strip() for item in individuals or [] if str(item).strip()]
    if selected:
        placeholders = ",".join("?" for _ in selected)
        clauses.append(f"i.identifier IN ({placeholders})")
        values.extend(selected)
    normalized_status = str(review_status or "").strip().lower()
    query = FIX_SELECT
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY i.identifier, f.time_ms, f.source_row"
    with closing(_connect(index_path)) as connection:
        rows = connection.execute(query, values).fetchall()
    confirmed_keys = {str(item) for item in confirmed_fix_keys or []}
    confirmed_individuals = {
        str(item[0])
        for item in confirmed_individual_tracks or []
        if isinstance(item, (list, tuple)) and item
    }
    fixes = [_fix_from_row(row) for row in rows]
    source_status = np.array([
        2 if str(row["source_outlier_status"] or "").lower() == "confirmed"
        else 1 if str(row["source_outlier_status"] or "").lower() == "suspected"
        else 0
        for row in rows
    ], dtype=np.uint8)
    artifact_rows: dict[str, np.ndarray] = {}
    for index, row in enumerate(rows):
        logical_name = str(row["logical_name"])
        source_row = int(row["source_row"])
        inverse = artifact_rows.get(logical_name)
        if inverse is None:
            inverse = np.full(source_row + 1, -1, dtype=np.int64)
            artifact_rows[logical_name] = inverse
        elif source_row >= len(inverse):
            expanded = np.full(source_row + 1, -1, dtype=np.int64)
            expanded[: len(inverse)] = inverse
            inverse = expanded
            artifact_rows[logical_name] = inverse
        inverse[source_row] = index
    projected_status = _review_projection(
        source_status=source_status,
        annotations=list(annotations or []),
        artifact_rows=artifact_rows,
    )
    records_by_group: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for index, fix in enumerate(fixes):
        excluded = (
            int(projected_status[index]) == 2
            or
            fix["fix_key"] in confirmed_keys
            or fix["individual"] in confirmed_individuals
        )
        fix["analytically_excluded"] = excluded
        if excluded:
            continue
        records_by_group[(fix["individual"], RDS_IMPLICIT_SET)].append({
            "row_index": int(fix["source_row"]),
            "fix_key": fix["fix_key"],
            "individual": fix["individual"],
            "set_name": RDS_IMPLICIT_SET,
            "time_ms": fix["time_ms"],
            "lon": fix["lon"],
            "lat": fix["lat"],
        })
    movement, _stats = compute_track_movement(records_by_group)
    for fix in fixes:
        values_for_fix = movement.get(fix["fix_key"])
        if values_for_fix is not None:
            fix["attributes"].update(values_for_fix)
    visible_pairs = [
        (row, fix)
        for row, fix in zip(rows, fixes, strict=True)
        if (start_ms is None or fix["time_ms"] >= int(start_ms))
        and (end_ms is None or fix["time_ms"] <= int(end_ms))
    ]
    matching = len(visible_pairs)
    returned_pairs = visible_pairs if limit is None else visible_pairs[: max(0, int(limit))]
    returned_rows = [row for row, _fix in returned_pairs]
    returned_fixes = [fix for _row, fix in returned_pairs]
    eligible_burst_rows = [
        row
        for row, fix in visible_pairs
        if not fix["analytically_excluded"]
    ]
    return {
        "fixes": returned_fixes,
        "segments": [],
        "auto_bursts": _source_bursts(eligible_burst_rows),
        "matching_fix_count": matching,
        "returned_fix_count": len(returned_rows),
        "truncated": len(returned_rows) < matching,
        "burst_source": "burst_",
        "detail_scope": {
            "individual": selected[0] if len(selected) == 1 else "",
            "individuals": selected,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "review_status": normalized_status,
            "limit": limit,
            "burst_source": "burst_",
        },
        "detail_loaded": True,
    }


def build_rds_overview(
    index_path: Path,
    *,
    overview_fix_limit: int = 25_000,
    max_series_points: int = 1_500,
) -> dict:
    with closing(_connect(index_path)) as connection:
        individuals = connection.execute(
            "SELECT * FROM individuals ORDER BY identifier"
        ).fetchall()
        aggregate = connection.execute(
            """
            SELECT COUNT(*) count, MIN(time_ms) min_time, MAX(time_ms) max_time,
                   MIN(lon) min_lon, MAX(lon) max_lon,
                   MIN(lat) min_lat, MAX(lat) max_lat
            FROM fixes
            """
        ).fetchone()
        review_counts = dict(
            connection.execute(
                """
                SELECT lower(source_outlier_status), COUNT(*) FROM fixes
                WHERE lower(source_outlier_status) IN ('suspected','confirmed')
                GROUP BY lower(source_outlier_status)
                """
            ).fetchall()
        )
        limit = max(0, int(overview_fix_limit))
        preview_rows = connection.execute(
            FIX_SELECT + " ORDER BY i.identifier, f.time_ms, f.source_row LIMIT ?",
            (limit,),
        ).fetchall() if limit else []
        total_rows = int(aggregate["count"])
        if not total_rows:
            raise ValueError("RDS bundle did not contain movement fixes")
        series_by_individual = {}
        coverage_by_individual = {}
        stats = {}
        for individual_row in individuals:
            identifier = str(individual_row["identifier"])
            rows = connection.execute(
                """
                SELECT time_ms, lon, lat, step_length_m, speed_mps, time_delta_s,
                       source_outlier_status, is_outlier
                FROM fixes WHERE individual_key=? ORDER BY time_ms, source_row
                """,
                (int(individual_row["individual_key"]),),
            ).fetchall()
            sample_count = min(max(1, int(max_series_points)), len(rows))
            sample_indexes = (
                np.linspace(0, len(rows) - 1, sample_count, dtype=np.int64).tolist()
                if rows else []
            )
            samples = [rows[index] for index in sample_indexes]
            series_by_individual[identifier] = {
                RDS_IMPLICIT_SET: {
                    "times": [int(row["time_ms"]) for row in samples],
                    "positions": [[float(row["lon"]), float(row["lat"])] for row in samples],
                }
            }
            coverage_by_individual[identifier] = {
                RDS_IMPLICIT_SET: {
                    "start_ms": int(individual_row["start_ms"]),
                    "end_ms": int(individual_row["end_ms"]),
                }
            }
            steps = [float(row["step_length_m"]) for row in rows if row["step_length_m"] is not None]
            speeds = [float(row["speed_mps"]) for row in rows if row["speed_mps"] is not None]
            intervals = [float(row["time_delta_s"]) for row in rows if row["time_delta_s"] is not None]
            stats[identifier] = {
                "row_count": int(individual_row["row_count"]),
                "median_fix_s": float(np.median(intervals)) if intervals else None,
                "median_step_m": float(np.median(steps)) if steps else None,
                "median_speed_mps": float(np.median(speeds)) if speeds else None,
                "p95_step_m": quantile(steps, 0.95),
                "p95_speed_mps": quantile(speeds, 0.95),
                "suspected_count": sum(
                    str(row["source_outlier_status"]).lower() == "suspected" for row in rows
                ),
                "confirmed_count": sum(
                    str(row["source_outlier_status"]).lower() == "confirmed" for row in rows
                ),
                "source_outlier_count": sum(int(row["is_outlier"]) for row in rows),
            }
        truncated = len(preview_rows) < total_rows
        all_rows = connection.execute(
            FIX_SELECT + " ORDER BY i.identifier, f.time_ms, f.source_row"
        ).fetchall() if not truncated else []
    span = max(
        float(aggregate["max_lon"] - aggregate["min_lon"]),
        float(aggregate["max_lat"] - aggregate["min_lat"]),
    )
    return {
        "source_format": RDS_SOURCE_FORMAT,
        "total_rows": total_rows,
        "columns": {
            "individual": "individual_local_identifier",
            "time": "t_",
            "lon": "x_",
            "lat": "y_",
            "set": None,
            "fix_id": None,
            "burst": "burst_",
        },
        "individuals": [str(row["identifier"]) for row in individuals],
        "species_by_individual": {},
        "stats": stats,
        "coverage_by_individual": coverage_by_individual,
        "series_by_individual": series_by_individual,
        "color_fields": [dict(item) for item in RDS_COLOR_FIELDS],
        "review_counts": {
            "suspected": int(review_counts.get("suspected", 0)),
            "confirmed": int(review_counts.get("confirmed", 0)),
        },
        "fixes": [_fix_from_row(row) for row in preview_rows],
        "segments": [],
        "auto_bursts": [] if truncated else _source_bursts(all_rows),
        "auto_bursts_truncated": truncated,
        "overview_truncated": truncated,
        "overview_fix_limit": limit,
        "overview_series_point_limit": int(max_series_points),
        "burst_source": "burst_",
        "initial_view": {
            "longitude": float((aggregate["min_lon"] + aggregate["max_lon"]) / 2),
            "latitude": float((aggregate["min_lat"] + aggregate["max_lat"]) / 2),
            "zoom": float(span_to_zoom(span)),
        },
        "min_time_ms": int(aggregate["min_time"]),
        "max_time_ms": int(aggregate["max_time"]),
        "detail_scope": {
            "individual": "",
            "individuals": [],
            "start_ms": None,
            "end_ms": None,
            "review_status": "reviewed",
            "limit": None,
            "burst_source": "burst_",
        },
        "detail_loaded": False,
    }


def _index_review_status(
    connection: sqlite3.Connection,
    annotations: list[dict] | None,
) -> np.ndarray:
    count = int(connection.execute("SELECT COUNT(*) FROM fixes").fetchone()[0])
    artifacts = connection.execute(
        "SELECT artifact_id, logical_name, row_count FROM artifacts ORDER BY artifact_id"
    ).fetchall()
    source_status = np.zeros(count, dtype=np.uint8)
    artifact_rows = {
        str(row["logical_name"]): np.full(int(row["row_count"]) + 1, -1, dtype=np.int64)
        for row in artifacts
    }
    for row in connection.execute(
        """
        SELECT f.ordinal, f.source_row, f.source_outlier_status, a.logical_name
        FROM fixes f JOIN artifacts a ON a.artifact_id=f.artifact_id
        """
    ):
        ordinal = int(row["ordinal"])
        artifact_rows[str(row["logical_name"])][int(row["source_row"])] = ordinal
        status = str(row["source_outlier_status"] or "").lower()
        source_status[ordinal] = 2 if status == "confirmed" else 1 if status == "suspected" else 0
    return _review_projection(
        source_status=source_status,
        annotations=list(annotations or []),
        artifact_rows=artifact_rows,
    )


def source_outlier_ranking(
    index_path: Path,
    annotations: list[dict] | None = None,
) -> dict:
    with closing(_connect(index_path)) as connection:
        review_status = _index_review_status(connection, annotations)
        cursor = connection.execute(
            """
            SELECT f.ordinal, i.identifier individual, f.burst_value,
                   f.time_ms, f.is_outlier, a.logical_name
            FROM fixes f
            JOIN individuals i ON i.individual_key=f.individual_key
            JOIN artifacts a ON a.artifact_id=f.artifact_id
            ORDER BY i.identifier, f.burst_value, f.time_ms, f.source_row
            """
        )
        grouped: dict[tuple[str, int], dict] = {}
        for row in cursor:
            if review_status[int(row["ordinal"])] == 2:
                continue
            key = (str(row["individual"]), int(row["burst_value"]))
            item = grouped.setdefault(key, {
                "start_time_ms": int(row["time_ms"]),
                "end_time_ms": int(row["time_ms"]),
                "fix_count": 0,
                "is_outlier_count": 0,
                "logical_name": str(row["logical_name"]),
            })
            item["end_time_ms"] = int(row["time_ms"])
            item["fix_count"] += 1
            item["is_outlier_count"] += int(row["is_outlier"])
    scored = []
    for (individual, burst_value), item in sorted(grouped.items()):
        scored.append(
            {
                "individual": individual,
                "set_name": RDS_IMPLICIT_SET,
                "burst_id": f"{individual}:{RDS_IMPLICIT_SET}:source_{burst_value}",
                "burst_idx": burst_value,
                "start_time_ms": item["start_time_ms"],
                "end_time_ms": item["end_time_ms"],
                "fix_count": item["fix_count"],
                "anomaly_score": float(item["is_outlier_count"]),
                "is_outlier_count": item["is_outlier_count"],
                "source_artifact": item["logical_name"],
            }
        )
    return {
        "run_status": "completed" if scored else "unresolved",
        "ranking_method": "source_is_outlier",
        "scored_bursts": scored,
    }


def rds_burst_feature_rows(
    index_path: Path,
    annotations: list[dict] | None = None,
) -> list[dict]:
    """Build the existing movement-only burst features from authoritative source bursts."""
    with closing(_connect(index_path)) as connection:
        review_status = _index_review_status(connection, annotations)
        cursor = connection.execute(
            """
            SELECT f.ordinal, f.source_row, f.time_ms, f.lon, f.lat,
                   f.burst_value, a.logical_name, i.identifier
            FROM fixes f
            JOIN artifacts a ON a.artifact_id=f.artifact_id
            JOIN individuals i ON i.individual_key=f.individual_key
            ORDER BY i.identifier, f.burst_value, f.time_ms, f.source_row
            """
        )
        feature_rows = []
        current_key: tuple[str, int] | None = None
        group: list[sqlite3.Row] = []

        def append_group(rows: list[sqlite3.Row]) -> None:
            eligible_rows = [row for row in rows if review_status[int(row["ordinal"])] != 2]
            if not eligible_rows:
                return
            individual = str(eligible_rows[0]["identifier"])
            burst_value = int(eligible_rows[0]["burst_value"])
            step_lengths = []
            speeds = []
            time_gaps = []
            for previous, current in zip(eligible_rows, eligible_rows[1:]):
                metrics = step_movement_metrics(
                    int(previous["time_ms"]), float(previous["lon"]), float(previous["lat"]),
                    int(current["time_ms"]), float(current["lon"]), float(current["lat"]),
                )
                if metrics["step_length_m"] is not None:
                    step_lengths.append(float(metrics["step_length_m"]))
                if metrics["speed_mps"] is not None:
                    speeds.append(float(metrics["speed_mps"]))
                if metrics["time_delta_s"] is not None:
                    time_gaps.append(float(metrics["time_delta_s"]))
            first = eligible_rows[0]
            last = eligible_rows[-1]
            path_length = float(sum(step_lengths))
            net_displacement = (
                0.0 if len(eligible_rows) == 1 else geodesic_distance_meters(
                    float(first["lon"]), float(first["lat"]),
                    float(last["lon"]), float(last["lat"]),
                )
            )
            mean = lambda values: float(np.mean(values)) if values else None
            sd = lambda values: float(np.std(values)) if values else None
            feature_rows.append({
                "burst_id": f"{individual}:{RDS_IMPLICIT_SET}:source_{burst_value}",
                "individual": individual,
                "set_name": RDS_IMPLICIT_SET,
                "start_time_ms": int(first["time_ms"]),
                "end_time_ms": int(last["time_ms"]),
                "n_fixes": len(eligible_rows),
                "duration_s": float((int(last["time_ms"]) - int(first["time_ms"])) / 1000.0),
                "path_length_m": path_length,
                "mean_step_length_m": mean(step_lengths),
                "sd_step_length_m": sd(step_lengths),
                "net_displacement_m": float(net_displacement),
                "straightness": float(net_displacement / path_length) if path_length > 0 else None,
                "mean_speed_mps": mean(speeds),
                "median_speed_mps": float(np.median(speeds)) if speeds else None,
                "max_speed_mps": float(max(speeds)) if speeds else None,
                "sd_speed_mps": sd(speeds),
                "max_time_gap_s": float(max(time_gaps)) if time_gaps else None,
            })

        for row in cursor:
            key = (str(row["identifier"]), int(row["burst_value"]))
            if current_key is not None and key != current_key:
                append_group(group)
                group = []
            current_key = key
            group.append(row)
        if group:
            append_group(group)
    return feature_rows


def source_rows_from_fix_keys(fix_keys: Iterable[str]) -> list[dict]:
    grouped: dict[str, list[int]] = defaultdict(list)
    for raw_key in fix_keys:
        fix_key = str(raw_key or "").strip()
        if not fix_key.startswith("file:") or "#row:" not in fix_key:
            raise ValueError("Invalid RDS movement fix key")
        logical_name, raw_row = fix_key[5:].rsplit("#row:", 1)
        if not logical_name or Path(logical_name).name != logical_name:
            raise ValueError("Invalid RDS movement fix key")
        try:
            row_number = int(raw_row)
        except ValueError as exc:
            raise ValueError("Invalid RDS movement fix key") from exc
        if row_number < 1:
            raise ValueError("Invalid RDS movement fix key")
        grouped[logical_name].append(row_number)
    result = []
    for logical_name, row_numbers in sorted(grouped.items()):
        ranges = []
        for row_number in sorted(set(row_numbers)):
            if ranges and row_number == ranges[-1][1] + 1:
                ranges[-1][1] = row_number
            else:
                ranges.append([row_number, row_number])
        result.append({"logical_name": logical_name, "row_ranges": ranges})
    return result


def build_rds_report_inputs(
    index_path: Path,
    *,
    annotations: list[dict],
    fix_keys: Iterable[str],
    issue_ids: Iterable[str],
    individuals: Iterable[str],
    snapshot_individuals: Iterable[str],
    target_artifact: str,
) -> tuple[bytes, bytes, dict[str, int], list[str]]:
    """Materialize focused CSV/report-sidecar inputs from the disposable index.

    Reports retain the existing CSV report implementation.  Only individuals
    contributing to the requested fixes, issues, profiles, or snapshots are
    materialized.  ``artifact_offsets`` maps a source row to its one-based row
    in that focused CSV without changing the persistent RDS review identity.
    """

    requested_issue_ids = {
        str(value).strip() for value in issue_ids if str(value).strip()
    }
    requested_individuals = {
        str(value).strip()
        for value in (*tuple(individuals), *tuple(snapshot_individuals))
        if str(value).strip()
    }
    selected_source_names = {
        item["logical_name"] for item in source_rows_from_fix_keys(fix_keys)
    }
    if requested_issue_ids:
        for annotation in annotations:
            if str(annotation.get("annotation_id") or "") not in requested_issue_ids:
                continue
            scope = dict(annotation.get("scope") or {})
            scoped_individual = str(scope.get("individual") or "").strip()
            if scoped_individual:
                requested_individuals.add(scoped_individual)
            selected_source_names.update(
                str(item.get("logical_name") or "")
                for item in scope.get("source_rows") or []
                if str(item.get("logical_name") or "")
            )

    with closing(_connect(index_path)) as connection:
        artifact_rows = connection.execute(
            """
            SELECT a.logical_name, a.row_count, i.identifier
            FROM artifacts a JOIN individuals i ON i.artifact_id=a.artifact_id
            ORDER BY a.artifact_id
            """
        ).fetchall()
        individual_by_artifact = {
            str(row["logical_name"]): str(row["identifier"])
            for row in artifact_rows
        }
        requested_individuals.update(
            individual_by_artifact[name]
            for name in selected_source_names
            if name in individual_by_artifact
        )
        included = [
            row for row in artifact_rows
            if str(row["identifier"]) in requested_individuals
        ]
        if not included:
            raise ValueError("The requested RDS report scope did not resolve to an individual")

        artifact_offsets: dict[str, int] = {}
        next_row = 0
        for row in included:
            artifact_offsets[str(row["logical_name"])] = next_row
            next_row += int(row["row_count"])

        included_names = [str(row["identifier"]) for row in included]
        placeholders = ",".join("?" for _ in included_names)
        cursor = connection.execute(
            f"""
            SELECT a.logical_name, f.source_row, i.identifier, f.time_ms,
                   f.lon, f.lat, f.burst_value, f.is_outlier,
                   f.source_outlier_status, f.source_outlier_issue_type,
                   f.source_outlier_comments, f.source_outlier_flag_step_ids
            FROM fixes f
            JOIN artifacts a ON a.artifact_id=f.artifact_id
            JOIN individuals i ON i.individual_key=f.individual_key
            WHERE i.identifier IN ({placeholders})
            ORDER BY f.ordinal
            """,
            included_names,
        )
        buffer = io.BytesIO()
        text_stream = io.TextIOWrapper(buffer, encoding="utf-8", newline="")
        writer = csv.writer(text_stream)
        writer.writerow([
            "event-id",
            "individual-local-identifier",
            "timestamp",
            "location-long",
            "location-lat",
            "burst_",
            "is_outlier",
            *RDS_REVIEW_COLUMNS,
        ])
        while True:
            batch = cursor.fetchmany(10_000)
            if not batch:
                break
            for row in batch:
                timestamp = datetime.fromtimestamp(
                    int(row["time_ms"]) / 1000.0, tz=timezone.utc
                ).isoformat().replace("+00:00", "Z")
                writer.writerow([
                    f'{row["logical_name"]}#{row["source_row"]}',
                    row["identifier"],
                    timestamp,
                    format(float(row["lon"]), ".17g"),
                    format(float(row["lat"]), ".17g"),
                    int(row["burst_value"]),
                    "true" if int(row["is_outlier"]) else "false",
                    row["source_outlier_status"],
                    row["source_outlier_issue_type"],
                    row["source_outlier_comments"],
                    row["source_outlier_flag_step_ids"],
                ])
        text_stream.flush()
        text_stream.detach()
        csv_bytes = buffer.getvalue()

    transformed = []
    for annotation in annotations:
        item = copy.deepcopy(annotation)
        scope = dict(item.get("scope") or {})
        row_ranges = []
        for source in scope.get("source_rows") or []:
            logical_name = str(source.get("logical_name") or "")
            offset = artifact_offsets.get(logical_name)
            if offset is None:
                continue
            for start, end in source.get("row_ranges") or []:
                row_ranges.append([offset + int(start), offset + int(end)])
        scope["row_ranges"] = row_ranges
        scope["source_rows"] = []
        item["scope"] = scope
        item["source_artifact"] = target_artifact
        transformed.append(item)
    sidecar_bytes = (
        json.dumps(
            {"schema_version": 6, "annotations": transformed},
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    return csv_bytes, sidecar_bytes, artifact_offsets, included_names


def rds_report_row_ranges(
    fix_keys: Iterable[str], artifact_offsets: dict[str, int]
) -> list[list[int]]:
    row_numbers = []
    for source in source_rows_from_fix_keys(fix_keys):
        offset = artifact_offsets.get(str(source["logical_name"]))
        if offset is None:
            continue
        for start, end in source["row_ranges"]:
            row_numbers.extend(range(offset + int(start), offset + int(end) + 1))
    ranges = []
    for row_number in sorted(set(row_numbers)):
        if ranges and row_number == ranges[-1][1] + 1:
            ranges[-1][1] = row_number
        else:
            ranges.append([row_number, row_number])
    return ranges


def _source_rows_from_query(
    index_path: Path,
    where: str,
    values: Sequence[object] = (),
) -> tuple[list[dict], list[str]]:
    with closing(_connect(index_path)) as connection:
        rows = connection.execute(
            """
            SELECT f.fix_key FROM fixes f
            JOIN artifacts a ON a.artifact_id=f.artifact_id
            JOIN individuals i ON i.individual_key=f.individual_key
            """
            + (" WHERE " + where if where else "")
            + " ORDER BY a.logical_name, f.source_row",
            tuple(values),
        ).fetchall()
    fix_keys = [str(row["fix_key"]) for row in rows]
    return source_rows_from_fix_keys(fix_keys), fix_keys


def resolve_rds_review_scope(index_path: Path, raw_scope: dict) -> tuple[dict, int]:
    kind = str(raw_scope.get("kind") or "").strip().lower()
    if kind in {"fix", "segment"}:
        fix_keys = [str(item) for item in raw_scope.get("fix_keys") or []]
        source_rows = source_rows_from_fix_keys(fix_keys)
        scope = {"kind": kind, "source_rows": source_rows}
        for field in (
            "start_fix_key",
            "end_fix_key",
            "individual",
            "set_name",
            "selection_method",
        ):
            if raw_scope.get(field) not in (None, ""):
                scope[field] = raw_scope[field]
        return scope, len(set(fix_keys))
    if kind == "individual":
        individual = str(raw_scope.get("individual") or "").strip()
        source_rows, fix_keys = _source_rows_from_query(
            index_path, "i.identifier = ?", (individual,)
        )
        return {
            "kind": "individual",
            "individual": individual,
            "set_name": RDS_IMPLICIT_SET,
            "source_rows": source_rows,
        }, len(fix_keys)
    if kind in {"burst", "bursts"}:
        burst_ids = (
            [str(raw_scope.get("burst_id") or "").strip()]
            if kind == "burst"
            else [str(item).strip() for item in raw_scope.get("burst_ids") or []]
        )
        clauses = []
        values: list[object] = []
        for burst_id in burst_ids:
            marker = f":{RDS_IMPLICIT_SET}:source_"
            if marker not in burst_id:
                raise ValueError("Invalid source burst id")
            individual, raw_burst = burst_id.rsplit(marker, 1)
            try:
                burst_value = int(raw_burst)
            except ValueError as exc:
                raise ValueError("Invalid source burst id") from exc
            clauses.append("(i.identifier = ? AND f.burst_value = ?)")
            values.extend((individual, burst_value))
        source_rows, fix_keys = _source_rows_from_query(
            index_path, " OR ".join(clauses), values
        )
        scope = {
            "kind": kind,
            "source_rows": source_rows,
            "burst_source": "burst_",
        }
        if kind == "burst":
            scope["burst_id"] = burst_ids[0]
        else:
            scope["burst_ids"] = burst_ids
        return scope, len(fix_keys)
    if kind == "filter":
        spec = dict(raw_scope.get("filter") or {})
        filter_kind = str(spec.get("kind") or "").strip().lower()
        scoped_individuals = [
            str(item) for item in spec.get("individuals") or [] if str(item)
        ]
        field_columns = {
            "step_length_m": "f.step_length_m",
            "speed_mps": "f.speed_mps",
            "time_delta_s": "f.time_delta_s",
            "turn_angle_deg": "f.turn_angle_deg",
            "is_outlier": "f.is_outlier",
        }
        values: list[object] = []
        if filter_kind == "gps_spike":
            where = "f.step_length_m > ? AND abs(f.turn_angle_deg) >= ?"
            values.extend(
                (
                    float(spec["step_length_threshold_m"]),
                    float(spec["minimum_abs_turn_angle_deg"]),
                )
            )
            if scoped_individuals:
                where += (
                    " AND i.identifier IN ("
                    + ",".join("?" for _ in scoped_individuals)
                    + ")"
                )
                values.extend(scoped_individuals)
        else:
            field_key = str(spec.get("field_key") or "")
            column = field_columns.get(field_key)
            if column is None:
                raise ValueError(f"Unsupported RDS filter field: {field_key}")
            field_kind = str(spec.get("field_kind") or "")
            if field_kind == "numeric":
                operator = "<" if spec.get("operator") == "lt" else ">"
                where = f"{column} {operator} ?"
                values.append(float(spec["threshold_value"]))
            elif field_kind == "boolean" and field_key == "is_outlier":
                selected = {str(item) for item in spec.get("selected_levels") or []}
                accepted = []
                if "True" in selected:
                    accepted.append(1)
                if "False" in selected:
                    accepted.append(0)
                if not accepted:
                    return {"kind": "filter", "filter": spec, "source_rows": []}, 0
                where = "f.is_outlier IN (" + ",".join("?" for _ in accepted) + ")"
                values.extend(accepted)
            else:
                raise ValueError(f"Unsupported RDS filter field: {field_key}")
            if scoped_individuals:
                where += (
                    " AND i.identifier IN ("
                    + ",".join("?" for _ in scoped_individuals)
                    + ")"
                )
                values.extend(scoped_individuals)
        source_rows, fix_keys = _source_rows_from_query(index_path, where, values)
        return {
            "kind": "filter",
            "filter": spec,
            "source_rows": source_rows,
        }, len(fix_keys)
    raise ValueError("Invalid RDS review scope")


def _review_projection(
    *,
    source_status: np.ndarray,
    annotations: list[dict],
    artifact_rows: dict[str, np.ndarray],
) -> np.ndarray:
    suspected = (source_status == 1).astype(np.int16)
    confirmed = (source_status == 2).astype(np.int16)
    parent_status: dict[str, str] = {}

    def indices_for_scope(scope: dict) -> list[np.ndarray]:
        result = []
        sources = list(scope.get("source_rows") or [])
        if not sources and scope.get("row_ranges"):
            source_name = str(scope.get("source_artifact") or "")
            if source_name:
                sources = [{"logical_name": source_name, "row_ranges": scope["row_ranges"]}]
        for source in sources:
            inverse = artifact_rows.get(str(source.get("logical_name") or ""))
            if inverse is None:
                continue
            for start, end in source.get("row_ranges") or []:
                bounded_start = max(1, int(start))
                bounded_end = min(len(inverse) - 1, int(end))
                if bounded_end < bounded_start:
                    continue
                indexes = inverse[bounded_start : bounded_end + 1]
                result.append(indexes[indexes >= 0])
        return result

    for annotation in annotations:
        kind = str(annotation.get("annotation_kind") or "issue")
        status = str(annotation.get("status") or "")
        parent_id = str(annotation.get("parent_annotation_id") or "")
        annotation_id = str(annotation.get("annotation_id") or "")
        index_groups = indices_for_scope(annotation.get("scope") or {})
        if not index_groups:
            continue
        if not parent_id and kind != "individual_review":
            parent_status[annotation_id] = status
            for indexes in index_groups:
                if status == "suspected":
                    suspected[indexes] += 1
                elif status == "confirmed":
                    confirmed[indexes] += 1
            continue
        if not parent_id:
            continue
        previous = parent_status.get(parent_id, "suspected")
        for indexes in index_groups:
            if previous == "suspected":
                suspected[indexes] = np.maximum(0, suspected[indexes] - 1)
            elif previous == "confirmed":
                confirmed[indexes] = np.maximum(0, confirmed[indexes] - 1)
            if status == "confirmed":
                confirmed[indexes] += 1
    result = np.zeros(len(source_status), dtype=np.uint8)
    result[suspected > 0] = 1
    result[confirmed > 0] = 2
    return result


def _pack_binary_columns(arrays: dict[str, np.ndarray], metadata: dict) -> bytes:
    array_meta = {}
    chunks = []
    offset = 0
    for name, raw in arrays.items():
        array = np.ascontiguousarray(raw)
        padding = (-offset) % max(1, min(8, array.dtype.itemsize))
        if padding:
            chunks.append(b"\0" * padding)
            offset += padding
        data = array.tobytes(order="C")
        array_meta[name] = {
            "dtype": array.dtype.str,
            "length": int(array.size),
            "shape": list(array.shape),
            "offset": offset,
            "byte_length": len(data),
        }
        chunks.append(data)
        offset += len(data)
    header = dict(metadata)
    header.update({"format": "vibecleaning-movement-columns", "version": 2, "arrays": array_meta})
    header_bytes = json.dumps(header, sort_keys=True, separators=(",", ":")).encode("utf-8")
    prefix = b"VCM1" + struct.pack("<I", len(header_bytes))
    header_padding = b"\0" * (-(len(prefix) + len(header_bytes)) % 8)
    return prefix + header_bytes + header_padding + b"".join(chunks)


def build_rds_binary_columns(
    index_path: Path,
    *,
    bundle_signature: str,
    annotations: list[dict] | None = None,
    individuals: list[str] | tuple[str, ...] | None = None,
) -> bytes:
    with closing(_connect(index_path)) as connection:
        artifacts = connection.execute(
            "SELECT artifact_id, logical_name, row_count FROM artifacts ORDER BY artifact_id"
        ).fetchall()
        individual_rows = connection.execute(
            "SELECT individual_key, identifier FROM individuals ORDER BY identifier"
        ).fetchall()
        requested_identifiers = {
            str(value).strip() for value in (individuals or []) if str(value).strip()
        }
        known_identifiers = {str(row["identifier"]) for row in individual_rows}
        unknown_identifiers = sorted(requested_identifiers - known_identifiers)
        if unknown_identifiers:
            raise ValueError(
                "Unknown RDS individual identifiers: " + ", ".join(unknown_identifiers)
            )
        selected_keys = [
            int(row["individual_key"])
            for row in individual_rows
            if not requested_identifiers or str(row["identifier"]) in requested_identifiers
        ]
        where_sql = ""
        query_values: list[int] = []
        if requested_identifiers:
            where_sql = " WHERE f.individual_key IN (" + ",".join("?" for _ in selected_keys) + ")"
            query_values = selected_keys
        count = int(connection.execute(
            "SELECT COUNT(*) FROM fixes f" + where_sql,
            query_values,
        ).fetchone()[0])
        individual_code = {
            int(row["individual_key"]): index for index, row in enumerate(individual_rows)
        }
        positions = np.empty((count, 2), dtype=np.float64)
        time_ms = np.empty(count, dtype=np.float64)
        individual_codes = np.empty(count, dtype=np.uint16 if len(individual_rows) <= 65535 else np.uint32)
        artifact_codes = np.empty(count, dtype=np.uint16 if len(artifacts) <= 65535 else np.uint32)
        source_rows = np.empty(count, dtype=np.uint32)
        burst_values = np.empty(count, dtype=np.int32)
        is_outlier = np.empty(count, dtype=np.uint8)
        source_status = np.zeros(count, dtype=np.uint8)
        derived = {
            name: np.empty(count, dtype=np.float32)
            for name in ("step_length_m", "speed_mps", "time_delta_s", "turn_angle_deg")
        }
        artifact_inverse = {
            str(row["logical_name"]): np.full(int(row["row_count"]) + 1, -1, dtype=np.int64)
            for row in artifacts
        }
        cursor = connection.execute(
            """
            SELECT f.*, a.logical_name FROM fixes f
            JOIN artifacts a ON a.artifact_id=f.artifact_id
            """ + where_sql + " ORDER BY f.individual_key, f.time_ms, f.source_row",
            query_values,
        )
        index = 0
        while True:
            batch = cursor.fetchmany(10_000)
            if not batch:
                break
            for row in batch:
                positions[index] = (float(row["lon"]), float(row["lat"]))
                time_ms[index] = int(row["time_ms"])
                individual_codes[index] = individual_code[int(row["individual_key"])]
                artifact_codes[index] = int(row["artifact_id"]) - 1
                source_rows[index] = int(row["source_row"])
                burst_values[index] = int(row["burst_value"])
                is_outlier[index] = int(row["is_outlier"])
                status = str(row["source_outlier_status"] or "").lower()
                source_status[index] = 2 if status == "confirmed" else 1 if status == "suspected" else 0
                for name, target in derived.items():
                    value = row[name]
                    target[index] = np.nan if value is None else float(value)
                artifact_inverse[str(row["logical_name"])][int(row["source_row"])] = index
                index += 1
    review_status = _review_projection(
        source_status=source_status,
        annotations=list(annotations or []),
        artifact_rows=artifact_inverse,
    )
    eligible = review_status != 2
    affected_individual_codes = {
        int(individual_codes[index])
        for index in np.flatnonzero(review_status == 2)
    }
    for code in affected_individual_codes:
        point_indexes = np.flatnonzero((individual_codes == code) & eligible)
        identifier = str(individual_rows[code]["identifier"])
        records = []
        for point_index in point_indexes:
            artifact_name = str(artifacts[int(artifact_codes[point_index])]["logical_name"])
            source_row = int(source_rows[point_index])
            records.append({
                "row_index": source_row,
                "fix_key": f"file:{artifact_name}#row:{source_row}",
                "individual": identifier,
                "set_name": RDS_IMPLICIT_SET,
                "time_ms": int(time_ms[point_index]),
                "lon": float(positions[point_index, 0]),
                "lat": float(positions[point_index, 1]),
                "point_index": int(point_index),
            })
        movement, _stats = compute_track_movement(
            {(identifier, RDS_IMPLICIT_SET): records}
        )
        for record in records:
            point_index = int(record["point_index"])
            values = movement[record["fix_key"]]
            for name, target in derived.items():
                value = values[name]
                target[point_index] = np.nan if value is None else float(value)
    line_sources_array = np.empty(count, dtype=np.uint32)
    line_targets_array = np.empty(count, dtype=np.uint32)
    line_count = 0
    previous_by_individual: dict[int, int] = {}
    for point_index in range(count):
        if not eligible[point_index]:
            continue
        code = int(individual_codes[point_index])
        previous = previous_by_individual.get(code)
        if previous is not None:
            line_sources_array[line_count] = previous
            line_targets_array[line_count] = point_index
            line_count += 1
        previous_by_individual[code] = point_index
    line_sources_array = line_sources_array[:line_count]
    line_targets_array = line_targets_array[:line_count]
    point_ranges = {}
    line_ranges = {}
    point_start = 0
    while point_start < count:
        code = int(individual_codes[point_start])
        point_end = point_start + 1
        while point_end < count and int(individual_codes[point_end]) == code:
            point_end += 1
        identifier = str(individual_rows[code]["identifier"])
        point_ranges[identifier] = [point_start, point_end]
        point_start = point_end
    line_start = 0
    while line_start < line_count:
        target_index = int(line_targets_array[line_start])
        code = int(individual_codes[target_index])
        line_end = line_start + 1
        while line_end < line_count:
            next_target = int(line_targets_array[line_end])
            if int(individual_codes[next_target]) != code:
                break
            line_end += 1
        identifier = str(individual_rows[code]["identifier"])
        line_ranges[identifier] = [line_start, line_end]
        line_start = line_end
    color_stats = {}
    for name, values in derived.items():
        finite = values[np.isfinite(values) & eligible]
        if len(finite):
            color_stats[name] = {
                "observed_min": float(np.min(finite)),
                "observed_max": float(np.max(finite)),
                "q01": float(np.quantile(finite, 0.01)),
                "q99": float(np.quantile(finite, 0.99)),
            }
    arrays = {
        "positions": positions,
        "time_ms": time_ms,
        "individual_codes": individual_codes,
        "artifact_codes": artifact_codes,
        "source_rows": source_rows,
        "burst_values": burst_values,
        "is_outlier": is_outlier,
        "review_status": review_status,
        **derived,
        "line_source_indexes": line_sources_array,
        "line_target_indexes": line_targets_array,
    }
    return _pack_binary_columns(
        arrays,
        {
            "source_format": "rds",
            "row_count": count,
            "line_count": len(line_sources_array),
            "source_bundle_signature": bundle_signature,
            "artifacts": [str(row["logical_name"]) for row in artifacts],
            "individuals": [str(row["identifier"]) for row in individual_rows],
            "loaded_individuals": [
                str(row["identifier"])
                for row in individual_rows
                if int(row["individual_key"]) in selected_keys
            ],
            "individual_point_ranges": point_ranges,
            "individual_line_ranges": line_ranges,
            "implicit_set": RDS_IMPLICIT_SET,
            "color_columns": {
                "step_length_m": {"array": "step_length_m", "kind": "numeric"},
                "speed_mps": {"array": "speed_mps", "kind": "numeric"},
                "time_delta_s": {"array": "time_delta_s", "kind": "numeric"},
                "turn_angle_deg": {"array": "turn_angle_deg", "kind": "numeric"},
                "is_outlier": {"array": "is_outlier", "kind": "boolean"},
            },
            "color_stats": color_stats,
        },
    )


def import_flat_rds_studies(source_dir: Path, family_dir: Path | None = None) -> dict:
    """Copy flat per-individual RDS files into one immutable project per study."""
    source_dir = source_dir.resolve()
    family_dir = (family_dir or source_dir).resolve()
    family_dir.mkdir(parents=True, exist_ok=True)
    grouped: dict[str, list[tuple[Path, dict]]] = defaultdict(list)
    for source_path in sorted(source_dir.glob("*.rds")):
        frame = read_movement_rds(source_path)
        info = validate_movement_rds(source_path, frame)
        grouped[str(info["study_id"])].append((source_path, info))
    if not grouped:
        raise ValueError(f"No RDS files found in {source_dir}")
    copied = 0
    skipped = 0
    studies = []
    for study_id, items in sorted(grouped.items()):
        destination_dir = family_dir / study_id
        destination_dir.mkdir(parents=True, exist_ok=True)
        for source_path, _info in items:
            destination = destination_dir / source_path.name
            if destination.exists():
                if _sha256_file(destination) != _sha256_file(source_path):
                    raise ValueError(f"Import destination conflicts with {destination}")
                skipped += 1
                continue
            shutil.copy2(source_path, destination)
            copied += 1
        studies.append({"study_id": study_id, "file_count": len(items)})
    return {
        "source_dir": str(source_dir),
        "family_dir": str(family_dir),
        "study_count": len(studies),
        "copied_file_count": copied,
        "skipped_file_count": skipped,
        "studies": studies,
    }
