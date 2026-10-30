"""Reviewed move2/sf RDS bundle export."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Iterable
from collections.abc import Callable

import numpy as np
import pandas as pd
import rdata
from rdata.parser import RObject, RObjectType

from app.filesystem import atomic_replace, atomic_write_json, exclusive_file_lock
from .rds_index import RDS_REVIEW_COLUMNS, read_movement_rds
from .review_annotations import (
    effective_issues_for_fix,
    effective_review_status,
    normalize_annotation,
)


def _unique(values: Iterable[str]) -> list[str]:
    result = []
    for raw in values:
        value = str(raw or "").strip()
        if value and value not in result:
            result.append(value)
    return result


def build_review_export_columns(
    rows: list[dict], annotations: list[dict]
) -> dict[str, list[str | None]]:
    output = {name: [] for name in RDS_REVIEW_COLUMNS}
    for row in rows:
        fix_key = str(row["fix_key"])
        source_status = str(row.get("source_outlier_status") or "").strip().lower()
        existing = []
        if source_status in {"suspected", "confirmed"}:
            existing.append(normalize_annotation({
                "annotation_id": f"source:{fix_key}",
                "status": source_status,
                "origin": "manual",
                "issue_type": row.get("source_outlier_issue_type"),
                "comment": row.get("source_outlier_comments"),
                "scope": {
                    "kind": "fix",
                    "source_rows": [{
                        "logical_name": row["logical_name"],
                        "row_ranges": [[row["source_row"], row["source_row"]]],
                    }],
                },
            }))
        effective = effective_issues_for_fix(
            annotations,
            fix_key=fix_key,
            individual=str(row["identifier"]),
            set_name="train",
            source_artifact=str(row["logical_name"]),
            existing_issues=existing,
        )
        active = [item for item in effective if item.get("status") != "dismissed"]
        status = effective_review_status(effective)
        issue_types = _unique(item.get("issue_type") for item in active)
        comments = _unique(item.get("issue_note") for item in active)
        step_ids = _unique(
            str(row.get("source_outlier_flag_step_ids") or "").split(";")
            + [
                value
                for item in active
                for value in (item.get("step_id"), item.get("resolution_step_id"))
            ]
        )
        output["outlier_status"].append(status or None)
        output["outlier_issue_type"].append("; ".join(issue_types) or None)
        output["outlier_comments"].append("; ".join(comments) or None)
        output["outlier_flag_step_ids"].append(";".join(step_ids) or None)
    return output


def _resolve(obj: RObject | None) -> RObject | None:
    if obj is not None and obj.info.type == RObjectType.REF:
        return obj.referenced_object
    return obj


def _tag_name(node: RObject) -> str:
    tag = _resolve(node.tag)
    if tag is not None and tag.info.type == RObjectType.SYM:
        tag = _resolve(tag.value)
    if tag is not None and tag.info.type == RObjectType.CHAR:
        return bytes(tag.value).decode("utf-8")
    return ""


def _attributes(obj: RObject) -> dict[str, RObject]:
    result = {}
    node = _resolve(obj.attributes)
    while node is not None and node.info.type != RObjectType.NILVALUE:
        value, tail = node.value
        resolved = _resolve(value)
        if resolved is not None:
            result[_tag_name(node)] = resolved
        node = _resolve(tail)
    return result


def _str_values(obj: RObject) -> list[str]:
    return [
        bytes(item.value).decode("utf-8") if item.value is not None else ""
        for item in obj.value
    ]


def _string_r_object(values: list[str | None]) -> RObject:
    array = np.array(values, dtype=object)
    return rdata.conversion.convert_python_to_r_data(array).object


def write_reviewed_rds_python(
    source_path: Path,
    output_path: Path,
    columns: dict[str, list[str | None]],
) -> None:
    # This is a lossless edit of the serialized R object, not a conversion to
    # Python data. Expanding ALTREP can remove symbols from the serialized
    # reference table while leaving later REF indexes unchanged (including
    # nested data.frame row.names). Keep the original representation intact.
    parsed = rdata.parser.parse_file(source_path, expand_altrep=False)
    root = parsed.object
    if root.info.type != RObjectType.VEC:
        raise ValueError(f"{source_path.name} is not an R data.frame")
    attrs = _attributes(root)
    names_obj = attrs.get("names")
    if names_obj is None or names_obj.info.type != RObjectType.STR:
        raise ValueError(f"{source_path.name} is missing data.frame names")
    names = _str_values(names_obj)
    # The first column may itself be a compact ALTREP vector; its serialized
    # payload length is not its number of rows. Use a separate decoded view.
    row_count = len(read_movement_rds(source_path))
    for name in RDS_REVIEW_COLUMNS:
        values = columns[name]
        if len(values) != row_count:
            raise ValueError(f"Review projection length does not match {source_path.name}")
        replacement = _string_r_object(values)
        if name in names:
            root.value[names.index(name)] = replacement
        else:
            root.value.append(replacement)
            names_obj.value.extend(_string_r_object([name]).value)
            names.append(name)
            agr = attrs.get("agr")
            if agr is not None and agr.info.type == RObjectType.INT:
                agr.value = np.ma.concatenate(
                    [agr.value, np.ma.array([0], mask=[True], dtype=np.int32)]
                )
                agr_names = _attributes(agr).get("names")
                if agr_names is not None and agr_names.info.type == RObjectType.STR:
                    agr_names.value.extend(_string_r_object([name]).value)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rdata.unparser.unparse_file(
        output_path,
        parsed,
        file_type="rds",
        file_format="xdr",
        compression="gzip",
    )


def _write_projection_tsv(path: Path, columns: dict[str, list[str | None]]) -> None:
    row_count = len(next(iter(columns.values()), []))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(RDS_REVIEW_COLUMNS)
        for index in range(row_count):
            writer.writerow([
                columns[name][index] if columns[name][index] is not None else "<NA>"
                for name in RDS_REVIEW_COLUMNS
            ])


def _r_writer_available() -> bool:
    rscript = shutil.which("Rscript")
    if not rscript:
        return False
    # Appending columns to the serialized input needs only base R, including AMT.
    return True


def write_reviewed_rds_r(
    source_path: Path,
    output_path: Path,
    columns: dict[str, list[str | None]],
) -> None:
    rscript = shutil.which("Rscript")
    if not rscript:
        raise RuntimeError("Rscript is unavailable")
    with tempfile.TemporaryDirectory(prefix="vibecleaning-rds-review-") as raw_dir:
        projection_path = Path(raw_dir) / "review.tsv"
        _write_projection_tsv(projection_path, columns)
        script = """
args <- commandArgs(trailingOnly=TRUE)
x <- readRDS(args[[1]])
original_attributes <- attributes(x)
# attributes() expands automatic row names; retain their original compact form.
original_attributes$row.names <- .row_names_info(x, type=0L)
review <- read.delim(args[[2]], stringsAsFactors=FALSE, check.names=FALSE,
                     colClasses="character",
                     na.strings="<NA>", quote="\\\"")
stopifnot(nrow(x) == nrow(review))
for (name in names(review)) x[[name]] <- review[[name]]
original_attributes$names <- names(x)
if (!is.null(attr(x, "agr"))) original_attributes$agr <- attr(x, "agr")
attributes(x) <- original_attributes
saveRDS(x, args[[3]], compress=TRUE)
"""
        result = subprocess.run(
            [rscript, "-e", script, str(source_path), str(projection_path), str(output_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or "R reviewed-RDS writer failed")


def _compare_attribute_values(left, right) -> None:
    if isinstance(left, pd.DataFrame) and isinstance(right, pd.DataFrame):
        pd.testing.assert_frame_equal(left, right)
    elif isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            raise AssertionError("Attribute keys changed")
        for key in left:
            _compare_attribute_values(left[key], right[key])
    elif isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        if len(left) != len(right):
            raise AssertionError("Attribute length changed")
        for a, b in zip(left, right, strict=True):
            _compare_attribute_values(a, b)
    else:
        np.testing.assert_equal(left, right)


def _compare_original_columns(source_path: Path, output_path: Path, expected_review: dict | None = None) -> None:
    source = read_movement_rds(source_path)
    output = read_movement_rds(output_path)
    original_names = list(map(str, source.columns))
    if list(map(str, output.columns[: len(original_names)])) != original_names:
        raise ValueError(f"Reviewed RDS changed original columns in {source_path.name}")
    if list(map(str, output.columns[len(original_names) :])) != [
        name for name in RDS_REVIEW_COLUMNS if name not in original_names
    ]:
        raise ValueError(f"Reviewed RDS has unexpected appended columns in {source_path.name}")
    if len(source) != len(output):
        raise ValueError(f"Reviewed RDS changed row count in {source_path.name}")
    for name in original_names:
        if name in RDS_REVIEW_COLUMNS:
            continue  # App-owned annotations may change on reimport/review.
        left = source[name].to_numpy()
        right = output[name].to_numpy()
        if name == "geometry":
            if any(not np.array_equal(a, b, equal_nan=True) for a, b in zip(left, right, strict=True)):
                raise ValueError(f"Reviewed RDS changed geometry in {source_path.name}")
        else:
            try:
                pd.testing.assert_series_equal(
                    source[name], output[name], check_names=False, check_dtype=True
                )
            except AssertionError as exc:
                raise ValueError(
                    f"Reviewed RDS changed {name} in {source_path.name}"
                ) from exc
    for attr in (set(source.attrs) | set(output.attrs) | set((
        "class", "sf_column", "time_column", "track_id_column", "crs_", "geometry_crs",
        "row.names", "track_data", "convergence", "v_max_used",
    ))) - {"names", "agr"}:
        try:
            _compare_attribute_values(source.attrs.get(attr), output.attrs.get(attr))
        except AssertionError as exc:
            raise ValueError(f"Reviewed RDS changed {attr} in {source_path.name}") from exc
    if expected_review is not None:
        for name in RDS_REVIEW_COLUMNS:
            # Legacy exports can represent an all-missing review column as
            # logical. Compare missing values without inserting text into a
            # nullable Boolean array; non-missing mismatches must still fail.
            actual = ["" if pd.isna(value) else str(value) for value in output[name]]
            expected = ["" if value is None else str(value) for value in expected_review[name]]
            if actual != expected:
                raise ValueError(f"Reviewed RDS has incorrect generated {name} in {source_path.name}")


def cleaned_rds_name(logical_name: str) -> str:
    path = Path(logical_name)
    if path.name != logical_name or path.suffix.lower() != ".rds":
        raise ValueError("Expected an RDS filename")
    stem = path.stem.removesuffix("_cleaned")
    return f"{stem}_cleaned.rds"


def _publish_cleaned_files(output_dir: Path, destination: Path, manifest: dict, report) -> None:
    """Publish a complete validated export, retaining the previous one on failure."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    backup = destination.with_name(".cleaned_files_previous")
    with tempfile.TemporaryDirectory(prefix=".cleaned-files-", dir=destination.parent) as temporary:
        staged = Path(temporary) / "ready"
        staged.mkdir()
        for index, item in enumerate(manifest["files"] + manifest.get("auxiliary_files", [])):
            report("saving", index, len(manifest["files"]), item["output_name"])
            shutil.copy2(output_dir / item["output_name"], staged / item["output_name"])
        shutil.copy2(output_dir / "writer_manifest.json", staged / "writer_manifest.json")
        with exclusive_file_lock(destination.with_suffix(".lock")):
            # Recover an interrupted directory replacement before the next save.
            if backup.exists():
                if not destination.exists():
                    atomic_replace(backup, destination)
                else:
                    shutil.rmtree(backup)
            if destination.exists():
                # Only replace files belonging to an earlier export, never a
                # pre-existing folder containing unrelated user files.
                try:
                    previous = json.loads((destination / "writer_manifest.json").read_text(encoding="utf-8"))
                    expected = {item["output_name"] for item in previous["files"] + previous.get("auxiliary_files", [])} | {"writer_manifest.json"}
                    actual = {item.name for item in destination.iterdir() if item.name != ".DS_Store"}
                    if destination.is_symlink() or actual != expected:
                        raise ValueError("Folder contains files outside the previous export")
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    raise ValueError(f"Cannot replace {destination}: it contains files not recognised as a previous RDS export") from exc
                atomic_replace(destination, backup)
            try:
                atomic_replace(staged, destination)
            except OSError:
                if backup.exists():
                    atomic_replace(backup, destination)
                raise
            if backup.exists():
                # Publishing has succeeded. A leftover backup can be cleaned
                # on the next export if Windows still has a file open in it.
                shutil.rmtree(backup, ignore_errors=True)


def export_reviewed_rds_bundle(
    *,
    sources: list[tuple[str, Path]],
    rows_by_artifact: dict[str, list[dict]],
    annotations: list[dict],
    output_dir: Path,
    cleaned_dir: Path | None,
    provenance: dict | None = None,
    writer: str = "auto",
    progress: Callable[[str, int, int, str], None] | None = None,
    decisions: list[dict] | None = None,
    metadata_path: Path | None = None,
) -> dict:
    def report(stage: str, completed: int, total: int, name: str = "") -> None:
        if progress is not None:
            progress(stage, completed, total, name)

    requested = str(writer or "auto").strip().lower()
    if requested not in {"auto", "r", "python"}:
        raise ValueError("VIBECLEANING_RDS_WRITER must be auto, r, or python")
    engine = "r" if requested == "r" or (requested == "auto" and _r_writer_available()) else "python"
    if engine == "r" and not _r_writer_available():
        raise RuntimeError("R writer requested but Rscript is unavailable")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_names = [cleaned_rds_name(name) for name, _ in sources]
    if not sources or len(set(name.casefold() for name in output_names)) != len(sources):
        raise ValueError("RDS export requires sources with distinct cleaned filenames")
    manifest_files = []
    for index, (logical_name, source_path) in enumerate(sources):
        report("reviewing", index, len(sources), logical_name)
        rows = rows_by_artifact.get(logical_name) or []
        columns = build_review_export_columns(rows, annotations)
        output_path = output_dir / output_names[index]
        report("writing", index, len(sources), logical_name)
        if engine == "r":
            write_reviewed_rds_r(source_path, output_path, columns)
        else:
            write_reviewed_rds_python(source_path, output_path, columns)
        report("checking", index, len(sources), logical_name)
        _compare_original_columns(source_path, output_path, columns)
        manifest_files.append({
            "logical_name": logical_name,
            "output_name": output_path.name,
            "row_count": len(rows),
            "source_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
            "output_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        })
    manifest = {
        "schema_version": 2,
        **(provenance or {}),
        "writer_engine": engine,
        "review_columns": list(RDS_REVIEW_COLUMNS),
        "files": manifest_files,
    }
    if decisions is not None:
        from examples.movement.decision_export import save_decision_export
        manifest["auxiliary_files"] = [save_decision_export(output_dir, decisions)]
    if metadata_path is not None:
        destination = output_dir / "metadata.json"
        shutil.copy2(metadata_path, destination)
        manifest.setdefault("auxiliary_files", []).append({
            "output_name": destination.name,
            "output_sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
        })
    atomic_write_json(output_dir / "writer_manifest.json", manifest)
    if cleaned_dir is not None:
        _publish_cleaned_files(output_dir, cleaned_dir, manifest, report)
    report("finishing", len(sources), len(sources))
    return {
        "run_status": "completed",
        "writer_engine": engine,
        "file_count": len(manifest_files),
        "row_count": sum(item["row_count"] for item in manifest_files),
        "output_directory": str((cleaned_dir or output_dir).resolve()),
        "output_artifact": "writer_manifest.json",
    }
