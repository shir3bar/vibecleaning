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
import zipfile

import numpy as np
import pandas as pd
import rdata
from rdata.parser import RObject, RObjectType

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
    parsed = rdata.parser.parse_file(source_path)
    root = parsed.object
    if root.info.type != RObjectType.VEC:
        raise ValueError(f"{source_path.name} is not an R data.frame")
    attrs = _attributes(root)
    names_obj = attrs.get("names")
    if names_obj is None or names_obj.info.type != RObjectType.STR:
        raise ValueError(f"{source_path.name} is missing data.frame names")
    names = _str_values(names_obj)
    first_column = _resolve(root.value[0]) if root.value else None
    row_count = len(first_column.value) if first_column is not None else 0
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
    check = subprocess.run(
        [
            rscript,
            "-e",
            'quit(status=if (requireNamespace("sf", quietly=TRUE) && requireNamespace("move2", quietly=TRUE)) 0 else 1)',
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    return check.returncode == 0


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
review <- read.delim(args[[2]], stringsAsFactors=FALSE, check.names=FALSE,
                     na.strings="<NA>", quote="\\\"")
stopifnot(nrow(x) == nrow(review))
for (name in names(review)) x[[name]] <- review[[name]]
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


def _compare_original_columns(source_path: Path, output_path: Path) -> None:
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
    for attr in (
        "class", "sf_column", "time_column", "track_id_column", "crs_",
        "row.names", "track_data", "convergence", "v_max_used",
    ):
        if repr(source.attrs.get(attr)) != repr(output.attrs.get(attr)):
            raise ValueError(f"Reviewed RDS changed {attr} in {source_path.name}")


def export_reviewed_rds_bundle(
    *,
    sources: list[tuple[str, Path]],
    rows_by_artifact: dict[str, list[dict]],
    annotations: list[dict],
    output_zip: Path,
    writer: str = "auto",
) -> dict:
    requested = str(writer or "auto").strip().lower()
    if requested not in {"auto", "r", "python"}:
        raise ValueError("VIBECLEANING_RDS_WRITER must be auto, r, or python")
    engine = "r" if requested == "r" or (requested == "auto" and _r_writer_available()) else "python"
    if engine == "r" and not _r_writer_available():
        raise RuntimeError("R writer requested but Rscript with sf and move2 is unavailable")
    output_zip.parent.mkdir(parents=True, exist_ok=True)
    manifest_files = []
    with tempfile.TemporaryDirectory(prefix="vibecleaning-reviewed-rds-") as raw_dir:
        temporary_dir = Path(raw_dir)
        for logical_name, source_path in sources:
            rows = rows_by_artifact.get(logical_name) or []
            columns = build_review_export_columns(rows, annotations)
            output_path = temporary_dir / logical_name
            if engine == "r":
                write_reviewed_rds_r(source_path, output_path, columns)
            else:
                write_reviewed_rds_python(source_path, output_path, columns)
            _compare_original_columns(source_path, output_path)
            manifest_files.append({
                "logical_name": logical_name,
                "row_count": len(rows),
                "source_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
                "output_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
            })
        manifest = {
            "schema_version": 1,
            "writer_engine": engine,
            "review_columns": list(RDS_REVIEW_COLUMNS),
            "files": manifest_files,
        }
        (temporary_dir / "writer_manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for item in sorted(temporary_dir.iterdir()):
                archive.write(item, item.name)
    return {
        "run_status": "completed",
        "writer_engine": engine,
        "file_count": len(manifest_files),
        "row_count": sum(item["row_count"] for item in manifest_files),
        "output_artifact": output_zip.name,
    }
