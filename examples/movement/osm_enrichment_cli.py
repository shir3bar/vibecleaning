import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from .osm_enrichment import OSMEnrichmentError, enrich_movement_csv_with_osm_context
from .osm_extracts import GEOFABRIK_INDEX_URL


CONFIRMATION_REQUIRED_EXIT_CODE = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline movement OSM road/railway context enrichment."
    )
    parser.add_argument("--input-csv", required=True, type=Path)
    parser.add_argument("--output-csv", required=True, type=Path)
    parser.add_argument("--radius-m", required=True, type=float)
    parser.add_argument("--cache-root", required=True, type=Path)
    parser.add_argument("--confirmed-large-download", action="store_true")
    parser.add_argument("--metadata-json", type=Path)
    parser.add_argument("--geofabrik-index-url")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--progress-json", action="store_true")
    return parser.parse_args()


def default_metadata_path(output_csv: Path) -> Path:
    return output_csv.with_name(f"{output_csv.stem}.metadata.json")


def emit_progress(stage: str, *, progress_json: bool):
    if progress_json:
        print(
            json.dumps({"stage": stage, "status": "started"}, sort_keys=True),
            file=sys.stderr,
            flush=True,
        )
    else:
        print(f"{stage}...", file=sys.stderr, flush=True)


def write_json_atomic(path: Path, payload: dict):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as output_handle:
            temporary_path = Path(output_handle.name)
            json.dump(payload, output_handle, indent=2, sort_keys=True)
            output_handle.write("\n")
            output_handle.flush()
            os.fsync(output_handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def validate_paths(args: argparse.Namespace, metadata_json: Path):
    input_csv = args.input_csv.resolve()
    output_csv = args.output_csv.resolve()
    if input_csv == output_csv:
        raise ValueError("--output-csv must be different from --input-csv")
    if not input_csv.is_file():
        raise ValueError(f"Input CSV does not exist: {input_csv}")
    existing = [
        path
        for path in [args.output_csv, metadata_json]
        if path.exists()
    ]
    if existing and not args.overwrite:
        names = ", ".join(str(path) for path in existing)
        raise ValueError(f"Refusing to overwrite existing file(s): {names}")


def main() -> int:
    args = parse_args()
    metadata_json = args.metadata_json or default_metadata_path(args.output_csv)
    try:
        validate_paths(args, metadata_json)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    catalog_url = (
        args.geofabrik_index_url
        or os.environ.get("VIBECLEANING_GEOFABRIK_INDEX_URL")
        or GEOFABRIK_INDEX_URL
    )

    try:
        summary = enrich_movement_csv_with_osm_context(
            input_csv=args.input_csv,
            output_csv=args.output_csv,
            search_radius_m=args.radius_m,
            data_root=args.cache_root,
            cache_root=args.cache_root,
            input_artifact_name=args.input_csv.name,
            output_artifact_name=args.output_csv.name,
            confirmed_large_download=args.confirmed_large_download,
            catalog_url=catalog_url,
            progress_callback=lambda stage: emit_progress(
                stage,
                progress_json=args.progress_json,
            ),
        )
        emit_progress("write_metadata", progress_json=args.progress_json)
        metadata = {
            **summary,
            "input_csv": str(args.input_csv.resolve()),
            "output_csv": str(args.output_csv.resolve()),
            "metadata_json": str(metadata_json.resolve()),
            "offline_cli": True,
        }
        write_json_atomic(metadata_json, metadata)
    except OSMEnrichmentError as exc:
        print(str(exc), file=sys.stderr)
        if exc.summary and exc.summary.get("run_status") == "confirmation_required":
            return CONFIRMATION_REQUIRED_EXIT_CODE
        return 1
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.progress_json:
        print(
            json.dumps(
                {
                    "status": "completed",
                    "output_csv": str(args.output_csv),
                    "metadata_json": str(metadata_json),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
    else:
        print(f"Wrote enriched CSV: {args.output_csv}", file=sys.stderr)
        print(f"Wrote metadata JSON: {metadata_json}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
