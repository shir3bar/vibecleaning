import csv
import json
import os
import tempfile
from copy import deepcopy
from pathlib import Path

from .osm_context import build_fix_osm_context, normalize_local_search_radius_m
from .osm_extracts import (
    GEOFABRIK_INDEX_URL,
    OSM_SOURCE_TYPE,
    apply_tiny_footprint_source_guardrail,
    build_required_footprints,
    get_geofabrik_registry,
    prepare_multi_source_feature_caches,
    production_layer_specs,
    resolve_geofabrik_sources,
)
from .summary import detect_columns, is_valid_coordinate, try_float


OUTPUT_ARTIFACT_NAME = "movement_osm_context.csv"
OSM_CONTEXT_NOT_PLANNED_STATUS = "context_not_planned"


class OSMEnrichmentError(RuntimeError):
    def __init__(self, message: str, *, summary: dict | None = None):
        super().__init__(message)
        self.summary = summary


def summary_safe(payload: dict) -> dict:
    return json.loads(json.dumps(payload, default=str))


def _write_csv_atomic(path: Path, *, fieldnames: list[str], rows: list[dict]):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            newline="",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as output_handle:
            temporary_path = Path(output_handle.name)
            writer = csv.DictWriter(output_handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            output_handle.flush()
            os.fsync(output_handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def _overall_reporting_extent(required_footprints: list[dict], search_radius_m: float, valid_fix_count: int) -> dict:
    return {
        "search_radius_m": search_radius_m,
        "valid_fix_count": valid_fix_count,
        "movement_bbox": {
            "west": min(footprint["movement_bbox"]["west"] for footprint in required_footprints),
            "south": min(footprint["movement_bbox"]["south"] for footprint in required_footprints),
            "east": max(footprint["movement_bbox"]["east"] for footprint in required_footprints),
            "north": max(footprint["movement_bbox"]["north"] for footprint in required_footprints),
        },
        "buffered_bbox": {
            "west": min(footprint["buffered_bbox"]["west"] for footprint in required_footprints),
            "south": min(footprint["buffered_bbox"]["south"] for footprint in required_footprints),
            "east": max(footprint["buffered_bbox"]["east"] for footprint in required_footprints),
            "north": max(footprint["buffered_bbox"]["north"] for footprint in required_footprints),
        },
    }


def _summary_footprint(footprint: dict) -> dict:
    """Return footprint metadata safe for summaries without per-fix key lists."""
    return {
        key: deepcopy(value) if isinstance(value, (dict, list)) else value
        for key, value in footprint.items()
        if key != "fix_keys"
    }


def _summary_footprints(footprints: list[dict]) -> list[dict]:
    return [_summary_footprint(footprint) for footprint in footprints]


def _not_planned_context(layer_specs: list[dict]) -> dict:
    context = {}
    for layer_spec in layer_specs:
        output_columns = layer_spec["output_columns"]
        context[output_columns["distance_m"]] = ""
        context[output_columns["class"]] = ""
        context[output_columns["match_status"]] = OSM_CONTEXT_NOT_PLANNED_STATUS
    return context


def _base_summary(
    *,
    run_status: str,
    input_artifact: str,
    output_artifact: str,
    search_radius_m: float,
    overall_reporting_extent: dict,
    required_footprints: list[dict],
    registry: dict,
    resolution: dict | None = None,
    feature_result: dict | None = None,
    warnings: list[str] | None = None,
    error: str | None = None,
) -> dict:
    resolution = resolution or {}
    feature_result = feature_result or {}
    summary = {
        "app": "movement",
        "action": "enrich_osm_context",
        "run_status": run_status,
        "source_type": OSM_SOURCE_TYPE,
        "input_artifact": input_artifact,
        "output_artifact": output_artifact,
        "search_radius_m": search_radius_m,
        "coverage_model": resolution.get("coverage_model"),
        "footprint_grid_degrees": resolution.get("grid_degrees"),
        "footprint_count": len(required_footprints),
        "planned_footprint_count": resolution.get(
            "planned_footprint_count",
            resolution.get("footprint_count", len(required_footprints)),
        ),
        "excluded_footprint_count": resolution.get("excluded_footprint_count", 0),
        "excluded_footprints": list(resolution.get("excluded_footprints") or []),
        "context_not_planned_fix_count": resolution.get("context_not_planned_fix_count", 0),
        "overall_reporting_extent": overall_reporting_extent,
        "required_footprints": _summary_footprints(required_footprints),
        "initial_region_ids": resolution.get("initial_region_ids", []),
        "selected_region_ids": resolution.get("selected_region_ids", []),
        "registry_cache": {
            "cache_hit": bool(registry["cache_hit"]),
            "metadata": registry["metadata"],
        },
        "warnings": list(warnings or []),
    }
    if "preflight" in feature_result:
        summary["source_preflight"] = feature_result.get("preflight", {})
    if "regional_caches" in feature_result:
        summary["regional_caches"] = feature_result.get("regional_caches", [])
    if error is not None:
        summary["error"] = error
    return summary_safe(summary)


def enrich_movement_csv_with_osm_context(
    *,
    input_csv: Path,
    output_csv: Path,
    search_radius_m: float,
    data_root: Path,
    cache_root: Path | None = None,
    input_artifact_name: str | None = None,
    output_artifact_name: str = OUTPUT_ARTIFACT_NAME,
    confirmed_large_download: bool = False,
    catalog_url: str | None = None,
    progress_callback=None,
) -> dict:
    """Enrich one movement CSV with persisted road/railway OSM context columns."""
    def emit(stage: str):
        if progress_callback is not None:
            progress_callback(stage)

    input_csv = Path(input_csv)
    output_csv = Path(output_csv)
    data_root = Path(data_root)
    resolved_cache_root = Path(cache_root) if cache_root is not None else None
    search_radius_m = normalize_local_search_radius_m(search_radius_m)
    input_artifact = input_artifact_name or input_csv.name
    catalog_url = catalog_url or os.environ.get("VIBECLEANING_GEOFABRIK_INDEX_URL", GEOFABRIK_INDEX_URL)

    emit("read_input")
    with input_csv.open("r", newline="", encoding="utf-8") as input_handle:
        reader = csv.DictReader(input_handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [dict(row) for row in reader]
    columns = detect_columns(fieldnames)
    if not columns["lon"] or not columns["lat"]:
        raise SystemExit("CSV is missing required coordinate columns for OSM context enrichment")

    layer_specs = production_layer_specs()
    output_columns = [
        column
        for layer_spec in layer_specs
        for column in layer_spec["output_columns"].values()
    ]
    duplicate_columns = [column for column in output_columns if column in fieldnames]
    if duplicate_columns:
        raise SystemExit(f"Input artifact already contains OSM context column(s): {duplicate_columns}")

    normalized_fixes = []
    for row_index, raw in enumerate(rows, start=1):
        lon = try_float(raw.get(columns["lon"]))
        lat = try_float(raw.get(columns["lat"]))
        if lon is None or lat is None or not is_valid_coordinate(lon, lat):
            continue
        normalized_fixes.append(
            {
                "fix_key": f"source_row:{row_index}",
                "lon": float(lon),
                "lat": float(lat),
            }
        )
    if not normalized_fixes:
        raise SystemExit("CSV did not contain any valid coordinates for OSM context enrichment")

    emit("build_footprints")
    required_footprints = build_required_footprints(
        normalized_fixes,
        search_radius_m=search_radius_m,
    )
    overall_reporting_extent = _overall_reporting_extent(
        required_footprints,
        search_radius_m,
        len(normalized_fixes),
    )
    emit("load_registry")
    registry = get_geofabrik_registry(data_root, cache_root=resolved_cache_root, catalog_url=catalog_url)
    emit("resolve_sources")
    resolution = resolve_geofabrik_sources(registry["extracts"], required_footprints)
    if resolution["run_status"] != "resolved":
        warnings = [*registry.get("warnings", []), *resolution.get("warnings", [])]
        summary = _base_summary(
            run_status="unresolved",
            input_artifact=input_artifact,
            output_artifact=output_artifact_name,
            search_radius_m=search_radius_m,
            overall_reporting_extent=overall_reporting_extent,
            required_footprints=required_footprints,
            registry=registry,
            resolution=resolution,
            warnings=warnings,
            error=resolution.get("error"),
        )
        raise OSMEnrichmentError(str(resolution.get("error") or "Could not resolve OSM sources."), summary=summary)
    guardrail_result = apply_tiny_footprint_source_guardrail(
        data_root,
        resolution,
        cache_root=resolved_cache_root,
    )
    resolution = guardrail_result["resolution"]
    if guardrail_result["run_status"] != "completed" or resolution.get("run_status") != "resolved":
        warnings = [
            *registry.get("warnings", []),
            *resolution.get("warnings", []),
            *guardrail_result.get("warnings", []),
        ]
        summary = _base_summary(
            run_status="unresolved",
            input_artifact=input_artifact,
            output_artifact=output_artifact_name,
            search_radius_m=search_radius_m,
            overall_reporting_extent=overall_reporting_extent,
            required_footprints=required_footprints,
            registry=registry,
            resolution=resolution,
            warnings=warnings,
            error=guardrail_result.get("error") or resolution.get("error"),
        )
        raise OSMEnrichmentError(
            str(
                guardrail_result.get("error")
                or resolution.get("error")
                or "Could not plan OSM sources after applying the tiny-footprint guardrail."
            ),
            summary=summary,
        )

    emit("preflight_sources")
    emit("prepare_feature_caches")
    feature_result = prepare_multi_source_feature_caches(
        data_root,
        resolution,
        cache_root=resolved_cache_root,
        confirmed_large_download=confirmed_large_download,
    )
    if feature_result["run_status"] == "confirmation_required":
        reasons = "; ".join(feature_result.get("preflight", {}).get("confirmation_reasons", []))
        warnings = [*registry.get("warnings", []), *feature_result.get("warnings", [])]
        summary = _base_summary(
            run_status="confirmation_required",
            input_artifact=input_artifact,
            output_artifact=output_artifact_name,
            search_radius_m=search_radius_m,
            overall_reporting_extent=overall_reporting_extent,
            required_footprints=required_footprints,
            registry=registry,
            resolution=resolution,
            feature_result=feature_result,
            warnings=warnings,
            error=reasons,
        )
        raise OSMEnrichmentError(
            "The required OSM regional extract set requires confirmation before download"
            + (f": {reasons}" if reasons else "")
            + "; repeat the action with confirmed_large_download=true.",
            summary=summary,
        )
    if feature_result["run_status"] != "completed" or feature_result.get("complete") is not True:
        warnings = [*registry.get("warnings", []), *feature_result.get("warnings", [])]
        summary = _base_summary(
            run_status="failed",
            input_artifact=input_artifact,
            output_artifact=output_artifact_name,
            search_radius_m=search_radius_m,
            overall_reporting_extent=overall_reporting_extent,
            required_footprints=required_footprints,
            registry=registry,
            resolution=resolution,
            feature_result=feature_result,
            warnings=warnings,
            error=feature_result.get("error"),
        )
        raise OSMEnrichmentError(
            str(feature_result.get("error") or "Could not obtain complete local OSM road and railway features."),
            summary=summary,
        )

    emit("compute_context")
    context_by_fix_key = build_fix_osm_context(
        normalized_fixes,
        feature_result["features_by_layer"],
        search_radius_m=search_radius_m,
        layer_specs=layer_specs,
    )
    excluded_fix_keys = {
        str(fix_key)
        for footprint in required_footprints
        if any(
            excluded.get("footprint_id") == footprint.get("footprint_id")
            for excluded in resolution.get("excluded_footprints", [])
        )
        for fix_key in footprint.get("fix_keys", [])
    }
    not_planned_context = _not_planned_context(layer_specs)
    for fix_key in excluded_fix_keys:
        context_by_fix_key[fix_key] = dict(not_planned_context)

    enriched_rows = []
    for row_index, raw in enumerate(rows, start=1):
        enriched = dict(raw)
        context = context_by_fix_key.get(f"source_row:{row_index}", {})
        for column in output_columns:
            enriched[column] = context.get(column, "")
        enriched_rows.append(enriched)

    emit("write_output")
    _write_csv_atomic(output_csv, fieldnames=[*fieldnames, *output_columns], rows=enriched_rows)

    warnings = [
        *registry.get("warnings", []),
        *resolution.get("warnings", []),
        *feature_result.get("warnings", []),
    ]
    unusable_coordinate_count = len(rows) - len(normalized_fixes)
    if unusable_coordinate_count:
        warnings.append(
            f"Left OSM context blank for {unusable_coordinate_count} row(s) without valid coordinates."
        )
    feature_counts = dict(feature_result.get("feature_counts") or {})
    selected_sources = list(resolution.get("selected_sources") or [])
    selected_region_ids = list(resolution.get("selected_region_ids") or [])
    excluded_footprints = list(resolution.get("excluded_footprints") or [])
    regional_caches = list(feature_result.get("regional_caches") or [])
    first_region_cache = regional_caches[0] if len(regional_caches) == 1 else None
    first_selected_source = selected_sources[0] if len(selected_sources) == 1 else None
    footprint_region_assignments = {
        source["region_id"]: list(source.get("assigned_footprint_ids") or [])
        for source in selected_sources
    }
    return summary_safe(
        {
            "app": "movement",
            "action": "enrich_osm_context",
            "run_status": "completed",
            "source_type": OSM_SOURCE_TYPE,
            "input_artifact": input_artifact,
            "output_artifact": output_artifact_name,
            "search_radius_m": search_radius_m,
            "input_row_count": len(rows),
            "output_row_count": len(enriched_rows),
            "valid_coordinate_row_count": len(normalized_fixes),
            "unusable_coordinate_row_count": unusable_coordinate_count,
            "appended_columns": output_columns,
            "coverage_model": resolution.get("coverage_model"),
            "footprint_grid_degrees": resolution.get("grid_degrees"),
            "footprint_count": len(required_footprints),
            "planned_footprint_count": resolution.get("planned_footprint_count", len(required_footprints)),
            "excluded_footprint_count": len(excluded_footprints),
            "excluded_footprints": excluded_footprints,
            "context_not_planned_fix_count": len(excluded_fix_keys),
            "overall_reporting_extent": overall_reporting_extent,
            "required_footprints": _summary_footprints(required_footprints),
            "initial_region_ids": list(resolution.get("initial_region_ids") or []),
            "selected_region_ids": selected_region_ids,
            "footprint_region_assignments": footprint_region_assignments,
            "required_extent": (
                first_selected_source.get("assigned_required_extent")
                if first_selected_source is not None
                else overall_reporting_extent
            ),
            "coverage_validation": {
                "validated": True,
                "region_id": selected_region_ids[0] if len(selected_region_ids) == 1 else None,
                "selected_region_ids": selected_region_ids,
                "footprint_count": resolution.get("planned_footprint_count", len(required_footprints)),
                "excluded_footprint_count": len(excluded_footprints),
                "resolution_mode": resolution["resolution_mode"],
            },
            "registry_cache": {
                "cache_hit": bool(registry["cache_hit"]),
                "metadata": registry["metadata"],
            },
            "source_cache": {
                "cache_hit": bool(first_region_cache["source_cache_hit"]) if first_region_cache else None,
                "region_id": first_selected_source["region_id"] if first_selected_source else None,
                "region_name": first_selected_source["region_name"] if first_selected_source else None,
                "pbf_url": first_selected_source["pbf_url"] if first_selected_source else None,
                "source_revision": first_region_cache["source_revision"] if first_region_cache else None,
                "metadata": first_region_cache["source_cache"]["metadata"] if first_region_cache else None,
            },
            "feature_cache": {
                "cache_hit": bool(first_region_cache["feature_cache_hit"]) if first_region_cache else None,
                "complete": True,
                "feature_counts": feature_counts,
                "duplicate_counts": dict(feature_result.get("duplicate_counts") or {}),
                "metadata": first_region_cache["feature_cache"]["metadata"] if first_region_cache else None,
            },
            "source_preflight": feature_result.get("preflight", {}),
            "source_caches": [
                {
                    "region_id": regional_cache["region_id"],
                    "cache_hit": bool(regional_cache["source_cache_hit"]),
                    "source_revision": regional_cache["source_revision"],
                    "metadata": regional_cache["source_cache"]["metadata"],
                }
                for regional_cache in regional_caches
            ],
            "feature_caches": [
                {
                    "region_id": regional_cache["region_id"],
                    "cache_hit": bool(regional_cache["feature_cache_hit"]),
                    "complete": bool(regional_cache["feature_cache"]["metadata"]["complete"]),
                    "feature_counts": regional_cache["feature_counts"],
                    "duplicate_counts": regional_cache["duplicate_counts"],
                    "metadata": regional_cache["feature_cache"]["metadata"],
                }
                for regional_cache in regional_caches
            ],
            "merged_duplicate_counts": dict(feature_result.get("duplicate_counts") or {}),
            "layers": [
                {
                    "layer_name": layer_spec["layer_name"],
                    "class_tag": layer_spec["class_tag"],
                    "output_columns": layer_spec["output_columns"],
                    "feature_count": int(feature_counts.get(layer_spec["layer_name"], 0)),
                }
                for layer_spec in layer_specs
            ],
            "warnings": warnings,
        }
    )
