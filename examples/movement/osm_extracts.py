import gzip
import hashlib
import json
import math
import os
import tempfile
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import httpx
from shapely.geometry import box, mapping, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from app.osm import EARTH_RADIUS_M

from .osm_context import OSM_CONTEXT_LAYER_SPECS, normalize_local_search_radius_m


OSM_SOURCE_PROVIDER = "geofabrik"
OSM_SOURCE_TYPE = "local_extract"
GEOFABRIK_INDEX_URL = "https://download.geofabrik.de/index-v1.json"
OSM_CACHE_SCHEMA_VERSION = 1
OSM_LAYER_SPEC_VERSION = "road-railway-v1"
OSM_EXTRACTOR_VERSION = "local-pbf-v1"
SUPPORTED_CONTEXT_LAYER_NAMES = ("road", "railway")
MAX_UNCONFIRMED_DOWNLOAD_BYTES = 500 * 1024 * 1024
MAX_MEMORY_LOCATION_INDEX_BYTES = 500 * 1024 * 1024
HTTP_TIMEOUT_SECONDS = 60.0
PBF_DOWNLOAD_READ_TIMEOUT_SECONDS = 300.0
PBF_DOWNLOAD_MAX_ATTEMPTS = 3
OSM_COVERAGE_MODEL = "occupied_grid_footprints_v1"
FOOTPRINT_GRID_DEGREES = 0.25
MAX_REQUIRED_FOOTPRINTS = 10_000
MAX_SELECTED_SOURCES = 50
MAX_UNCONFIRMED_SOURCE_COUNT = 8
MIN_FIXES_TO_FORCE_LARGE_OSM_SOURCE = 5
MAX_SOURCE_BYTES_FOR_TINY_FOOTPRINT = 2 * 1024 * 1024 * 1024
TINY_FOOTPRINT_EXCLUSION_REASON = "tiny_footprint_requires_oversized_source"


class OSMExtractSourceError(ValueError):
    pass


def production_layer_specs() -> list[dict]:
    """Return the production layer definitions supported by local extracts."""
    return [
        deepcopy(OSM_CONTEXT_LAYER_SPECS[layer_name])
        for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES
    ]


def _finite_number(raw_value: object) -> float | None:
    if isinstance(raw_value, bool):
        return None
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _normalized_coordinate(fix: dict) -> tuple[float, float] | None:
    lon = _finite_number(fix.get("lon"))
    lat = _finite_number(fix.get("lat"))
    if lon is None or lat is None:
        return None
    if lon < -180.0 or lon > 180.0 or lat < -90.0 or lat > 90.0:
        return None
    return lon, lat


def _build_required_extent_from_coordinates(
    coordinates: list[tuple[float, float]],
    *,
    search_radius_m: float,
) -> dict:
    radius_m = normalize_local_search_radius_m(search_radius_m)
    if not coordinates:
        raise OSMExtractSourceError(
            "No valid fix coordinates are available for local OSM source selection."
        )

    west = min(lon for lon, _ in coordinates)
    south = min(lat for _, lat in coordinates)
    east = max(lon for lon, _ in coordinates)
    north = max(lat for _, lat in coordinates)
    latitude_padding = math.degrees(radius_m / EARTH_RADIUS_M)
    padded_south = max(-90.0, south - latitude_padding)
    padded_north = min(90.0, north + latitude_padding)
    furthest_latitude = max(abs(padded_south), abs(padded_north))
    if furthest_latitude >= 89.999999:
        longitude_padding = 180.0
    else:
        longitude_padding = math.degrees(
            radius_m
            / (EARTH_RADIUS_M * math.cos(math.radians(furthest_latitude)))
        )
    padded_west = west - longitude_padding
    padded_east = east + longitude_padding
    if padded_west < -180.0 or padded_east > 180.0:
        raise OSMExtractSourceError(
            "Local OSM source selection does not yet support buffered extents crossing the antimeridian."
        )

    buffered_geometry = box(padded_west, padded_south, padded_east, padded_north)
    return {
        "search_radius_m": float(radius_m),
        "valid_fix_count": len(coordinates),
        "movement_bbox": {
            "west": float(west),
            "south": float(south),
            "east": float(east),
            "north": float(north),
        },
        "buffered_bbox": {
            "west": float(padded_west),
            "south": float(padded_south),
            "east": float(padded_east),
            "north": float(padded_north),
        },
        "geometry": mapping(buffered_geometry),
    }


def build_required_extent(fixes: list[dict], *, search_radius_m: float) -> dict:
    """Return a conservative WGS84 bbox covering every fix plus its search radius."""
    coordinates = [
        coordinate
        for fix in fixes
        if (coordinate := _normalized_coordinate(fix)) is not None
    ]
    return _build_required_extent_from_coordinates(
        coordinates,
        search_radius_m=search_radius_m,
    )


def _normalized_grid_degrees(grid_degrees: object) -> float:
    value = _finite_number(grid_degrees)
    if value is None or value <= 0.0 or value > 360.0:
        raise OSMExtractSourceError(
            "OSM footprint grid_degrees must be a positive finite value no greater than 360."
        )
    return value


def build_required_footprints(
    fixes: list[dict],
    *,
    search_radius_m: float,
    grid_degrees: float = FOOTPRINT_GRID_DEGREES,
) -> list[dict]:
    """Return deterministic buffered occupied-cell footprints for source planning."""
    radius_m = normalize_local_search_radius_m(search_radius_m)
    normalized_grid = _normalized_grid_degrees(grid_degrees)
    occupied: dict[tuple[int, int], list[tuple[float, float]]] = {}
    occupied_fix_keys: dict[tuple[int, int], list[str]] = {}
    for fix in fixes:
        coordinate = _normalized_coordinate(fix)
        if coordinate is None:
            continue
        lon, lat = coordinate
        lon_index = math.floor((lon + 180.0) / normalized_grid)
        lat_index = math.floor((lat + 90.0) / normalized_grid)
        cell_key = (lat_index, lon_index)
        occupied.setdefault(cell_key, []).append(coordinate)
        fix_key = str(fix.get("fix_key") or "").strip()
        if fix_key:
            occupied_fix_keys.setdefault(cell_key, []).append(fix_key)

    if not occupied:
        raise OSMExtractSourceError(
            "No valid fix coordinates are available for local OSM source selection."
        )
    if len(occupied) > MAX_REQUIRED_FOOTPRINTS:
        raise OSMExtractSourceError(
            "Movement locations require "
            f"{len(occupied)} occupied OSM footprints, exceeding the supported maximum "
            f"of {MAX_REQUIRED_FOOTPRINTS}."
        )

    footprints = []
    for lat_index, lon_index in sorted(occupied):
        footprint = _build_required_extent_from_coordinates(
            occupied[(lat_index, lon_index)],
            search_radius_m=radius_m,
        )
        footprint.update(
            {
                "footprint_id": f"grid:{lat_index:06d}:{lon_index:06d}",
                "coverage_model": OSM_COVERAGE_MODEL,
                "grid_degrees": float(normalized_grid),
                "fix_keys": sorted(occupied_fix_keys.get((lat_index, lon_index), [])),
            }
        )
        footprints.append(footprint)
    return footprints


def _valid_coverage_geometry(raw_geometry: object) -> BaseGeometry | None:
    if not isinstance(raw_geometry, dict):
        return None
    try:
        geometry = shape(raw_geometry)
    except (TypeError, ValueError):
        return None
    if geometry.is_empty or not geometry.is_valid or geometry.geom_type not in {
        "Polygon",
        "MultiPolygon",
    }:
        return None
    return geometry


def parse_geofabrik_extracts(index_payload: dict) -> list[dict]:
    """Parse downloadable PBF records from a supplied Geofabrik catalog payload."""
    if not isinstance(index_payload, dict) or index_payload.get("type") != "FeatureCollection":
        raise OSMExtractSourceError("Geofabrik index payload must be a GeoJSON FeatureCollection.")
    raw_features = index_payload.get("features")
    if not isinstance(raw_features, list):
        raise OSMExtractSourceError("Geofabrik index payload does not include features.")

    extracts = []
    for feature in raw_features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue
        urls = properties.get("urls")
        pbf_url = str(urls.get("pbf") or "").strip() if isinstance(urls, dict) else ""
        region_id = str(properties.get("id") or "").strip()
        coverage = _valid_coverage_geometry(feature.get("geometry"))
        if not region_id or not pbf_url or coverage is None:
            continue
        extracts.append(
            {
                "provider": OSM_SOURCE_PROVIDER,
                "source_type": OSM_SOURCE_TYPE,
                "region_id": region_id,
                "region_name": str(properties.get("name") or region_id),
                "parent_region_id": (
                    str(properties["parent"]).strip()
                    if properties.get("parent") is not None
                    else None
                ),
                "pbf_url": pbf_url,
                "coverage_geometry": feature["geometry"],
                "coverage_area_degrees": float(coverage.area),
            }
        )
    return sorted(extracts, key=lambda record: record["region_id"])


def _extent_geometry(required_extent: dict) -> BaseGeometry:
    geometry = _valid_coverage_geometry(required_extent.get("geometry"))
    if geometry is None:
        raise OSMExtractSourceError("Required OSM extent must contain a valid polygon geometry.")
    return geometry


def resolve_geofabrik_extract(
    extracts: list[dict],
    required_extent: dict,
    *,
    region_id: str | None = None,
) -> dict:
    """Resolve one complete regional PBF source without network or filesystem access."""
    extent_geometry = _extent_geometry(required_extent)
    requested_region_id = str(region_id or "").strip()
    valid_records = []
    for extract in extracts:
        coverage = _valid_coverage_geometry(extract.get("coverage_geometry"))
        if coverage is None:
            continue
        valid_records.append((extract, coverage))

    if requested_region_id:
        selected = next(
            (
                (extract, coverage)
                for extract, coverage in valid_records
                if extract.get("region_id") == requested_region_id
            ),
            None,
        )
        if selected is None:
            return {
                "run_status": "unresolved",
                "source": None,
                "warnings": [],
                "error": f"Requested Geofabrik region was not found: {requested_region_id}",
            }
        if not selected[1].covers(extent_geometry):
            return {
                "run_status": "unresolved",
                "source": None,
                "warnings": [],
                "error": (
                    f"Requested Geofabrik region {requested_region_id} does not cover "
                    "the movement extent plus search radius."
                ),
            }
        return {
            "run_status": "resolved",
            "source": deepcopy(selected[0]),
            "resolution_mode": "explicit_region",
            "candidate_region_ids": [requested_region_id],
            "warnings": [],
        }

    covering = [
        (extract, coverage)
        for extract, coverage in valid_records
        if coverage.covers(extent_geometry)
    ]
    if not covering:
        return {
            "run_status": "unresolved",
            "source": None,
            "warnings": [],
            "error": (
                "No single Geofabrik regional extract covers the movement extent plus "
                "search radius; multi-extract enrichment is not supported in this version."
            ),
        }
    covering.sort(
        key=lambda pair: (
            pair[1].area,
            str(pair[0].get("region_id") or ""),
        )
    )
    return {
        "run_status": "resolved",
        "source": deepcopy(covering[0][0]),
        "resolution_mode": "smallest_covering_region",
        "candidate_region_ids": [extract["region_id"] for extract, _ in covering],
        "warnings": [],
    }


def _combined_footprint_extent(footprints: list[dict], *, grid_degrees: float) -> dict:
    geometries = [_extent_geometry(footprint) for footprint in footprints]
    combined_geometry = unary_union(geometries)
    if (
        combined_geometry.is_empty
        or not combined_geometry.is_valid
        or combined_geometry.geom_type not in {"Polygon", "MultiPolygon"}
    ):
        raise OSMExtractSourceError(
            "Selected OSM footprints do not form a valid extraction geometry."
        )
    radius_values = [_finite_number(footprint.get("search_radius_m")) for footprint in footprints]
    if any(radius is None for radius in radius_values) or len(set(radius_values)) != 1:
        raise OSMExtractSourceError(
            "Selected OSM footprints must use one common search radius."
        )
    movement_bboxes = [footprint["movement_bbox"] for footprint in footprints]
    buffered_bboxes = [footprint["buffered_bbox"] for footprint in footprints]
    return {
        "coverage_model": OSM_COVERAGE_MODEL,
        "grid_degrees": float(grid_degrees),
        "search_radius_m": float(radius_values[0]),
        "valid_fix_count": sum(int(footprint["valid_fix_count"]) for footprint in footprints),
        "footprint_count": len(footprints),
        "footprint_ids": sorted(footprint["footprint_id"] for footprint in footprints),
        "movement_bbox": {
            "west": min(bbox["west"] for bbox in movement_bboxes),
            "south": min(bbox["south"] for bbox in movement_bboxes),
            "east": max(bbox["east"] for bbox in movement_bboxes),
            "north": max(bbox["north"] for bbox in movement_bboxes),
        },
        "buffered_bbox": {
            "west": min(bbox["west"] for bbox in buffered_bboxes),
            "south": min(bbox["south"] for bbox in buffered_bboxes),
            "east": max(bbox["east"] for bbox in buffered_bboxes),
            "north": max(bbox["north"] for bbox in buffered_bboxes),
        },
        "geometry": mapping(combined_geometry),
    }


def resolve_geofabrik_sources(extracts: list[dict], footprints: list[dict]) -> dict:
    """Resolve and consolidate complete PBF sources for occupied movement footprints."""
    if not footprints:
        return {
            "run_status": "unresolved",
            "coverage_model": OSM_COVERAGE_MODEL,
            "footprint_count": 0,
            "selected_region_ids": [],
            "selected_sources": [],
            "warnings": [],
            "error": "At least one occupied OSM footprint is required for source resolution.",
        }

    ordered_footprints = sorted(deepcopy(footprints), key=lambda footprint: footprint["footprint_id"])
    footprint_ids = [str(footprint.get("footprint_id") or "") for footprint in ordered_footprints]
    if not all(footprint_ids) or len(set(footprint_ids)) != len(footprint_ids):
        raise OSMExtractSourceError("OSM footprints must have unique non-empty footprint_id values.")
    grid_values = {
        _normalized_grid_degrees(footprint.get("grid_degrees", FOOTPRINT_GRID_DEGREES))
        for footprint in ordered_footprints
    }
    if len(grid_values) != 1:
        raise OSMExtractSourceError("OSM footprints must use one common grid_degrees value.")
    grid_degrees = grid_values.pop()

    candidate_sources: dict[str, dict] = {}
    initial_region_by_footprint = {}
    for footprint in ordered_footprints:
        resolution = resolve_geofabrik_extract(extracts, footprint)
        if resolution["run_status"] != "resolved":
            footprint_id = footprint["footprint_id"]
            return {
                "run_status": "unresolved",
                "coverage_model": OSM_COVERAGE_MODEL,
                "grid_degrees": grid_degrees,
                "footprint_count": len(ordered_footprints),
                "footprints": ordered_footprints,
                "initial_region_ids": sorted(candidate_sources),
                "selected_region_ids": [],
                "selected_sources": [],
                "warnings": [],
                "error": (
                    f"Could not resolve OSM source for footprint {footprint_id}: "
                    f"{resolution['error']}"
                ),
            }
        source = resolution["source"]
        candidate_sources[source["region_id"]] = source
        initial_region_by_footprint[footprint["footprint_id"]] = source["region_id"]

    chosen_region_ids = sorted(candidate_sources)
    if len(chosen_region_ids) > MAX_SELECTED_SOURCES:
        return {
            "run_status": "unresolved",
            "coverage_model": OSM_COVERAGE_MODEL,
            "grid_degrees": grid_degrees,
            "footprint_count": len(ordered_footprints),
            "footprints": ordered_footprints,
            "initial_region_ids": sorted(candidate_sources),
            "selected_region_ids": [],
            "selected_sources": [],
            "warnings": [],
            "error": (
                "Occupied OSM footprints require more than "
                f"{MAX_SELECTED_SOURCES} regional extracts."
            ),
        }

    assignments = {region_id: [] for region_id in chosen_region_ids}
    for footprint in ordered_footprints:
        assignments[initial_region_by_footprint[footprint["footprint_id"]]].append(footprint)

    selected_sources = []
    for region_id in sorted(chosen_region_ids):
        assigned_footprints = assignments[region_id]
        selected_source = deepcopy(candidate_sources[region_id])
        selected_source.update(
            {
                "assigned_footprint_ids": [
                    footprint["footprint_id"] for footprint in assigned_footprints
                ],
                "assigned_required_extent": _combined_footprint_extent(
                    assigned_footprints,
                    grid_degrees=grid_degrees,
                ),
            }
        )
        selected_sources.append(selected_source)

    return {
        "run_status": "resolved",
        "coverage_model": OSM_COVERAGE_MODEL,
        "grid_degrees": grid_degrees,
        "footprint_count": len(ordered_footprints),
        "planned_footprint_count": len(ordered_footprints),
        "excluded_footprint_count": 0,
        "excluded_footprints": [],
        "context_not_planned_fix_count": 0,
        "footprints": ordered_footprints,
        "initial_region_ids": sorted(candidate_sources),
        "selected_region_ids": sorted(chosen_region_ids),
        "selected_sources": selected_sources,
        "resolution_mode": "occupied_footprints_consolidated",
        "warnings": [],
    }


def _non_negative_int(raw_value: object) -> int | None:
    if isinstance(raw_value, bool):
        return None
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None


def _latest_cached_source_metadata_lightweight(
    data_root: Path,
    source: dict,
    *,
    cache_root: Path | None = None,
) -> dict | None:
    """Return recent matching source metadata without hashing large PBF files."""
    region_id = str(source.get("region_id") or "")
    region_dir = (
        osm_cache_root(data_root, cache_root=cache_root)
        / "sources"
        / OSM_SOURCE_PROVIDER
        / region_id
    )
    candidates = []
    for metadata_path in sorted(region_dir.glob("*/metadata.json")):
        metadata = _load_json(metadata_path)
        if metadata is None:
            continue
        source_revision = str(metadata.get("source_revision") or "")
        if (
            metadata.get("cache_valid") is not True
            or metadata.get("region_id") != source.get("region_id")
            or metadata.get("pbf_url") != source.get("pbf_url")
            or not source_revision
        ):
            continue
        paths = geofabrik_source_paths(
            data_root,
            region_id=region_id,
            source_revision=source_revision,
            cache_root=cache_root,
        )
        if paths["pbf"].is_file():
            candidates.append(metadata)
    if not candidates:
        return None
    candidates.sort(
        key=lambda metadata: (
            str(metadata.get("downloaded_at") or ""),
            str(metadata.get("source_revision") or ""),
        ),
        reverse=True,
    )
    return candidates[0]


def _inspect_source_size_for_guardrail(
    data_root: Path,
    source: dict,
    *,
    cache_root: Path | None = None,
    client: httpx.Client | None = None,
) -> dict:
    cached_metadata = _latest_cached_source_metadata_lightweight(
        data_root,
        source,
        cache_root=cache_root,
    )
    if cached_metadata is not None:
        content_length = _non_negative_int(cached_metadata.get("content_length"))
        return {
            "source_size_bytes": content_length,
            "source_size_known": content_length is not None,
            "source_size_status": (
                "cached_metadata" if content_length is not None else "cached_metadata_unknown_size"
            ),
            "cache_hit": True,
            "source_revision": str(cached_metadata.get("source_revision") or ""),
            "warnings": [],
        }

    active_client, owns_client = _http_client(client)
    try:
        try:
            response = active_client.head(source["pbf_url"])
            response.raise_for_status()
        except httpx.HTTPError as exc:
            return {
                "source_size_bytes": None,
                "source_size_known": False,
                "source_size_status": "head_failed",
                "cache_hit": False,
                "source_revision": "",
                "warnings": [
                    f"Could not inspect OSM source size for {source['region_id']}: {exc}"
                ],
            }
        content_length = _content_length(response.headers)
        return {
            "source_size_bytes": content_length,
            "source_size_known": content_length is not None,
            "source_size_status": "head" if content_length is not None else "head_unknown_size",
            "cache_hit": False,
            "source_revision": source_revision_key(
                pbf_url=source["pbf_url"],
                etag=response.headers.get("etag"),
                last_modified=response.headers.get("last-modified"),
            ),
            "warnings": [],
        }
    finally:
        if owns_client:
            active_client.close()


def _source_without_assignment(source: dict) -> dict:
    omitted = {"assigned_footprint_ids", "assigned_required_extent"}
    return deepcopy({key: value for key, value in source.items() if key not in omitted})


def _excluded_footprint_record(
    footprint: dict,
    *,
    source: dict,
    source_size: dict,
) -> dict:
    fix_keys = footprint.get("fix_keys") if isinstance(footprint.get("fix_keys"), list) else []
    return {
        "footprint_id": str(footprint["footprint_id"]),
        "valid_fix_count": int(footprint["valid_fix_count"]),
        "movement_bbox": deepcopy(footprint["movement_bbox"]),
        "buffered_bbox": deepcopy(footprint["buffered_bbox"]),
        "required_region_id": str(source["region_id"]),
        "region_id": str(source["region_id"]),
        "source_url": str(source.get("pbf_url") or ""),
        "source_size_bytes": source_size.get("source_size_bytes"),
        "source_size_known": bool(source_size.get("source_size_known")),
        "source_size_status": str(source_size.get("source_size_status") or "unknown"),
        "reason": TINY_FOOTPRINT_EXCLUSION_REASON,
        "fix_count_marked_context_not_planned": len(fix_keys) or int(footprint["valid_fix_count"]),
    }


def apply_tiny_footprint_source_guardrail(
    data_root: Path,
    resolved_plan: dict,
    *,
    cache_root: Path | None = None,
    client: httpx.Client | None = None,
    min_fixes_to_force_large_source: int = MIN_FIXES_TO_FORCE_LARGE_OSM_SOURCE,
    max_source_bytes_for_tiny_footprint: int = MAX_SOURCE_BYTES_FOR_TINY_FOOTPRINT,
) -> dict:
    """Exclude tiny footprints that would require huge or unknown-size OSM sources.

    The returned resolution keeps only planned footprints in selected_sources. Excluded
    footprints remain in metadata so callers can mark their fixes as context_not_planned.
    """
    if resolved_plan.get("run_status") != "resolved":
        return {
            "run_status": "unresolved",
            "resolution": resolved_plan,
            "excluded_footprints": [],
            "warnings": [],
            "error": resolved_plan.get("error") or "OSM source plan is not resolved.",
        }

    min_fixes = int(min_fixes_to_force_large_source)
    max_bytes = int(max_source_bytes_for_tiny_footprint)
    if min_fixes < 1:
        raise OSMExtractSourceError("Tiny-footprint guardrail minimum fix count must be positive.")
    if max_bytes < 0:
        raise OSMExtractSourceError("Tiny-footprint guardrail maximum source bytes must be non-negative.")

    sources = _multi_source_plan_sources(resolved_plan)
    footprints = {
        str(footprint.get("footprint_id") or ""): deepcopy(footprint)
        for footprint in resolved_plan.get("footprints", [])
        if isinstance(footprint, dict)
    }
    region_for_footprint: dict[str, str] = {}
    source_by_region: dict[str, dict] = {}
    for source in sources:
        region_id = str(source["region_id"])
        source_by_region[region_id] = source
        for footprint_id in source.get("assigned_footprint_ids") or []:
            region_for_footprint[str(footprint_id)] = region_id

    source_size_by_region: dict[str, dict] = {}
    excluded_by_id: dict[str, dict] = {}
    warnings: list[str] = []
    for source in sources:
        region_id = str(source["region_id"])
        assigned_footprints = [
            footprints[footprint_id]
            for footprint_id in source.get("assigned_footprint_ids") or []
            if footprint_id in footprints
        ]
        tiny_footprints = [
            footprint
            for footprint in assigned_footprints
            if int(footprint.get("valid_fix_count") or 0) < min_fixes
        ]
        if not tiny_footprints:
            continue
        source_size = source_size_by_region.get(region_id)
        if source_size is None:
            source_size = _inspect_source_size_for_guardrail(
                data_root,
                source,
                cache_root=cache_root,
                client=client,
            )
            source_size_by_region[region_id] = source_size
            warnings.extend(source_size.get("warnings", []))
        source_bytes = source_size.get("source_size_bytes")
        source_is_allowed = (
            source_size.get("source_size_known") is True
            and source_bytes is not None
            and int(source_bytes) <= max_bytes
        )
        if source_is_allowed:
            continue
        for footprint in tiny_footprints:
            excluded_by_id[str(footprint["footprint_id"])] = _excluded_footprint_record(
                footprint,
                source=source,
                source_size=source_size,
            )

    if not excluded_by_id:
        updated_resolution = deepcopy(resolved_plan)
        updated_resolution.setdefault("planned_footprint_count", len(footprints))
        updated_resolution.setdefault("excluded_footprint_count", 0)
        updated_resolution.setdefault("excluded_footprints", [])
        updated_resolution.setdefault("context_not_planned_fix_count", 0)
        return {
            "run_status": "completed",
            "resolution": updated_resolution,
            "excluded_footprints": [],
            "warnings": warnings,
        }

    planned_footprints = [
        deepcopy(footprint)
        for footprint_id, footprint in sorted(footprints.items())
        if footprint_id not in excluded_by_id
    ]
    excluded_footprints = [
        excluded_by_id[footprint_id]
        for footprint_id in sorted(excluded_by_id)
    ]
    context_not_planned_fix_count = sum(
        int(record["fix_count_marked_context_not_planned"])
        for record in excluded_footprints
    )
    if not planned_footprints:
        updated_resolution = deepcopy(resolved_plan)
        updated_resolution.update(
            {
                "run_status": "unresolved",
                "selected_region_ids": [],
                "selected_sources": [],
                "planned_footprint_count": 0,
                "excluded_footprint_count": len(excluded_footprints),
                "excluded_footprints": excluded_footprints,
                "context_not_planned_fix_count": context_not_planned_fix_count,
                "warnings": [*resolved_plan.get("warnings", []), *warnings],
                "error": (
                    "All occupied OSM footprints were excluded by the tiny/high-cost "
                    "source guardrail; no complete OSM context artifact was written."
                ),
            }
        )
        return {
            "run_status": "unresolved",
            "resolution": updated_resolution,
            "excluded_footprints": excluded_footprints,
            "warnings": warnings,
            "error": updated_resolution["error"],
        }

    grid_values = {
        _normalized_grid_degrees(footprint.get("grid_degrees", FOOTPRINT_GRID_DEGREES))
        for footprint in planned_footprints
    }
    if len(grid_values) != 1:
        raise OSMExtractSourceError("OSM footprints must use one common grid_degrees value.")
    grid_degrees = grid_values.pop()
    planned_by_region: dict[str, list[dict]] = {}
    for footprint in planned_footprints:
        region_id = region_for_footprint[str(footprint["footprint_id"])]
        planned_by_region.setdefault(region_id, []).append(footprint)

    selected_sources = []
    for region_id in sorted(planned_by_region):
        assigned_footprints = sorted(
            planned_by_region[region_id],
            key=lambda footprint: footprint["footprint_id"],
        )
        selected_source = _source_without_assignment(source_by_region[region_id])
        selected_source.update(
            {
                "assigned_footprint_ids": [
                    footprint["footprint_id"] for footprint in assigned_footprints
                ],
                "assigned_required_extent": _combined_footprint_extent(
                    assigned_footprints,
                    grid_degrees=grid_degrees,
                ),
            }
        )
        selected_sources.append(selected_source)

    updated_resolution = deepcopy(resolved_plan)
    updated_resolution.update(
        {
            "run_status": "resolved",
            "footprints": planned_footprints,
            "selected_region_ids": sorted(planned_by_region),
            "selected_sources": selected_sources,
            "planned_footprint_count": len(planned_footprints),
            "excluded_footprint_count": len(excluded_footprints),
            "excluded_footprints": excluded_footprints,
            "context_not_planned_fix_count": context_not_planned_fix_count,
            "warnings": [*resolved_plan.get("warnings", []), *warnings],
        }
    )
    return {
        "run_status": "completed",
        "resolution": updated_resolution,
        "excluded_footprints": excluded_footprints,
        "warnings": warnings,
    }


def osm_cache_root(data_root: Path, *, cache_root: Path | None = None) -> Path:
    if cache_root is not None:
        return Path(cache_root)
    return Path(data_root) / ".vibecleaning" / "osm"


def geofabrik_registry_paths(data_root: Path, *, cache_root: Path | None = None) -> dict[str, Path]:
    root = osm_cache_root(data_root, cache_root=cache_root) / "registry" / OSM_SOURCE_PROVIDER
    return {
        "directory": root,
        "index": root / "index-v1.json",
        "metadata": root / "metadata.json",
    }


def source_revision_key(
    *,
    pbf_url: str,
    etag: str | None = None,
    last_modified: str | None = None,
) -> str:
    canonical = json.dumps(
        {
            "pbf_url": str(pbf_url),
            "etag": str(etag or ""),
            "last_modified": str(last_modified or ""),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:20]


def geofabrik_source_paths(
    data_root: Path,
    *,
    region_id: str,
    source_revision: str,
    cache_root: Path | None = None,
) -> dict[str, Path]:
    root = (
        osm_cache_root(data_root, cache_root=cache_root)
        / "sources"
        / OSM_SOURCE_PROVIDER
        / str(region_id)
        / str(source_revision)
    )
    return {
        "directory": root,
        "pbf": root / "source.osm.pbf",
        "metadata": root / "metadata.json",
    }


def derived_extent_key(
    *,
    source_sha256: str,
    required_extent: dict,
    layer_spec_version: str = OSM_LAYER_SPEC_VERSION,
    extractor_version: str = OSM_EXTRACTOR_VERSION,
) -> str:
    canonical = json.dumps(
        {
            "source_sha256": str(source_sha256),
            "required_extent": required_extent,
            "layer_spec_version": str(layer_spec_version),
            "extractor_version": str(extractor_version),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:20]


def derived_feature_paths(
    data_root: Path,
    *,
    source_sha256: str,
    extent_key: str,
    cache_root: Path | None = None,
) -> dict[str, Path]:
    root = (
        osm_cache_root(data_root, cache_root=cache_root)
        / "derived"
        / str(source_sha256)
        / str(extent_key)
    )
    return {
        "directory": root,
        "features": root / "road_railway_features.geojsonl.gz",
        "metadata": root / "metadata.json",
    }


def registry_metadata(
    *,
    fetched_at: str,
    etag: str | None = None,
    last_modified: str | None = None,
    sha256: str | None = None,
    catalog_url: str = GEOFABRIK_INDEX_URL,
) -> dict:
    return {
        "schema_version": OSM_CACHE_SCHEMA_VERSION,
        "provider": OSM_SOURCE_PROVIDER,
        "catalog_url": str(catalog_url),
        "fetched_at": str(fetched_at),
        "etag": str(etag or ""),
        "last_modified": str(last_modified or ""),
        "sha256": str(sha256 or ""),
    }


def source_metadata(
    source: dict,
    *,
    source_revision: str,
    downloaded_at: str,
    sha256: str,
    content_length: int | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    cache_valid: bool = False,
) -> dict:
    return {
        "schema_version": OSM_CACHE_SCHEMA_VERSION,
        "provider": OSM_SOURCE_PROVIDER,
        "source_type": OSM_SOURCE_TYPE,
        "region_id": source["region_id"],
        "region_name": source["region_name"],
        "pbf_url": source["pbf_url"],
        "coverage_geometry": source["coverage_geometry"],
        "source_revision": str(source_revision),
        "downloaded_at": str(downloaded_at),
        "sha256": str(sha256),
        "content_length": content_length,
        "etag": str(etag or ""),
        "last_modified": str(last_modified or ""),
        "cache_valid": bool(cache_valid),
    }


def derived_metadata(
    *,
    source_sha256: str,
    source_region_id: str,
    required_extent: dict,
    feature_counts: dict[str, int] | None = None,
    output_sha256: str = "",
    created_at: str = "",
    complete: bool = False,
    warnings: list[str] | None = None,
) -> dict:
    return {
        "schema_version": OSM_CACHE_SCHEMA_VERSION,
        "source_sha256": str(source_sha256),
        "source_region_id": str(source_region_id),
        "required_extent": deepcopy(required_extent),
        "layers": production_layer_specs(),
        "layer_spec_version": OSM_LAYER_SPEC_VERSION,
        "extractor_version": OSM_EXTRACTOR_VERSION,
        "feature_counts": {
            layer_name: int((feature_counts or {}).get(layer_name, 0))
            for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES
        },
        "output_sha256": str(output_sha256),
        "created_at": str(created_at),
        "complete": bool(complete),
        "warnings": list(warnings or []),
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict | None:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_bytes_atomic(path: Path, content: bytes):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary.write(content)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def _write_json_atomic(path: Path, payload: dict):
    content = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    _write_bytes_atomic(path, content)


def _http_timeout(*, read_timeout_seconds: float = HTTP_TIMEOUT_SECONDS) -> httpx.Timeout:
    return httpx.Timeout(
        connect=HTTP_TIMEOUT_SECONDS,
        read=read_timeout_seconds,
        write=HTTP_TIMEOUT_SECONDS,
        pool=HTTP_TIMEOUT_SECONDS,
    )


def _http_client(
    client: httpx.Client | None,
    *,
    read_timeout_seconds: float = HTTP_TIMEOUT_SECONDS,
) -> tuple[httpx.Client, bool]:
    if client is not None:
        return client, False
    return httpx.Client(
        follow_redirects=True,
        timeout=_http_timeout(read_timeout_seconds=read_timeout_seconds),
    ), True


def get_geofabrik_registry(
    data_root: Path,
    *,
    cache_root: Path | None = None,
    client: httpx.Client | None = None,
    refresh: bool = False,
    fetched_at: str | None = None,
    catalog_url: str = GEOFABRIK_INDEX_URL,
) -> dict:
    """Get a cached Geofabrik catalog or retrieve and atomically cache a fresh copy."""
    paths = geofabrik_registry_paths(data_root, cache_root=cache_root)
    if not refresh:
        cached_content = None
        try:
            cached_content = paths["index"].read_bytes()
        except OSError:
            pass
        cached_metadata = _load_json(paths["metadata"])
        if cached_content is not None and cached_metadata is not None:
            try:
                cached_payload = json.loads(cached_content)
                extracts = parse_geofabrik_extracts(cached_payload)
            except (json.JSONDecodeError, OSMExtractSourceError):
                extracts = []
            if (
                extracts
                and cached_metadata.get("catalog_url") == catalog_url
                and cached_metadata.get("sha256") == _sha256_bytes(cached_content)
            ):
                return {
                    "run_status": "completed",
                    "cache_hit": True,
                    "paths": paths,
                    "payload": cached_payload,
                    "extracts": extracts,
                    "metadata": cached_metadata,
                    "warnings": [],
                }

    active_client, owns_client = _http_client(client)
    try:
        response = active_client.get(catalog_url)
        response.raise_for_status()
        content = response.content
        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise OSMExtractSourceError("Geofabrik catalog response is not valid JSON.") from exc
        extracts = parse_geofabrik_extracts(payload)
        if not extracts:
            raise OSMExtractSourceError(
                "Geofabrik catalog response did not contain downloadable regional extracts."
            )
        metadata = registry_metadata(
            fetched_at=fetched_at or _now_iso(),
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
            sha256=_sha256_bytes(content),
            catalog_url=catalog_url,
        )
        _write_bytes_atomic(paths["index"], content)
        _write_json_atomic(paths["metadata"], metadata)
        return {
            "run_status": "completed",
            "cache_hit": False,
            "paths": paths,
            "payload": payload,
            "extracts": extracts,
            "metadata": metadata,
            "warnings": [],
        }
    except httpx.HTTPError as exc:
        raise OSMExtractSourceError(f"Could not retrieve Geofabrik catalog: {exc}") from exc
    finally:
        if owns_client:
            active_client.close()


def _source_cache_record(
    data_root: Path,
    source: dict,
    metadata: dict,
    *,
    cache_root: Path | None = None,
) -> dict | None:
    source_revision = str(metadata.get("source_revision") or "")
    if not source_revision:
        return None
    paths = geofabrik_source_paths(
        data_root,
        region_id=str(source.get("region_id") or ""),
        source_revision=source_revision,
        cache_root=cache_root,
    )
    if (
        metadata.get("cache_valid") is not True
        or metadata.get("region_id") != source.get("region_id")
        or metadata.get("pbf_url") != source.get("pbf_url")
        or not paths["pbf"].is_file()
        or metadata.get("sha256") != _sha256_path(paths["pbf"])
    ):
        return None
    return {
        "run_status": "completed",
        "cache_hit": True,
        "source": deepcopy(source),
        "source_revision": source_revision,
        "paths": paths,
        "metadata": metadata,
        "warnings": [],
    }


def find_cached_geofabrik_source(
    data_root: Path,
    source: dict,
    *,
    cache_root: Path | None = None,
) -> dict | None:
    region_id = str(source.get("region_id") or "")
    region_dir = (
        osm_cache_root(data_root, cache_root=cache_root)
        / "sources"
        / OSM_SOURCE_PROVIDER
        / region_id
    )
    candidates = []
    for metadata_path in sorted(region_dir.glob("*/metadata.json")):
        metadata = _load_json(metadata_path)
        if metadata is None:
            continue
        record = _source_cache_record(data_root, source, metadata, cache_root=cache_root)
        if record is not None:
            candidates.append(record)
    if not candidates:
        return None
    candidates.sort(
        key=lambda record: (
            str(record["metadata"].get("downloaded_at") or ""),
            str(record["source_revision"]),
        ),
        reverse=True,
    )
    return candidates[0]


def _content_length(headers: httpx.Headers) -> int | None:
    raw_value = headers.get("content-length")
    try:
        value = int(raw_value) if raw_value is not None else None
    except ValueError:
        return None
    return value if value is not None and value >= 0 else None


def cache_geofabrik_source_pbf(
    data_root: Path,
    source: dict,
    *,
    cache_root: Path | None = None,
    client: httpx.Client | None = None,
    confirmed_large_download: bool = False,
    downloaded_at: str | None = None,
    max_unconfirmed_bytes: int = MAX_UNCONFIRMED_DOWNLOAD_BYTES,
    max_download_attempts: int = PBF_DOWNLOAD_MAX_ATTEMPTS,
) -> dict:
    """Cache one source PBF; never download a large/unknown-size uncached source unconfirmed."""
    cached = find_cached_geofabrik_source(data_root, source, cache_root=cache_root)
    if cached is not None:
        return cached

    active_client, owns_client = _http_client(
        client,
        read_timeout_seconds=PBF_DOWNLOAD_READ_TIMEOUT_SECONDS,
    )
    try:
        try:
            head_response = active_client.head(source["pbf_url"])
            head_response.raise_for_status()
        except httpx.HTTPError as exc:
            raise OSMExtractSourceError(
                f"Could not inspect Geofabrik source {source['region_id']}: {exc}"
            ) from exc
        content_length = _content_length(head_response.headers)
        etag = head_response.headers.get("etag")
        last_modified = head_response.headers.get("last-modified")
        source_revision = source_revision_key(
            pbf_url=source["pbf_url"],
            etag=etag,
            last_modified=last_modified,
        )
        paths = geofabrik_source_paths(
            data_root,
            region_id=source["region_id"],
            source_revision=source_revision,
            cache_root=cache_root,
        )
        if (
            not confirmed_large_download
            and (content_length is None or content_length > int(max_unconfirmed_bytes))
        ):
            return {
                "run_status": "confirmation_required",
                "cache_hit": False,
                "source": deepcopy(source),
                "source_revision": source_revision,
                "paths": paths,
                "estimated_content_length": content_length,
                "confirmation_threshold_bytes": int(max_unconfirmed_bytes),
                "warnings": [
                    "This regional OSM source requires confirmation before downloading."
                ],
            }

        paths["directory"].mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        downloaded_size = 0
        download_warnings: list[str] = []
        attempts = max(1, int(max_download_attempts))
        for attempt in range(1, attempts + 1):
            temporary_path = None
            digest = hashlib.sha256()
            downloaded_size = 0
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    dir=paths["directory"],
                    prefix=".source.osm.pbf.",
                    suffix=".tmp",
                    delete=False,
                ) as temporary:
                    temporary_path = Path(temporary.name)
                    with active_client.stream("GET", source["pbf_url"]) as response:
                        response.raise_for_status()
                        for chunk in response.iter_bytes():
                            temporary.write(chunk)
                            digest.update(chunk)
                            downloaded_size += len(chunk)
                    temporary.flush()
                    os.fsync(temporary.fileno())
                if content_length is not None and downloaded_size != content_length:
                    raise OSMExtractSourceError(
                        f"Downloaded Geofabrik source size mismatch for {source['region_id']}."
                    )
                os.replace(temporary_path, paths["pbf"])
                temporary_path = None
                break
            except httpx.HTTPError as exc:
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)
                if attempt >= attempts:
                    raise
                download_warnings.append(
                    "Retrying Geofabrik source download for "
                    f"{source['region_id']} after {exc.__class__.__name__} "
                    f"(attempt {attempt} of {attempts})."
                )
            except Exception:
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)
                raise

        metadata = source_metadata(
            source,
            source_revision=source_revision,
            downloaded_at=downloaded_at or _now_iso(),
            sha256=digest.hexdigest(),
            content_length=downloaded_size,
            etag=etag,
            last_modified=last_modified,
            cache_valid=True,
        )
        _write_json_atomic(paths["metadata"], metadata)
        return {
            "run_status": "completed",
            "cache_hit": False,
            "source": deepcopy(source),
            "source_revision": source_revision,
            "paths": paths,
            "metadata": metadata,
            "warnings": download_warnings,
        }
    except httpx.HTTPError as exc:
        raise OSMExtractSourceError(
            f"Could not download Geofabrik source {source['region_id']}: {exc}"
        ) from exc
    finally:
        if owns_client:
            active_client.close()


def _validate_complete_derived_cache(
    paths: dict[str, Path],
    *,
    source_sha256: str,
    required_extent: dict,
) -> dict | None:
    metadata = _load_json(paths["metadata"])
    if (
        metadata is None
        or metadata.get("complete") is not True
        or metadata.get("source_sha256") != source_sha256
        or _canonical_json(metadata.get("required_extent")) != _canonical_json(required_extent)
        or metadata.get("layer_spec_version") != OSM_LAYER_SPEC_VERSION
        or metadata.get("extractor_version") != OSM_EXTRACTOR_VERSION
        or not paths["features"].is_file()
        or metadata.get("output_sha256") != _sha256_path(paths["features"])
    ):
        return None
    return metadata


def _write_gzip_features_atomic(path: Path, features: list[dict]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            with gzip.GzipFile(fileobj=temporary, mode="wb", filename="", mtime=0) as output:
                for feature in features:
                    output.write(
                        (json.dumps(feature, sort_keys=True, separators=(",", ":")) + "\n").encode(
                            "utf-8"
                        )
                    )
            temporary.flush()
            os.fsync(temporary.fileno())
        digest = _sha256_path(temporary_path)
        os.replace(temporary_path, path)
        return digest
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def extract_context_feature_cache(
    data_root: Path,
    source_cache: dict,
    required_extent: dict,
    *,
    cache_root: Path | None = None,
    created_at: str | None = None,
) -> dict:
    """Extract clipped road/railway line features from a valid locally cached PBF."""
    source_metadata_payload = source_cache.get("metadata")
    paths = source_cache.get("paths")
    if not isinstance(source_metadata_payload, dict) or not isinstance(paths, dict):
        raise OSMExtractSourceError("Local PBF source cache information is incomplete.")
    source_path = Path(paths["pbf"])
    source_sha256 = str(source_metadata_payload.get("sha256") or "")
    if (
        source_metadata_payload.get("cache_valid") is not True
        or not source_path.is_file()
        or not source_sha256
        or _sha256_path(source_path) != source_sha256
    ):
        raise OSMExtractSourceError("Local PBF source cache is not valid.")
    extent_geometry = _extent_geometry(required_extent)
    source_coverage = _valid_coverage_geometry(source_metadata_payload.get("coverage_geometry"))
    if source_coverage is None or not source_coverage.covers(extent_geometry):
        raise OSMExtractSourceError(
            "Local PBF source coverage does not include the required buffered movement extent."
        )
    source_content_length = _finite_number(source_metadata_payload.get("content_length"))
    location_index = (
        "sparse_file_array"
        if source_content_length is not None and source_content_length > MAX_MEMORY_LOCATION_INDEX_BYTES
        else "flex_mem"
    )
    extent_key = derived_extent_key(
        source_sha256=source_sha256,
        required_extent=required_extent,
    )
    derived_paths = derived_feature_paths(
        data_root,
        source_sha256=source_sha256,
        extent_key=extent_key,
        cache_root=cache_root,
    )
    cached_metadata = _validate_complete_derived_cache(
        derived_paths,
        source_sha256=source_sha256,
        required_extent=required_extent,
    )
    if cached_metadata is not None:
        return {
            "run_status": "completed",
            "cache_hit": True,
            "paths": derived_paths,
            "metadata": cached_metadata,
            "warnings": [],
        }

    try:
        import osmium
    except ImportError as exc:
        raise OSMExtractSourceError(
            "The osmium dependency is required to extract local OSM PBF context features."
        ) from exc

    extracted_features: list[dict] = []
    layer_specs = production_layer_specs()

    class ContextFeatureHandler(osmium.SimpleHandler):
        def way(self, way):
            tags = dict(way.tags)
            matching_specs = [
                spec for spec in layer_specs if str(tags.get(spec["class_tag"]) or "")
            ]
            if not matching_specs:
                return
            try:
                coordinates = [(float(node.lon), float(node.lat)) for node in way.nodes]
            except osmium.InvalidLocationError as exc:
                raise OSMExtractSourceError(
                    f"Tagged OSM way {way.id} has missing node coordinates."
                ) from exc
            if len(coordinates) < 2:
                raise OSMExtractSourceError(
                    f"Tagged OSM way {way.id} has insufficient coordinates."
                )
            geometry = shape({"type": "LineString", "coordinates": coordinates})
            if not geometry.intersects(extent_geometry):
                return
            for spec in matching_specs:
                layer_name = spec["layer_name"]
                class_tag = spec["class_tag"]
                extracted_features.append(
                    {
                        "type": "Feature",
                        "id": f"way/{way.id}:{layer_name}",
                        "geometry": mapping(geometry),
                        "properties": {
                            "osm_id": int(way.id),
                            "osm_type": "way",
                            "layer_name": layer_name,
                            "tags": {class_tag: str(tags[class_tag])},
                        },
                    }
                )

    try:
        ContextFeatureHandler().apply_file(str(source_path), locations=True, idx=location_index)
    except OSMExtractSourceError:
        raise
    except Exception as exc:
        raise OSMExtractSourceError(
            f"Could not extract local OSM context features from {source_path.name}."
        ) from exc

    extracted_features.sort(key=lambda feature: str(feature["id"]))
    feature_counts = {
        layer_name: sum(
            feature["properties"]["layer_name"] == layer_name
            for feature in extracted_features
        )
        for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES
    }
    output_sha256 = _write_gzip_features_atomic(derived_paths["features"], extracted_features)
    metadata = derived_metadata(
        source_sha256=source_sha256,
        source_region_id=str(source_metadata_payload.get("region_id") or ""),
        required_extent=required_extent,
        feature_counts=feature_counts,
        output_sha256=output_sha256,
        created_at=created_at or _now_iso(),
        complete=True,
    )
    _write_json_atomic(derived_paths["metadata"], metadata)
    return {
        "run_status": "completed",
        "cache_hit": False,
        "paths": derived_paths,
        "metadata": metadata,
        "warnings": [],
    }


def load_cached_context_features(derived_cache: dict) -> dict[str, list[dict]]:
    """Load a completed derived feature cache grouped for osm_context consumption."""
    metadata = derived_cache.get("metadata")
    paths = derived_cache.get("paths")
    if not isinstance(metadata, dict) or not isinstance(paths, dict):
        raise OSMExtractSourceError("Derived OSM context cache information is incomplete.")
    features_path = Path(paths["features"])
    if (
        metadata.get("complete") is not True
        or not features_path.is_file()
        or metadata.get("output_sha256") != _sha256_path(features_path)
    ):
        raise OSMExtractSourceError("Derived OSM context feature cache is not valid.")
    grouped = {layer_name: [] for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES}
    with gzip.open(features_path, "rt", encoding="utf-8") as input_file:
        for line in input_file:
            feature = json.loads(line)
            layer_name = (
                feature.get("properties", {}).get("layer_name")
                if isinstance(feature, dict)
                else None
            )
            if layer_name not in grouped:
                raise OSMExtractSourceError(
                    f"Derived OSM context cache contains unsupported layer: {layer_name}"
                )
            grouped[layer_name].append(feature)
    return grouped


def _multi_source_plan_sources(resolved_plan: dict) -> list[dict]:
    if resolved_plan.get("run_status") != "resolved":
        raise OSMExtractSourceError(
            "Multi-source OSM cache preparation requires a resolved source plan."
        )
    raw_sources = resolved_plan.get("selected_sources")
    if not isinstance(raw_sources, list) or not raw_sources:
        raise OSMExtractSourceError(
            "Resolved multi-source OSM plan does not contain selected sources."
        )
    sources = []
    region_ids = set()
    for raw_source in raw_sources:
        if not isinstance(raw_source, dict):
            raise OSMExtractSourceError(
                "Resolved multi-source OSM plan contains an invalid selected source."
            )
        region_id = str(raw_source.get("region_id") or "")
        if not region_id or region_id in region_ids:
            raise OSMExtractSourceError(
                "Resolved multi-source OSM plan must contain unique source region IDs."
            )
        _extent_geometry(raw_source.get("assigned_required_extent") or {})
        region_ids.add(region_id)
        sources.append(deepcopy(raw_source))
    return sorted(sources, key=lambda source: source["region_id"])


def preflight_multi_source_caches(
    data_root: Path,
    resolved_plan: dict,
    *,
    cache_root: Path | None = None,
    client: httpx.Client | None = None,
    confirmed_large_download: bool = False,
    max_unconfirmed_bytes: int = MAX_UNCONFIRMED_DOWNLOAD_BYTES,
    max_unconfirmed_sources: int = MAX_UNCONFIRMED_SOURCE_COUNT,
) -> dict:
    """Inspect every selected source before any multi-source PBF download occurs."""
    sources = _multi_source_plan_sources(resolved_plan)
    source_records = []
    active_client, owns_client = _http_client(client)
    try:
        for source in sources:
            cached_source = find_cached_geofabrik_source(data_root, source, cache_root=cache_root)
            if cached_source is not None:
                source_records.append(
                    {
                        "region_id": source["region_id"],
                        "source": source,
                        "cache_hit": True,
                        "download_required": False,
                        "estimated_content_length": 0,
                        "source_revision": cached_source["source_revision"],
                        "cached_source": cached_source,
                    }
                )
                continue
            try:
                response = active_client.head(source["pbf_url"])
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise OSMExtractSourceError(
                    f"Could not inspect Geofabrik source {source['region_id']}: {exc}"
                ) from exc
            estimated_content_length = _content_length(response.headers)
            source_records.append(
                {
                    "region_id": source["region_id"],
                    "source": source,
                    "cache_hit": False,
                    "download_required": True,
                    "estimated_content_length": estimated_content_length,
                    "source_revision": source_revision_key(
                        pbf_url=source["pbf_url"],
                        etag=response.headers.get("etag"),
                        last_modified=response.headers.get("last-modified"),
                    ),
                    "cached_source": None,
                }
            )
    finally:
        if owns_client:
            active_client.close()

    uncached_records = [
        record for record in source_records if record["download_required"]
    ]
    unknown_size_regions = [
        record["region_id"]
        for record in uncached_records
        if record["estimated_content_length"] is None
    ]
    known_download_bytes = sum(
        int(record["estimated_content_length"])
        for record in uncached_records
        if record["estimated_content_length"] is not None
    )
    aggregate_download_bytes = (
        None if unknown_size_regions else known_download_bytes
    )
    confirmation_reasons = []
    if unknown_size_regions:
        confirmation_reasons.append(
            "Download size is unknown for: " + ", ".join(sorted(unknown_size_regions)) + "."
        )
    if (
        aggregate_download_bytes is not None
        and aggregate_download_bytes > int(max_unconfirmed_bytes)
    ):
        confirmation_reasons.append(
            "Aggregate uncached OSM download size exceeds "
            f"{int(max_unconfirmed_bytes)} bytes."
        )
    if len(uncached_records) > int(max_unconfirmed_sources):
        confirmation_reasons.append(
            "The plan requires downloading more than "
            f"{int(max_unconfirmed_sources)} uncached OSM regional sources."
        )
    confirmation_required = bool(confirmation_reasons) and not confirmed_large_download
    return {
        "run_status": "confirmation_required" if confirmation_required else "completed",
        "source_records": source_records,
        "selected_source_count": len(source_records),
        "cached_source_count": len(source_records) - len(uncached_records),
        "uncached_source_count": len(uncached_records),
        "known_uncached_download_bytes": known_download_bytes,
        "aggregate_uncached_download_bytes": aggregate_download_bytes,
        "confirmation_required": confirmation_required,
        "confirmation_reasons": confirmation_reasons,
        "warnings": list(confirmation_reasons) if confirmed_large_download else [],
    }


def _feature_identity(layer_name: str, feature: dict) -> tuple[str, str, str]:
    properties = feature.get("properties") if isinstance(feature, dict) else None
    properties = properties if isinstance(properties, dict) else {}
    osm_type = str(properties.get("osm_type") or "")
    osm_id = str(properties.get("osm_id") or "")
    if osm_type and osm_id:
        return layer_name, osm_type, osm_id
    return layer_name, "feature", _canonical_json(feature)


def prepare_multi_source_feature_caches(
    data_root: Path,
    resolved_plan: dict,
    *,
    cache_root: Path | None = None,
    client: httpx.Client | None = None,
    confirmed_large_download: bool = False,
    downloaded_at: str | None = None,
    created_at: str | None = None,
    max_unconfirmed_bytes: int = MAX_UNCONFIRMED_DOWNLOAD_BYTES,
    max_unconfirmed_sources: int = MAX_UNCONFIRMED_SOURCE_COUNT,
) -> dict:
    """Prepare complete per-source feature caches and merge them for context lookup."""
    try:
        preflight = preflight_multi_source_caches(
            data_root,
            resolved_plan,
            cache_root=cache_root,
            client=client,
            confirmed_large_download=confirmed_large_download,
            max_unconfirmed_bytes=max_unconfirmed_bytes,
            max_unconfirmed_sources=max_unconfirmed_sources,
        )
    except OSMExtractSourceError as exc:
        return {
            "run_status": "failed",
            "complete": False,
            "features_by_layer": None,
            "regional_caches": [],
            "warnings": [],
            "error": str(exc),
        }
    if preflight["run_status"] == "confirmation_required":
        return {
            "run_status": "confirmation_required",
            "complete": False,
            "features_by_layer": None,
            "regional_caches": [],
            "preflight": preflight,
            "warnings": list(preflight["confirmation_reasons"]),
        }

    regional_caches = []
    loaded_regions = []
    for record in preflight["source_records"]:
        source = record["source"]
        region_id = source["region_id"]
        try:
            source_cache = record["cached_source"] or cache_geofabrik_source_pbf(
                data_root,
                source,
                cache_root=cache_root,
                client=client,
                confirmed_large_download=True,
                downloaded_at=downloaded_at,
                max_unconfirmed_bytes=max_unconfirmed_bytes,
            )
            if source_cache["run_status"] != "completed":
                raise OSMExtractSourceError(
                    f"OSM source cache did not complete for region {region_id}."
                )
            feature_cache = extract_context_feature_cache(
                data_root,
                source_cache,
                source["assigned_required_extent"],
                cache_root=cache_root,
                created_at=created_at,
            )
            if (
                feature_cache["run_status"] != "completed"
                or feature_cache["metadata"].get("complete") is not True
            ):
                raise OSMExtractSourceError(
                    f"OSM feature cache did not complete for region {region_id}."
                )
            features_by_layer = load_cached_context_features(feature_cache)
        except OSMExtractSourceError as exc:
            return {
                "run_status": "failed",
                "complete": False,
                "features_by_layer": None,
                "regional_caches": regional_caches,
                "preflight": preflight,
                "warnings": [],
                "error": f"OSM context extraction failed for region {region_id}: {exc}",
            }
        regional_caches.append(
            {
                "region_id": region_id,
                "source_cache_hit": bool(source_cache["cache_hit"]),
                "source_revision": source_cache["source_revision"],
                "feature_cache_hit": bool(feature_cache["cache_hit"]),
                "feature_counts": dict(feature_cache["metadata"]["feature_counts"]),
                "duplicate_counts": {
                    layer_name: 0 for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES
                },
                "assigned_footprint_ids": list(source.get("assigned_footprint_ids") or []),
                "source_cache": source_cache,
                "feature_cache": feature_cache,
                "warnings": [
                    *source_cache.get("warnings", []),
                    *feature_cache.get("warnings", []),
                ],
            }
        )
        loaded_regions.append((region_id, features_by_layer))

    merged_features = {layer_name: [] for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES}
    duplicate_counts = {layer_name: 0 for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES}
    seen_feature_ids = set()
    cache_by_region = {record["region_id"]: record for record in regional_caches}
    for region_id, features_by_layer in sorted(loaded_regions, key=lambda item: item[0]):
        for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES:
            for feature in features_by_layer[layer_name]:
                identity = _feature_identity(layer_name, feature)
                if identity in seen_feature_ids:
                    duplicate_counts[layer_name] += 1
                    cache_by_region[region_id]["duplicate_counts"][layer_name] += 1
                    continue
                seen_feature_ids.add(identity)
                merged_features[layer_name].append(feature)
    for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES:
        merged_features[layer_name].sort(
            key=lambda feature: _feature_identity(layer_name, feature)
        )
    feature_counts = {
        layer_name: len(merged_features[layer_name])
        for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES
    }
    for regional_cache in regional_caches:
        regional_cache["duplicate_counts"]["total"] = sum(
            regional_cache["duplicate_counts"][layer_name]
            for layer_name in SUPPORTED_CONTEXT_LAYER_NAMES
        )
    return {
        "run_status": "completed",
        "complete": True,
        "features_by_layer": merged_features,
        "feature_counts": feature_counts,
        "duplicate_counts": {
            **duplicate_counts,
            "total": sum(duplicate_counts.values()),
        },
        "regional_caches": regional_caches,
        "preflight": preflight,
        "warnings": [
            *preflight["warnings"],
            *[
                warning
                for regional_cache in regional_caches
                for warning in regional_cache["warnings"]
            ],
        ],
    }
