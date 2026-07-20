import hashlib
import json
import math
from math import isfinite

from app.osm import EARTH_RADIUS_M


DEFAULT_CONTEXT_TILE_SIZE_M = 4_000.0
DEFAULT_CONTEXT_MAX_SUBSCOPES = 500
MAX_CONTEXT_SUBDIVISION_DEPTH = 3
MAX_CONTEXT_FETCH_ATTEMPTS = 96
MAX_CONTEXT_INDEX_CELLS_PER_FEATURE = 10_000
# Keep buffered broad-exists bbox fetches below app.osm's 25 km^2 limit.
MAX_CONTEXT_SEARCH_RADIUS_M = 2_450.0
OSM_CONTEXT_LAYER_SPECS = {
    "road": {
        "layer_name": "road",
        "selectors": [{"tags": [{"key": "highway", "op": "exists"}]}],
        "element_types": ["way"],
        "class_tag": "highway",
        "output_columns": {
            "distance_m": "osm:nearest_road_distance_m",
            "class": "osm:nearest_road_class",
            "match_status": "osm:road_match_status",
        },
    },
    "railway": {
        "layer_name": "railway",
        "selectors": [{"tags": [{"key": "railway", "op": "exists"}]}],
        "element_types": ["way"],
        "class_tag": "railway",
        "output_columns": {
            "distance_m": "osm:nearest_railway_distance_m",
            "class": "osm:nearest_railway_class",
            "match_status": "osm:railway_match_status",
        },
    },
}
MATCHED_STATUS = "matched"
NOT_FOUND_STATUS = "not_found_within_radius"


def _finite_number(raw_value: object) -> float | None:
    if isinstance(raw_value, bool):
        return None
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    return value if isfinite(value) else None


def normalize_search_radius_m(raw_value: object) -> float:
    """Validate a radius used by the current bounded Overpass enrichment path."""
    radius_m = normalize_local_search_radius_m(raw_value)
    if radius_m > MAX_CONTEXT_SEARCH_RADIUS_M:
        raise ValueError(f"search_radius_m must be <= {int(MAX_CONTEXT_SEARCH_RADIUS_M)}")
    return radius_m


def normalize_local_search_radius_m(raw_value: object) -> float:
    """Validate a radius for local context calculations without fetch-size limits."""
    radius_m = _finite_number(raw_value)
    if radius_m is None or radius_m <= 0:
        raise ValueError("search_radius_m must be a positive finite number")
    return radius_m


def build_fix_osm_context(
    fixes: list[dict],
    features_by_layer: dict[str, list[dict]],
    *,
    search_radius_m: float,
    layer_specs: list[dict] | tuple[dict, ...] | None = None,
) -> dict[str, dict[str, float | str | None]]:
    """Return pure fix-keyed context values from already-fetched layer features."""
    radius_m = normalize_local_search_radius_m(search_radius_m)
    specs = list(layer_specs) if layer_specs is not None else list(OSM_CONTEXT_LAYER_SPECS.values())

    normalized_fixes = []
    for fix in fixes:
        fix_key = str(fix.get("fix_key") or "").strip()
        lon = _finite_number(fix.get("lon"))
        lat = _finite_number(fix.get("lat"))
        if not fix_key or lon is None or lat is None:
            raise ValueError("Context enrichment requires normalized fixes with fix_key, lon, and lat")
        normalized_fixes.append((fix_key, lon, lat))
    if not normalized_fixes:
        return {}

    reference_lat = sum(lat for _, _, lat in normalized_fixes) / len(normalized_fixes)
    indexed_specs = []
    for spec in specs:
        layer_name = str(spec["layer_name"])
        features = features_by_layer.get(layer_name, [])
        if not isinstance(features, list):
            features = []
        indexed_specs.append(
            (
                spec,
                _build_feature_spatial_index(
                    features,
                    search_radius_m=radius_m,
                    reference_lat=reference_lat,
                ),
            )
        )

    context_by_fix_key = {}
    for fix_key, lon, lat in normalized_fixes:
        context = {}
        for spec, feature_index in indexed_specs:
            output_columns = spec["output_columns"]
            nearest = nearest_osm_feature(lon, lat, _candidate_features(feature_index, lon, lat))
            if nearest is None or nearest["distance_m"] > radius_m:
                context[output_columns["distance_m"]] = None
                context[output_columns["class"]] = ""
                context[output_columns["match_status"]] = NOT_FOUND_STATUS
                continue
            properties = nearest["properties"]
            tags = properties.get("tags") if isinstance(properties.get("tags"), dict) else {}
            context[output_columns["distance_m"]] = float(nearest["distance_m"])
            context[output_columns["class"]] = str(tags.get(spec["class_tag"]) or "")
            context[output_columns["match_status"]] = MATCHED_STATUS
        context_by_fix_key[fix_key] = context
    return context_by_fix_key


def _build_feature_spatial_index(
    features: list[dict],
    *,
    search_radius_m: float,
    reference_lat: float,
) -> dict:
    cell_size_m = max(100.0, search_radius_m * 2.0)
    cells: dict[tuple[int, int], list[dict]] = {}
    global_features = []
    for feature in features:
        bounds = _feature_projected_bounds(feature, reference_lat)
        if bounds is None:
            continue
        min_x, min_y, max_x, max_y = bounds
        min_col = math.floor((min_x - search_radius_m) / cell_size_m)
        max_col = math.floor((max_x + search_radius_m) / cell_size_m)
        min_row = math.floor((min_y - search_radius_m) / cell_size_m)
        max_row = math.floor((max_y + search_radius_m) / cell_size_m)
        cell_count = (max_col - min_col + 1) * (max_row - min_row + 1)
        if cell_count > MAX_CONTEXT_INDEX_CELLS_PER_FEATURE:
            global_features.append(feature)
            continue
        for col in range(min_col, max_col + 1):
            for row in range(min_row, max_row + 1):
                cells.setdefault((col, row), []).append(feature)
    return {
        "cell_size_m": cell_size_m,
        "reference_lat": reference_lat,
        "cells": cells,
        "global_features": global_features,
    }


def _candidate_features(index: dict, lon: float, lat: float) -> list[dict]:
    x, y = project_lon_lat(lon, lat, index["reference_lat"])
    key = (
        math.floor(x / index["cell_size_m"]),
        math.floor(y / index["cell_size_m"]),
    )
    return [*index["cells"].get(key, []), *index["global_features"]]


def _feature_projected_bounds(feature: dict, reference_lat: float) -> tuple[float, float, float, float] | None:
    coordinates = _geometry_coordinate_pairs(feature.get("geometry"))
    if not coordinates:
        return None
    projected = [project_lon_lat(lon, lat, reference_lat) for lon, lat in coordinates]
    return (
        min(x for x, _ in projected),
        min(y for _, y in projected),
        max(x for x, _ in projected),
        max(y for _, y in projected),
    )


def _geometry_coordinate_pairs(geometry: object) -> list[tuple[float, float]]:
    if not isinstance(geometry, dict):
        return []
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if geometry_type == "Point" and _valid_lon_lat_pair(coordinates):
        return [(float(coordinates[0]), float(coordinates[1]))]
    if geometry_type == "LineString" and isinstance(coordinates, list):
        return [
            (float(point[0]), float(point[1]))
            for point in coordinates
            if _valid_lon_lat_pair(point)
        ]
    if geometry_type == "Polygon" and isinstance(coordinates, list):
        return [
            (float(point[0]), float(point[1]))
            for ring in coordinates
            if isinstance(ring, list)
            for point in ring
            if _valid_lon_lat_pair(point)
        ]
    if geometry_type == "MultiLineString" and isinstance(coordinates, list):
        return [
            (float(point[0]), float(point[1]))
            for line in coordinates
            if isinstance(line, list)
            for point in line
            if _valid_lon_lat_pair(point)
        ]
    if geometry_type == "MultiPolygon" and isinstance(coordinates, list):
        return [
            (float(point[0]), float(point[1]))
            for polygon in coordinates
            if isinstance(polygon, list)
            for ring in polygon
            if isinstance(ring, list)
            for point in ring
            if _valid_lon_lat_pair(point)
        ]
    return []


def build_osm_fetch_scopes(
    fixes: list[dict],
    *,
    search_radius_m: float,
    max_subscopes: int = DEFAULT_CONTEXT_MAX_SUBSCOPES,
) -> list[dict]:
    """Build deterministic buffered bbox scopes without performing OSM requests."""
    radius_m = normalize_search_radius_m(search_radius_m)
    if isinstance(max_subscopes, bool) or int(max_subscopes) <= 0:
        raise ValueError("max_subscopes must be a positive integer")
    coordinates = []
    for fix in fixes:
        lon = _finite_number(fix.get("lon"))
        lat = _finite_number(fix.get("lat"))
        if lon is None or lat is None:
            continue
        coordinates.append((fix, lon, lat))
    if not coordinates:
        raise ValueError("No valid fix coordinates are available for OSM context enrichment.")

    reference_lat = sum(lat for _, _, lat in coordinates) / len(coordinates)
    tile_size_m = max(50.0, min(DEFAULT_CONTEXT_TILE_SIZE_M, 4_900.0 - (2.0 * radius_m)))
    tiles: dict[tuple[int, int], list[dict]] = {}
    for fix, lon, lat in coordinates:
        x, y = project_lon_lat(lon, lat, reference_lat)
        key = (math.floor(x / tile_size_m), math.floor(y / tile_size_m))
        tiles.setdefault(key, []).append(fix)
    if len(tiles) > int(max_subscopes):
        raise ValueError(
            f"OSM context enrichment would require {len(tiles)} spatial subscopes; "
            f"max_subscopes is {int(max_subscopes)}."
        )
    return [
        _buffered_bbox_scope(tile_fixes, radius_m)
        for _, tile_fixes in sorted(tiles.items(), key=lambda item: item[0])
    ]


def merge_osm_features(feature_collections: list[dict]) -> list[dict]:
    features = []
    seen = set()
    for collection in feature_collections:
        raw_features = collection.get("features") if isinstance(collection.get("features"), list) else []
        for feature in raw_features:
            if not isinstance(feature, dict):
                continue
            key = str(feature.get("id") or "")
            if not key:
                serialized = json.dumps(feature, sort_keys=True, separators=(",", ":"))
                key = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
            if key in seen:
                continue
            seen.add(key)
            features.append(feature)
    return features


def subdivide_osm_fetch_scope(scope: dict) -> list[dict]:
    """Partition one bbox scope into deterministic quadrants for retrying dense OSM fetches."""
    if not isinstance(scope, dict) or str(scope.get("type") or "").strip().lower() != "bbox":
        raise ValueError("OSM context scope subdivision requires a bbox scope")
    west = _finite_number(scope.get("west"))
    south = _finite_number(scope.get("south"))
    east = _finite_number(scope.get("east"))
    north = _finite_number(scope.get("north"))
    if west is None or south is None or east is None or north is None or east <= west or north <= south:
        raise ValueError("OSM context scope subdivision requires a valid non-empty bbox")
    middle_lon = (west + east) / 2.0
    middle_lat = (south + north) / 2.0
    if middle_lon in {west, east} or middle_lat in {south, north}:
        raise ValueError("OSM context scope is too small to subdivide further")
    return [
        {"type": "bbox", "west": west, "south": south, "east": middle_lon, "north": middle_lat},
        {"type": "bbox", "west": middle_lon, "south": south, "east": east, "north": middle_lat},
        {"type": "bbox", "west": west, "south": middle_lat, "east": middle_lon, "north": north},
        {"type": "bbox", "west": middle_lon, "south": middle_lat, "east": east, "north": north},
    ]


def nearest_osm_feature(lon: float, lat: float, features: list[dict]) -> dict | None:
    nearest = None
    for feature in features:
        if not isinstance(feature, dict):
            continue
        distance_m = distance_to_feature_m(lon, lat, feature.get("geometry"))
        if distance_m is None:
            continue
        if nearest is None or distance_m < nearest["distance_m"]:
            properties = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
            nearest = {
                "feature": feature,
                "properties": properties,
                "distance_m": distance_m,
            }
    return nearest


def distance_to_feature_m(lon: float, lat: float, geometry: object) -> float | None:
    if not isinstance(geometry, dict):
        return None
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if geometry_type == "Point" and _valid_lon_lat_pair(coordinates):
        return _haversine_m(lon, lat, coordinates[0], coordinates[1])
    if geometry_type == "LineString" and isinstance(coordinates, list):
        return _distance_to_line_m(lon, lat, coordinates)
    if geometry_type == "Polygon" and isinstance(coordinates, list):
        rings = [ring for ring in coordinates if isinstance(ring, list)]
        if not rings:
            return None
        if _point_in_ring(lon, lat, rings[0]):
            return 0.0
        distances = [
            _distance_to_line_m(lon, lat, ring)
            for ring in rings
            if len(ring) >= 2
        ]
        distances = [distance for distance in distances if distance is not None]
        return min(distances) if distances else None
    if geometry_type == "MultiLineString" and isinstance(coordinates, list):
        distances = [_distance_to_line_m(lon, lat, line) for line in coordinates if isinstance(line, list)]
        distances = [distance for distance in distances if distance is not None]
        return min(distances) if distances else None
    if geometry_type == "MultiPolygon" and isinstance(coordinates, list):
        distances = []
        for polygon in coordinates:
            if not isinstance(polygon, list):
                continue
            distance = distance_to_feature_m(lon, lat, {"type": "Polygon", "coordinates": polygon})
            if distance is not None:
                distances.append(distance)
        return min(distances) if distances else None
    return None


def _distance_to_line_m(lon: float, lat: float, coordinates: list) -> float | None:
    points = [point for point in coordinates if _valid_lon_lat_pair(point)]
    if not points:
        return None
    if len(points) == 1:
        return _haversine_m(lon, lat, points[0][0], points[0][1])
    px, py = project_lon_lat(lon, lat, lat)
    min_distance = None
    for start, end in zip(points, points[1:]):
        ax, ay = project_lon_lat(start[0], start[1], lat)
        bx, by = project_lon_lat(end[0], end[1], lat)
        distance = _point_to_segment_distance(px, py, ax, ay, bx, by)
        if min_distance is None or distance < min_distance:
            min_distance = distance
    return min_distance


def _buffered_bbox_scope(fixes: list[dict], distance_m: float) -> dict:
    coordinates = []
    for fix in fixes:
        lon = _finite_number(fix.get("lon"))
        lat = _finite_number(fix.get("lat"))
        if lon is not None and lat is not None:
            coordinates.append((lon, lat))
    if not coordinates:
        raise ValueError("No valid fix coordinates are available for OSM context enrichment.")
    west = min(lon for lon, _ in coordinates)
    east = max(lon for lon, _ in coordinates)
    south = min(lat for _, lat in coordinates)
    north = max(lat for _, lat in coordinates)
    mid_lat = max(min((south + north) / 2.0, 89.0), -89.0)
    lat_buffer = math.degrees(distance_m / EARTH_RADIUS_M)
    cos_lat = max(math.cos(math.radians(mid_lat)), 0.01)
    lon_buffer = math.degrees(distance_m / (EARTH_RADIUS_M * cos_lat))
    return {
        "type": "bbox",
        "west": max(-180.0, west - lon_buffer),
        "south": max(-90.0, south - lat_buffer),
        "east": min(180.0, east + lon_buffer),
        "north": min(90.0, north + lat_buffer),
    }


def _valid_lon_lat_pair(value: object) -> bool:
    if not isinstance(value, (list, tuple)) or len(value) < 2:
        return False
    lon = _finite_number(value[0])
    lat = _finite_number(value[1])
    return lon is not None and lat is not None and -180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0


def project_lon_lat(lon: float, lat: float, reference_lat: float) -> tuple[float, float]:
    return (
        math.radians(lon) * EARTH_RADIUS_M * math.cos(math.radians(reference_lat)),
        math.radians(lat) * EARTH_RADIUS_M,
    )


def _point_to_segment_distance(px: float, py: float, ax: float, ay: float, bx: float, by: float) -> float:
    dx = bx - ax
    dy = by - ay
    if dx == 0.0 and dy == 0.0:
        return math.hypot(px - ax, py - ay)
    t = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    closest_x = ax + t * dx
    closest_y = ay + t * dy
    return math.hypot(px - closest_x, py - closest_y)


def _point_in_ring(lon: float, lat: float, ring: list) -> bool:
    points = [point for point in ring if _valid_lon_lat_pair(point)]
    inside = False
    if len(points) < 3:
        return False
    previous_lon, previous_lat = points[-1][0], points[-1][1]
    for current_lon, current_lat in points:
        intersects = ((current_lat > lat) != (previous_lat > lat)) and (
            lon < (previous_lon - current_lon) * (lat - current_lat) / (previous_lat - current_lat) + current_lon
        )
        if intersects:
            inside = not inside
        previous_lon, previous_lat = current_lon, current_lat
    return inside


def _haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_M * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
