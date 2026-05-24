import itertools
import hashlib
import html
import json
import math
import os
import re
from datetime import datetime, timezone
from typing import Any

import httpx


OVERPASS_URL = os.environ.get("VIBECLEANING_OVERPASS_URL", "https://overpass-api.de/api/interpreter")
OVERPASS_USER_AGENT = os.environ.get("VIBECLEANING_OVERPASS_USER_AGENT", "vibecleaning/0.1 local OSM context proxy")
OSM_SOURCE = {
    "name": "OpenStreetMap via Overpass API",
    "url": OVERPASS_URL,
}

MAX_SELECTORS = 8
MAX_TAGS_PER_SELECTOR = 6
MAX_IN_VALUES = 20
MAX_EXPANDED_CLAUSES = 64
DEFAULT_MAX_FEATURES = 500
MAX_FEATURES = 1000
DEFAULT_TIMEOUT_S = 15
MAX_TIMEOUT_S = 25
MAX_POINT_RADIUS_M = 25_000.0
MAX_BBOX_WIDTH_M = 100_000.0
MAX_BBOX_HEIGHT_M = 100_000.0
MAX_BBOX_AREA_M2 = 2_500_000_000.0
MAX_EXISTS_POINT_RADIUS_M = 5_000.0
MAX_EXISTS_BBOX_WIDTH_M = 10_000.0
MAX_EXISTS_BBOX_HEIGHT_M = 10_000.0
MAX_EXISTS_BBOX_AREA_M2 = 25_000_000.0
MAX_EXISTS_FEATURES = 500
EARTH_RADIUS_M = 6_371_008.8

VALID_ELEMENT_TYPES = {"node", "way", "relation"}
VALID_TAG_OPS = {"exists", "equals", "in"}


class OSMValidationError(ValueError):
    pass


class OSMFetchError(RuntimeError):
    pass


def normalize_osm_request(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise OSMValidationError("Invalid OSM request body")

    scope = _normalize_scope(payload.get("scope"))
    selectors = _normalize_selectors(payload.get("selectors"))
    element_types, element_types_explicit = _normalize_element_types(
        payload.get("element_types"),
        payload.get("element_types_explicit"),
    )
    max_features = _normalize_positive_int(
        payload.get("max_features"),
        default=DEFAULT_MAX_FEATURES,
        maximum=MAX_FEATURES,
        label="max_features",
    )
    timeout_s = _normalize_positive_int(
        payload.get("timeout_s"),
        default=DEFAULT_TIMEOUT_S,
        maximum=MAX_TIMEOUT_S,
        label="timeout_s",
    )

    normalized = {
        "scope": scope,
        "selectors": selectors,
        "element_types": element_types,
        "element_types_explicit": element_types_explicit,
        "max_features": max_features,
        "timeout_s": timeout_s,
    }
    _validate_expanded_clause_count(normalized)
    _validate_broad_exists_constraints(normalized)
    return normalized


def build_overpass_query(query: dict) -> str:
    normalized = normalize_osm_request(query)
    clauses = []
    for selector in normalized["selectors"]:
        for tag_filters in _expanded_tag_filter_groups(selector["tags"]):
            filters = "".join(_overpass_filter_for_tag(tag) for tag in tag_filters)
            for element_type in normalized["element_types"]:
                clauses.append(f"  {element_type}{filters}{_scope_filter(normalized['scope'])};")

    body = "\n".join(clauses)
    output_limit = normalized["max_features"] + 1
    return (
        f"[out:json][timeout:{normalized['timeout_s']}];\n"
        "(\n"
        f"{body}\n"
        ");\n"
        f"out geom qt {output_limit};"
    )


def fetch_osm_features(query: dict) -> dict:
    normalized = normalize_osm_request(query)
    overpass_query = build_overpass_query(normalized)
    try:
        response = httpx.post(
            OSM_SOURCE["url"],
            data={"data": overpass_query},
            headers={
                "Accept": "application/json",
                "User-Agent": OVERPASS_USER_AGENT,
            },
            timeout=normalized["timeout_s"] + 5,
        )
    except httpx.TimeoutException as exc:
        raise OSMFetchError("Overpass request timed out") from exc
    except httpx.HTTPError as exc:
        raise OSMFetchError(f"Overpass request failed: {exc}") from exc

    if response.status_code >= 400:
        detail = _plain_error_detail(response.text)
        message = f"Overpass request failed with HTTP {response.status_code}"
        if detail:
            message = f"{message}: {detail}"
        raise OSMFetchError(message)

    try:
        payload = response.json()
    except ValueError as exc:
        raise OSMFetchError("Overpass returned invalid JSON") from exc

    elements = payload.get("elements")
    if not isinstance(elements, list):
        raise OSMFetchError("Overpass response did not include an elements list")

    feature_collection = overpass_elements_to_geojson(elements, normalized["max_features"])
    conversion_metadata = dict(feature_collection.pop("metadata", {}))
    feature_collection["metadata"] = {
        "normalized_query": normalized,
        "scope": normalized["scope"],
        "scope_signature": _payload_signature(normalized["scope"]),
        "query_signature": _payload_signature({
            "scope": normalized["scope"],
            "selectors": normalized["selectors"],
            "element_types": normalized["element_types"],
        }),
        "source": dict(OSM_SOURCE),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "feature_count": len(feature_collection["features"]),
        **conversion_metadata,
    }
    return feature_collection


def overpass_elements_to_geojson(elements: list[dict], max_features: int) -> dict:
    feature_limit = max(0, min(int(max_features), MAX_FEATURES))
    features = []
    omitted_feature_count = 0
    truncated_feature_count = 0
    unsupported_relation_count = 0
    unsupported_element_count = 0
    unsupported_geometry_count = 0
    for element in elements:
        if not isinstance(element, dict):
            omitted_feature_count += 1
            unsupported_element_count += 1
            continue
        geometry = _element_geometry(element)
        if geometry is None:
            omitted_feature_count += 1
            if element.get("type") == "relation":
                unsupported_relation_count += 1
            elif element.get("type") not in VALID_ELEMENT_TYPES:
                unsupported_element_count += 1
            else:
                unsupported_geometry_count += 1
            continue
        if len(features) >= feature_limit:
            omitted_feature_count += 1
            truncated_feature_count += 1
            continue
        osm_type = str(element.get("type") or "")
        osm_id = element.get("id")
        tags = element.get("tags") if isinstance(element.get("tags"), dict) else {}
        properties = {
            "osm_type": osm_type,
            "osm_id": osm_id,
            "tags": tags,
        }
        if isinstance(tags.get("name"), str):
            properties["name"] = tags["name"]
        features.append(
            {
                "type": "Feature",
                "id": f"{osm_type}/{osm_id}",
                "geometry": geometry,
                "properties": properties,
            }
        )
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "input_element_count": len(elements),
            "omitted_feature_count": omitted_feature_count,
            "truncated_feature_count": truncated_feature_count,
            "unsupported_relation_count": unsupported_relation_count,
            "unsupported_element_count": unsupported_element_count,
            "unsupported_geometry_count": unsupported_geometry_count,
        },
    }


def _normalize_scope(raw_scope: object) -> dict:
    if not isinstance(raw_scope, dict):
        raise OSMValidationError("Missing OSM scope")
    scope_type = str(raw_scope.get("type") or "").strip().lower()
    if scope_type == "segment":
        raise OSMValidationError("Segment OSM scope is not supported yet; use a bbox scope around the segment.")
    if scope_type == "point":
        lon = _normalize_lon(raw_scope.get("lon"))
        lat = _normalize_lat(raw_scope.get("lat"))
        radius_m = _normalize_float(raw_scope.get("radius_m"), label="radius_m")
        if radius_m <= 0:
            raise OSMValidationError("radius_m must be greater than 0")
        if radius_m > MAX_POINT_RADIUS_M:
            raise OSMValidationError(f"radius_m must be <= {int(MAX_POINT_RADIUS_M)}")
        return {
            "type": "point",
            "lon": lon,
            "lat": lat,
            "radius_m": radius_m,
        }
    if scope_type == "bbox":
        west = _normalize_lon(raw_scope.get("west"), label="west")
        south = _normalize_lat(raw_scope.get("south"), label="south")
        east = _normalize_lon(raw_scope.get("east"), label="east")
        north = _normalize_lat(raw_scope.get("north"), label="north")
        if east < west:
            raise OSMValidationError("Dateline-crossing bbox scopes are not supported yet")
        if east == west or north == south:
            raise OSMValidationError("bbox scope must have non-zero width and height")
        if north < south:
            raise OSMValidationError("bbox north must be greater than south")
        _validate_bbox_size(west=west, south=south, east=east, north=north)
        return {
            "type": "bbox",
            "west": west,
            "south": south,
            "east": east,
            "north": north,
        }
    raise OSMValidationError("scope.type must be point or bbox")


def _normalize_selectors(raw_selectors: object) -> list[dict]:
    if not isinstance(raw_selectors, list) or not raw_selectors:
        raise OSMValidationError("selectors must be a non-empty list")
    if len(raw_selectors) > MAX_SELECTORS:
        raise OSMValidationError(f"selectors must contain at most {MAX_SELECTORS} entries")

    selectors = []
    for raw_selector in raw_selectors:
        if not isinstance(raw_selector, dict):
            raise OSMValidationError("Each selector must be an object")
        raw_tags = raw_selector.get("tags")
        if not isinstance(raw_tags, list) or not raw_tags:
            raise OSMValidationError("Each selector must include at least one tag")
        if len(raw_tags) > MAX_TAGS_PER_SELECTOR:
            raise OSMValidationError(f"Each selector may include at most {MAX_TAGS_PER_SELECTOR} tags")
        selectors.append({"tags": [_normalize_tag_filter(tag) for tag in raw_tags]})
    return selectors


def _normalize_tag_filter(raw_tag: object) -> dict:
    if not isinstance(raw_tag, dict):
        raise OSMValidationError("Each tag filter must be an object")
    key = _normalize_tag_key(raw_tag.get("key"))
    op = str(raw_tag.get("op") or "equals").strip().lower()
    if op not in VALID_TAG_OPS:
        raise OSMValidationError(f"Unsupported OSM tag operator: {op or 'missing'}")
    if op == "exists":
        return {"key": key, "op": op}
    if op == "equals":
        value = _normalize_tag_value(raw_tag.get("value"), label=f"value for {key}")
        return {"key": key, "op": op, "value": value}

    raw_values = raw_tag.get("values", raw_tag.get("value"))
    if not isinstance(raw_values, list) or not raw_values:
        raise OSMValidationError(f"in operator for {key} requires a non-empty values list")
    if len(raw_values) > MAX_IN_VALUES:
        raise OSMValidationError(f"in operator for {key} supports at most {MAX_IN_VALUES} values")
    values = []
    seen = set()
    for raw_value in raw_values:
        value = _normalize_tag_value(raw_value, label=f"value for {key}")
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    if not values:
        raise OSMValidationError(f"in operator for {key} requires a non-empty values list")
    return {"key": key, "op": op, "values": values}


def _normalize_element_types(raw_element_types: object, raw_explicit: object = None) -> tuple[list[str], bool]:
    if raw_element_types in (None, ""):
        return ["node", "way", "relation"], False
    if not isinstance(raw_element_types, list) or not raw_element_types:
        raise OSMValidationError("element_types must be a non-empty list")
    element_types_explicit = raw_explicit if isinstance(raw_explicit, bool) else True
    element_types = []
    seen = set()
    for raw_type in raw_element_types:
        element_type = str(raw_type or "").strip().lower()
        if element_type not in VALID_ELEMENT_TYPES:
            raise OSMValidationError("element_types may only include node, way, or relation")
        if element_type in seen:
            continue
        seen.add(element_type)
        element_types.append(element_type)
    return element_types, element_types_explicit


def _normalize_positive_int(raw_value: object, *, default: int, maximum: int, label: str) -> int:
    if raw_value in (None, ""):
        return default
    if isinstance(raw_value, bool):
        raise OSMValidationError(f"{label} must be an integer")
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise OSMValidationError(f"{label} must be an integer") from exc
    if value <= 0:
        raise OSMValidationError(f"{label} must be greater than 0")
    if value > maximum:
        raise OSMValidationError(f"{label} must be <= {maximum}")
    return value


def _normalize_tag_key(raw_key: object) -> str:
    if not isinstance(raw_key, str):
        raise OSMValidationError("Missing tag key")
    key = raw_key.strip()
    if not key:
        raise OSMValidationError("Missing tag key")
    if len(key) > 80:
        raise OSMValidationError("Tag key is too long")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:-.")
    if any(character not in allowed for character in key):
        raise OSMValidationError(f"Invalid tag key: {key}")
    return key


def _normalize_tag_value(raw_value: object, *, label: str) -> str:
    if not isinstance(raw_value, str):
        raise OSMValidationError(f"Missing {label}")
    value = raw_value.strip()
    if not value:
        raise OSMValidationError(f"Missing {label}")
    if len(value) > 120:
        raise OSMValidationError(f"{label} is too long")
    if any(ord(character) < 32 for character in value):
        raise OSMValidationError(f"Invalid {label}")
    return value


def _normalize_lon(raw_value: object, *, label: str = "lon") -> float:
    value = _normalize_float(raw_value, label=label)
    if value < -180.0 or value > 180.0:
        raise OSMValidationError(f"{label} must be between -180 and 180")
    return value


def _normalize_lat(raw_value: object, *, label: str = "lat") -> float:
    value = _normalize_float(raw_value, label=label)
    if value < -90.0 or value > 90.0:
        raise OSMValidationError(f"{label} must be between -90 and 90")
    return value


def _normalize_float(raw_value: object, *, label: str) -> float:
    if isinstance(raw_value, bool):
        raise OSMValidationError(f"{label} must be a number")
    try:
        value = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise OSMValidationError(f"{label} must be a number") from exc
    if not math.isfinite(value):
        raise OSMValidationError(f"{label} must be finite")
    return value


def _validate_bbox_size(*, west: float, south: float, east: float, north: float):
    width_m, height_m = _bbox_dimensions_m({
        "west": west,
        "south": south,
        "east": east,
        "north": north,
    })
    if width_m > MAX_BBOX_WIDTH_M:
        raise OSMValidationError(f"bbox width must be <= {int(MAX_BBOX_WIDTH_M / 1000)} km")
    if height_m > MAX_BBOX_HEIGHT_M:
        raise OSMValidationError(f"bbox height must be <= {int(MAX_BBOX_HEIGHT_M / 1000)} km")
    if width_m * height_m > MAX_BBOX_AREA_M2:
        raise OSMValidationError("bbox area must be <= 2500 km^2")


def _bbox_dimensions_m(scope: dict) -> tuple[float, float]:
    west = scope["west"]
    south = scope["south"]
    east = scope["east"]
    north = scope["north"]
    mid_lon = (west + east) / 2.0
    mid_lat = (south + north) / 2.0
    width_m = _haversine_m(west, mid_lat, east, mid_lat)
    height_m = _haversine_m(mid_lon, south, mid_lon, north)
    return width_m, height_m


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


def _validate_expanded_clause_count(query: dict):
    count = 0
    for selector in query["selectors"]:
        count += len(_expanded_tag_filter_groups(selector["tags"])) * len(query["element_types"])
    if count > MAX_EXPANDED_CLAUSES:
        raise OSMValidationError(f"Expanded OSM query may include at most {MAX_EXPANDED_CLAUSES} clauses")


def _validate_broad_exists_constraints(query: dict):
    broad_exists_selectors = [
        selector for selector in query["selectors"]
        if selector["tags"] and all(tag["op"] == "exists" for tag in selector["tags"])
    ]
    if not broad_exists_selectors:
        return

    if not query.get("element_types_explicit"):
        raise OSMValidationError(
            "Broad exists OSM queries require explicit element_types; "
            "use equals/in tags or choose node, way, and/or relation explicitly."
        )
    if query["max_features"] > MAX_EXISTS_FEATURES:
        raise OSMValidationError(
            f"Broad exists OSM queries support max_features <= {MAX_EXISTS_FEATURES}; "
            "use equals/in tags or a smaller spatial scope to narrow the request."
        )

    scope = query["scope"]
    if scope["type"] == "point" and scope["radius_m"] > MAX_EXISTS_POINT_RADIUS_M:
        raise OSMValidationError(
            f"Broad exists OSM point queries require radius_m <= {int(MAX_EXISTS_POINT_RADIUS_M)}; "
            "use equals/in tags or a smaller radius to narrow the request."
        )
    if scope["type"] == "bbox":
        width_m, height_m = _bbox_dimensions_m(scope)
        if (
            width_m > MAX_EXISTS_BBOX_WIDTH_M
            or height_m > MAX_EXISTS_BBOX_HEIGHT_M
            or width_m * height_m > MAX_EXISTS_BBOX_AREA_M2
        ):
            raise OSMValidationError(
                "Broad exists OSM bbox queries are limited to <= 10 km width, "
                "<= 10 km height, and <= 25 km^2 area; use equals/in tags, "
                "a smaller bbox, or a point scope to narrow the request."
            )


def _expanded_tag_filter_groups(tags: list[dict]) -> list[list[dict]]:
    choices = []
    for tag in tags:
        if tag["op"] == "in":
            choices.append([
                {"key": tag["key"], "op": "equals", "value": value}
                for value in tag["values"]
            ])
        else:
            choices.append([tag])
    return [list(group) for group in itertools.product(*choices)]


def _overpass_filter_for_tag(tag: dict) -> str:
    key = _escape_overpass_string(tag["key"])
    if tag["op"] == "exists":
        return f'["{key}"]'
    return f'["{key}"="{_escape_overpass_string(tag["value"])}"]'


def _scope_filter(scope: dict) -> str:
    if scope["type"] == "point":
        return (
            f"(around:{_format_number(scope['radius_m'])},"
            f"{_format_number(scope['lat'])},"
            f"{_format_number(scope['lon'])})"
        )
    return (
        f"({_format_number(scope['south'])},"
        f"{_format_number(scope['west'])},"
        f"{_format_number(scope['north'])},"
        f"{_format_number(scope['east'])})"
    )


def _escape_overpass_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _format_number(value: float) -> str:
    return f"{value:.7f}".rstrip("0").rstrip(".")


def _payload_signature(payload: dict) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def _plain_error_detail(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return " ".join(text.split())[:300]


def _element_geometry(element: dict) -> dict | None:
    element_type = element.get("type")
    if element_type == "node":
        lon = element.get("lon")
        lat = element.get("lat")
        if _valid_coordinate(lon, lat):
            return {"type": "Point", "coordinates": [float(lon), float(lat)]}
        return None

    geometry = element.get("geometry")
    if isinstance(geometry, list):
        coordinates = [
            [float(point["lon"]), float(point["lat"])]
            for point in geometry
            if isinstance(point, dict) and _valid_coordinate(point.get("lon"), point.get("lat"))
        ]
        if len(coordinates) >= 2:
            if len(coordinates) >= 4 and coordinates[0] == coordinates[-1]:
                return {"type": "Polygon", "coordinates": [coordinates]}
            return {"type": "LineString", "coordinates": coordinates}

    center = element.get("center")
    if isinstance(center, dict) and _valid_coordinate(center.get("lon"), center.get("lat")):
        return {"type": "Point", "coordinates": [float(center["lon"]), float(center["lat"])]}

    bounds = element.get("bounds")
    if isinstance(bounds, dict):
        west = bounds.get("minlon")
        south = bounds.get("minlat")
        east = bounds.get("maxlon")
        north = bounds.get("maxlat")
        if all(isinstance(value, (int, float)) and math.isfinite(float(value)) for value in (west, south, east, north)):
            return {
                "type": "Polygon",
                "coordinates": [[
                    [float(west), float(south)],
                    [float(east), float(south)],
                    [float(east), float(north)],
                    [float(west), float(north)],
                    [float(west), float(south)],
                ]],
            }

    return None


def _valid_coordinate(lon: Any, lat: Any) -> bool:
    if isinstance(lon, bool) or isinstance(lat, bool):
        return False
    try:
        lon_value = float(lon)
        lat_value = float(lat)
    except (TypeError, ValueError):
        return False
    return (
        math.isfinite(lon_value)
        and math.isfinite(lat_value)
        and -180.0 <= lon_value <= 180.0
        and -90.0 <= lat_value <= 90.0
    )
