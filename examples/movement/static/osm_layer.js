const DEFAULT_POINT_RADIUS_M = 500;
const DEFAULT_SEGMENT_PADDING_M = 500;

export function scopeFromPoint(fixOrLonLat, radiusM = DEFAULT_POINT_RADIUS_M) {
  const position = extractLonLat(fixOrLonLat);
  if (!position) {
    throw new Error("Cannot build OSM point scope without a valid lon/lat position");
  }
  return {
    type: "point",
    lon: position[0],
    lat: position[1],
    radius_m: radiusM,
  };
}

export function scopeFromMapBounds(map) {
  const bounds = map?.getBounds?.();
  if (!bounds) {
    throw new Error("Cannot build OSM bbox scope without map bounds");
  }
  return {
    type: "bbox",
    west: bounds.getWest(),
    south: bounds.getSouth(),
    east: bounds.getEast(),
    north: bounds.getNorth(),
  };
}

export function scopeFromSegmentBounds(fixes, paddingM = DEFAULT_SEGMENT_PADDING_M) {
  const positions = (Array.isArray(fixes) ? fixes : [])
    .map(extractLonLat)
    .filter(Boolean);
  if (!positions.length) {
    throw new Error("Cannot build OSM segment bbox scope without valid positions");
  }
  let west = Infinity;
  let east = -Infinity;
  let south = Infinity;
  let north = -Infinity;
  for (const [lon, lat] of positions) {
    west = Math.min(west, lon);
    east = Math.max(east, lon);
    south = Math.min(south, lat);
    north = Math.max(north, lat);
  }
  const midLat = (south + north) / 2;
  const latPadding = metersToLatDegrees(paddingM);
  const lonPadding = metersToLonDegrees(paddingM, midLat);
  return {
    type: "bbox",
    west: west - lonPadding,
    south: south - latPadding,
    east: east + lonPadding,
    north: north + latPadding,
  };
}

export async function fetchOsmContext(query, options = {}) {
  const response = await fetch("/api/osm/features", {
    method: "POST",
    signal: options.signal,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    body: JSON.stringify(query),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `${response.status} ${response.statusText}`);
  }
  return payload;
}

export function buildOsmDeckLayers(featureCollection, options = {}) {
  const deckInstance = options.deckInstance || globalThis.deck;
  if (!deckInstance?.GeoJsonLayer || !featureCollection || featureCollection.type !== "FeatureCollection") {
    return [];
  }
  const idPrefix = options.idPrefix || "osm-context";
  return [
    new deckInstance.GeoJsonLayer({
      id: `${idPrefix}-features`,
      data: featureCollection,
      pickable: false,
      stroked: true,
      filled: true,
      pointType: "circle",
      getLineColor: options.lineColor || [33, 150, 243, 215],
      getFillColor: options.fillColor || [33, 150, 243, 54],
      getPointRadius: options.pointRadiusM || 70,
      pointRadiusMinPixels: options.pointRadiusMinPixels || 4,
      pointRadiusMaxPixels: options.pointRadiusMaxPixels || 11,
      lineWidthMinPixels: options.lineWidthMinPixels || 2,
      getLineWidth: options.lineWidthM || 2,
    }),
  ];
}

function extractLonLat(value) {
  if (Array.isArray(value) && value.length >= 2) {
    return validLonLat(value[0], value[1]);
  }
  if (Array.isArray(value?.position) && value.position.length >= 2) {
    return validLonLat(value.position[0], value.position[1]);
  }
  const lon = value?.lon ?? value?.lng ?? value?.longitude;
  const lat = value?.lat ?? value?.latitude;
  return validLonLat(lon, lat);
}

function validLonLat(rawLon, rawLat) {
  const lon = Number(rawLon);
  const lat = Number(rawLat);
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) {
    return null;
  }
  if (lon < -180 || lon > 180 || lat < -90 || lat > 90) {
    return null;
  }
  return [lon, lat];
}

function metersToLatDegrees(meters) {
  const value = Number(meters);
  return Number.isFinite(value) && value > 0 ? value / 111_320 : 0;
}

function metersToLonDegrees(meters, latitude) {
  const value = Number(meters);
  const lat = Number(latitude);
  if (!Number.isFinite(value) || value <= 0) {
    return 0;
  }
  const divisor = 111_320 * Math.max(0.1, Math.cos((Number.isFinite(lat) ? lat : 0) * Math.PI / 180));
  return value / divisor;
}
