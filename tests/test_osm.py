from pathlib import Path
import sys

from fastapi.testclient import TestClient
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app import osm
from app.web import create_app


def point_query(**overrides):
    payload = {
        "scope": {"type": "point", "lon": -70.1, "lat": 40.1, "radius_m": 500},
        "selectors": [
            {"tags": [{"key": "amenity", "op": "equals", "value": "veterinary"}]},
        ],
        "element_types": ["node", "way"],
    }
    payload.update(overrides)
    return payload


def bbox_query(**overrides):
    payload = {
        "scope": {"type": "bbox", "west": -70.05, "south": 40.0, "east": -70.0, "north": 40.05},
        "selectors": [
            {"tags": [{"key": "highway", "op": "exists"}]},
        ],
        "element_types": ["way"],
    }
    payload.update(overrides)
    return payload


def test_normalize_osm_request_supports_point_scope_and_equals():
    normalized = osm.normalize_osm_request(point_query())

    assert normalized["scope"] == {"type": "point", "lon": -70.1, "lat": 40.1, "radius_m": 500.0}
    assert normalized["selectors"][0]["tags"][0] == {
        "key": "amenity",
        "op": "equals",
        "value": "veterinary",
    }
    assert normalized["element_types"] == ["node", "way"]
    assert normalized["max_features"] == 500
    assert normalized["timeout_s"] == 15


def test_build_overpass_query_supports_bbox_exists():
    query = osm.build_overpass_query(bbox_query(max_features=25, timeout_s=10))

    assert "[out:json][timeout:10];" in query
    assert 'way["highway"](40,-70.05,40.05,-70);' in query
    assert "out geom qt 26;" in query


def test_build_overpass_query_expands_in_without_regex():
    query = osm.build_overpass_query(
        point_query(
            selectors=[
                {
                    "tags": [
                        {
                            "key": "amenity",
                            "op": "in",
                            "values": ["veterinary", "clinic"],
                        }
                    ]
                }
            ],
            element_types=["node"],
        )
    )

    assert 'node["amenity"="veterinary"](around:500,40.1,-70.1);' in query
    assert 'node["amenity"="clinic"](around:500,40.1,-70.1);' in query
    assert "regex" not in query


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            point_query(selectors=[{"tags": [{"key": "amenity", "op": "regex", "value": "vet.*"}]}]),
            "Unsupported OSM tag operator: regex",
        ),
        (
            point_query(scope={"type": "segment", "coordinates": [[-70.0, 40.0], [-70.1, 40.1]], "buffer_m": 500}),
            "Segment OSM scope is not supported yet",
        ),
        (
            point_query(scope={"type": "point", "lon": -70.1, "lat": 40.1, "radius_m": 25001}),
            "radius_m must be <=",
        ),
        (
            bbox_query(scope={"type": "bbox", "west": -70.0, "south": 40.0, "east": -71.0, "north": 40.1}),
            "Dateline-crossing bbox scopes are not supported yet",
        ),
        (
            bbox_query(scope={"type": "bbox", "west": -70.0, "south": 40.0, "east": -68.0, "north": 40.1}),
            "bbox width must be <=",
        ),
        (
            {
                "scope": {"type": "bbox", "west": -70.05, "south": 40.0, "east": -70.0, "north": 40.05},
                "selectors": [{"tags": [{"key": "building", "op": "exists"}]}],
            },
            "Broad exists OSM queries require explicit element_types",
        ),
        (
            bbox_query(scope={"type": "bbox", "west": -70.2, "south": 40.0, "east": -70.0, "north": 40.2}),
            "Broad exists OSM bbox queries are limited",
        ),
        (
            point_query(
                scope={"type": "point", "lon": -70.1, "lat": 40.1, "radius_m": 6000},
                selectors=[{"tags": [{"key": "building", "op": "exists"}]}],
                element_types=["way"],
            ),
            "Broad exists OSM point queries require radius_m <=",
        ),
        (
            bbox_query(max_features=501),
            "Broad exists OSM queries support max_features <=",
        ),
        (
            point_query(max_features=1001),
            "max_features must be <=",
        ),
        (
            point_query(selectors=[{"tags": [{"key": f"amenity{i}", "op": "exists"}]} for i in range(9)]),
            "selectors must contain at most 8 entries",
        ),
    ],
)
def test_osm_request_validation_errors(payload, message):
    with pytest.raises(osm.OSMValidationError, match=message):
        osm.normalize_osm_request(payload)


def test_overpass_elements_to_geojson_limits_features_and_handles_common_geometries():
    payload = osm.overpass_elements_to_geojson(
        [
            {"type": "node", "id": 1, "lon": -70.0, "lat": 40.0, "tags": {"amenity": "veterinary"}},
            {
                "type": "way",
                "id": 2,
                "geometry": [
                    {"lon": -70.0, "lat": 40.0},
                    {"lon": -70.1, "lat": 40.1},
                ],
                "tags": {"highway": "track"},
            },
        ],
        max_features=1,
    )

    assert payload["type"] == "FeatureCollection"
    assert len(payload["features"]) == 1
    assert payload["features"][0]["geometry"]["type"] == "Point"
    assert payload["features"][0]["properties"]["tags"]["amenity"] == "veterinary"
    assert payload["metadata"]["truncated_feature_count"] == 1
    assert payload["metadata"]["omitted_feature_count"] == 1


def test_overpass_elements_to_geojson_reports_unsupported_relation_and_elements():
    payload = osm.overpass_elements_to_geojson(
        [
            {"type": "relation", "id": 10, "tags": {"type": "multipolygon"}},
            {"type": "node", "id": 11, "lon": "bad", "lat": 40.0},
            {"type": "area", "id": 12, "tags": {}},
        ],
        max_features=10,
    )

    assert payload["features"] == []
    assert payload["metadata"]["input_element_count"] == 3
    assert payload["metadata"]["omitted_feature_count"] == 3
    assert payload["metadata"]["unsupported_relation_count"] == 1
    assert payload["metadata"]["unsupported_element_count"] == 1
    assert payload["metadata"]["unsupported_geometry_count"] == 1


def test_osm_features_route_returns_geojson_metadata(monkeypatch, tmp_path):
    class FakeResponse:
        status_code = 200
        text = ""

        def json(self):
            return {
                "elements": [
                    {
                        "type": "node",
                        "id": 123,
                        "lon": -70.1,
                        "lat": 40.1,
                        "tags": {"amenity": "veterinary", "name": "Clinic"},
                    }
                ]
            }

    captured = {}

    def fake_post(url, *, data, headers, timeout):
        captured["url"] = url
        captured["data"] = data
        captured["headers"] = headers
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(osm.httpx, "post", fake_post)
    app = create_app(data_root=tmp_path / "data", static_root=tmp_path / "static")
    client = TestClient(app)

    response = client.post("/api/osm/features", json=point_query(max_features=10))

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"
    assert payload["metadata"]["source"]["name"] == "OpenStreetMap via Overpass API"
    assert payload["metadata"]["feature_count"] == 1
    assert payload["metadata"]["normalized_query"]["max_features"] == 10
    assert payload["metadata"]["scope_signature"]
    assert payload["metadata"]["query_signature"]
    assert payload["metadata"]["omitted_feature_count"] == 0
    assert payload["features"][0]["properties"]["name"] == "Clinic"
    assert captured["url"] == osm.OSM_SOURCE["url"]
    assert captured["headers"]["Accept"] == "application/json"
    assert captured["headers"]["User-Agent"] == osm.OVERPASS_USER_AGENT
    assert "out geom qt 11;" in captured["data"]["data"]


def test_osm_features_route_supports_bbox_exists_query(monkeypatch, tmp_path):
    class FakeResponse:
        status_code = 200
        text = ""

        def json(self):
            return {
                "elements": [
                    {
                        "type": "way",
                        "id": 456,
                        "geometry": [
                            {"lon": -70.1, "lat": 40.1},
                            {"lon": -70.05, "lat": 40.15},
                        ],
                        "tags": {"highway": "track"},
                    }
                ]
            }

    captured = {}

    def fake_post(url, *, data, headers, timeout):
        captured["data"] = data
        return FakeResponse()

    monkeypatch.setattr(osm.httpx, "post", fake_post)
    app = create_app(data_root=tmp_path / "data", static_root=tmp_path / "static")
    client = TestClient(app)

    response = client.post("/api/osm/features", json=bbox_query())

    assert response.status_code == 200
    payload = response.json()
    assert payload["features"][0]["geometry"]["type"] == "LineString"
    assert payload["features"][0]["properties"]["tags"]["highway"] == "track"
    assert 'way["highway"](40,-70.05,40.05,-70);' in captured["data"]["data"]


def test_osm_features_route_expands_in_query(monkeypatch, tmp_path):
    class FakeResponse:
        status_code = 200
        text = ""

        def json(self):
            return {"elements": []}

    captured = {}

    def fake_post(url, *, data, headers, timeout):
        captured["data"] = data
        return FakeResponse()

    monkeypatch.setattr(osm.httpx, "post", fake_post)
    app = create_app(data_root=tmp_path / "data", static_root=tmp_path / "static")
    client = TestClient(app)

    response = client.post(
        "/api/osm/features",
        json=point_query(
            selectors=[
                {
                    "tags": [
                        {
                            "key": "amenity",
                            "op": "in",
                            "values": ["veterinary", "clinic"],
                        }
                    ]
                }
            ],
            element_types=["node"],
        ),
    )

    assert response.status_code == 200
    query = captured["data"]["data"]
    assert 'node["amenity"="veterinary"](around:500,40.1,-70.1);' in query
    assert 'node["amenity"="clinic"](around:500,40.1,-70.1);' in query


def test_osm_features_route_returns_clear_validation_error(tmp_path):
    app = create_app(data_root=tmp_path / "data", static_root=tmp_path / "static")
    client = TestClient(app)

    response = client.post(
        "/api/osm/features",
        json=point_query(selectors=[{"tags": [{"key": "name", "op": "regex", "value": ".*"}]}]),
    )

    assert response.status_code == 400
    assert response.json()["error"] == "Unsupported OSM tag operator: regex"


def test_overpass_html_errors_are_summarized(monkeypatch, tmp_path):
    class FakeResponse:
        status_code = 406
        text = "<html><body><h1>Not Acceptable</h1><p>No representation.</p></body></html>"

    def fake_post(url, *, data, headers, timeout):
        return FakeResponse()

    monkeypatch.setattr(osm.httpx, "post", fake_post)
    app = create_app(data_root=tmp_path / "data", static_root=tmp_path / "static")
    client = TestClient(app)

    response = client.post("/api/osm/features", json=point_query(max_features=10))

    assert response.status_code == 502
    assert response.json()["error"] == "Overpass request failed with HTTP 406: Not Acceptable No representation."
