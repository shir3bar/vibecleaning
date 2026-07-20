import csv
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import sys
import threading

from fastapi.testclient import TestClient
import osmium
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MOVEMENT_APP_JS = REPO_ROOT / "examples" / "movement" / "static" / "app.js"

from app.osm import normalize_osm_request
from app.state import get_dataset_artifact, list_history, load_dataset, load_project_state
from app.web import create_app
from examples.movement.routes import (
    OSM_ENRICHMENT_SCRIPT,
    OSM_ENRICHMENT_TEMPLATE_PATH,
    register_movement_routes,
)
from examples.movement.osm_extracts import MAX_UNCONFIRMED_DOWNLOAD_BYTES
import examples.movement.osm_context as osm_context
from examples.movement.osm_context import (
    MAX_CONTEXT_SEARCH_RADIUS_M,
    OSM_CONTEXT_LAYER_SPECS,
    build_fix_osm_context,
    build_osm_fetch_scopes,
    distance_to_feature_m,
)
from examples.movement.summary import build_movement_fixes


ENRICHMENT_CSV = """eventid,individual,timestamp,longitude,latitude,set,source_note
fix_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train,retain me
fix_2,alpha,2024-01-01T01:00:00Z,-70.0,40.01,train,retain me too
"""


def _feature(feature_id: str, geometry: dict, tags: dict) -> dict:
    return {
        "type": "Feature",
        "id": feature_id,
        "geometry": geometry,
        "properties": {"tags": tags},
    }


def _create_enrichment_client(tmp_path: Path) -> tuple[TestClient, Path, str]:
    data_root = tmp_path / "data"
    study_dir = data_root / "movement_clean" / "test_study"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(ENRICHMENT_CSV, encoding="utf-8")
    app = create_app(
        data_root=data_root,
        static_root=REPO_ROOT / "examples" / "movement" / "static",
    )
    register_movement_routes(app, data_root=data_root)
    dataset_id = load_project_state(study_dir)["current_dataset_id"]
    return TestClient(app), study_dir, dataset_id


def _write_local_pbf(
    path: Path,
    *,
    lon: float = -70.0,
    lat: float = 40.0,
    way_id_offset: int = 0,
):
    writer = osmium.SimpleWriter(str(path))
    try:
        for node_id, lon, lat in [
            (1, lon, lat - 0.0001),
            (2, lon, lat + 0.0001),
            (3, lon - 0.0002, lat - 0.0001),
            (4, lon - 0.0002, lat + 0.0001),
            (5, lon - 0.0003, lat - 0.0001),
            (6, lon - 0.0003, lat + 0.0001),
        ]:
            writer.add_node(osmium.osm.mutable.Node(id=node_id, location=(lon, lat)))
        writer.add_way(
            osmium.osm.mutable.Way(id=1 + way_id_offset, nodes=[1, 2], tags={"highway": "track"})
        )
        writer.add_way(
            osmium.osm.mutable.Way(id=2 + way_id_offset, nodes=[3, 4], tags={"railway": "rail"})
        )
        writer.add_way(
            osmium.osm.mutable.Way(id=3 + way_id_offset, nodes=[5, 6], tags={"building": "yes"})
        )
    finally:
        writer.close()


def _start_geofabrik_server(tmp_path: Path, mode: str = "success") -> tuple[HTTPServer, threading.Thread]:
    pbf_path = tmp_path / "test-region.osm.pbf"
    _write_local_pbf(pbf_path)
    pbf_contents = {"test-region": pbf_path.read_bytes()}
    if mode in {"two_region", "two_region_large_unconfirmed"}:
        denmark_pbf_path = tmp_path / "denmark-region.osm.pbf"
        _write_local_pbf(denmark_pbf_path, lon=12.51, lat=55.71, way_id_offset=100)
        pbf_contents["denmark-region"] = denmark_pbf_path.read_bytes()

    class FakeGeofabrikHandler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            if self.path in {f"/{region_id}.osm.pbf" for region_id in pbf_contents}:
                region_id = self.path.removeprefix("/").removesuffix(".osm.pbf")
                if mode == "download_failure":
                    self.send_response(503)
                    self.end_headers()
                    return
                body = b"not an osm pbf" if mode == "invalid_pbf" else pbf_contents[region_id]
                content_length = (
                    MAX_UNCONFIRMED_DOWNLOAD_BYTES + 1
                    if mode == "large_unconfirmed"
                    else (MAX_UNCONFIRMED_DOWNLOAD_BYTES // 2) + 1
                    if mode == "two_region_large_unconfirmed"
                    else len(body)
                )
                self.send_response(200)
                self.send_header("Content-Length", str(content_length))
                self.send_header("ETag", '"test-pbf"')
                self.end_headers()
                return
            self.send_response(404)
            self.end_headers()

        def do_GET(self):
            if self.path == "/index-v1.json":
                if mode == "coverage_failure":
                    coordinates = [
                        [[[-80.0, 30.0], [-79.0, 30.0], [-79.0, 31.0], [-80.0, 31.0], [-80.0, 30.0]]]
                    ]
                else:
                    coordinates = [
                        [[[-71.0, 39.0], [-69.0, 39.0], [-69.0, 41.0], [-71.0, 41.0], [-71.0, 39.0]]]
                    ]
                features = [
                    {
                        "type": "Feature",
                        "properties": {
                            "id": "test-region",
                            "name": "Test Region",
                            "urls": {
                                "pbf": f"http://127.0.0.1:{self.server.server_port}/test-region.osm.pbf"
                            },
                        },
                        "geometry": {"type": "MultiPolygon", "coordinates": coordinates},
                    }
                ]
                if mode in {"two_region", "two_region_large_unconfirmed"}:
                    features.append(
                        {
                            "type": "Feature",
                            "properties": {
                                "id": "denmark-region",
                                "name": "Denmark Region",
                                "urls": {
                                    "pbf": f"http://127.0.0.1:{self.server.server_port}/denmark-region.osm.pbf"
                                },
                            },
                            "geometry": {
                                "type": "MultiPolygon",
                                "coordinates": [
                                    [[[7.0, 54.0], [16.0, 54.0], [16.0, 58.0], [7.0, 58.0], [7.0, 54.0]]]
                                ],
                            },
                        }
                    )
                payload = {"type": "FeatureCollection", "features": features}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("ETag", '"test-index"')
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path in {f"/{region_id}.osm.pbf" for region_id in pbf_contents}:
                self.server.pbf_get_count += 1
                region_id = self.path.removeprefix("/").removesuffix(".osm.pbf")
                if mode == "invalid_pbf":
                    body = b"not an osm pbf"
                else:
                    body = pbf_contents[region_id]
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()

        def do_POST(self):
            self.server.overpass_request_count += 1
            self.send_response(500)
            self.end_headers()

        def log_message(self, format, *args):
            return

    server = HTTPServer(("127.0.0.1", 0), FakeGeofabrikHandler)
    server.overpass_request_count = 0
    server.pbf_get_count = 0
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_initial_osm_context_layers_are_road_and_railway_ways_only():
    assert set(OSM_CONTEXT_LAYER_SPECS) == {"road", "railway"}
    assert OSM_CONTEXT_LAYER_SPECS["road"]["layer_name"] == "road"
    assert OSM_CONTEXT_LAYER_SPECS["road"]["selectors"] == [
        {"tags": [{"key": "highway", "op": "exists"}]}
    ]
    assert OSM_CONTEXT_LAYER_SPECS["road"]["element_types"] == ["way"]
    assert OSM_CONTEXT_LAYER_SPECS["road"]["class_tag"] == "highway"
    assert OSM_CONTEXT_LAYER_SPECS["road"]["output_columns"] == {
        "distance_m": "osm:nearest_road_distance_m",
        "class": "osm:nearest_road_class",
        "match_status": "osm:road_match_status",
    }
    assert OSM_CONTEXT_LAYER_SPECS["railway"]["layer_name"] == "railway"
    assert OSM_CONTEXT_LAYER_SPECS["railway"]["selectors"] == [
        {"tags": [{"key": "railway", "op": "exists"}]}
    ]
    assert OSM_CONTEXT_LAYER_SPECS["railway"]["element_types"] == ["way"]
    assert OSM_CONTEXT_LAYER_SPECS["railway"]["class_tag"] == "railway"
    assert OSM_CONTEXT_LAYER_SPECS["railway"]["output_columns"] == {
        "distance_m": "osm:nearest_railway_distance_m",
        "class": "osm:nearest_railway_class",
        "match_status": "osm:railway_match_status",
    }


def test_build_fix_osm_context_returns_nearest_road_and_railway_values():
    fixes = [{"fix_key": "fix-1", "lon": -70.0, "lat": 40.0}]
    context = build_fix_osm_context(
        fixes,
        {
            "road": [
                _feature(
                    "way/road",
                    {"type": "LineString", "coordinates": [[-70.01, 40.0], [-69.99, 40.0]]},
                    {"highway": "track"},
                )
            ],
            "railway": [
                _feature(
                    "way/rail",
                    {"type": "Point", "coordinates": [-70.0, 40.001]},
                    {"railway": "rail"},
                )
            ],
        },
        search_radius_m=200.0,
    )["fix-1"]

    assert context["osm:nearest_road_distance_m"] == pytest.approx(0.0)
    assert context["osm:nearest_road_class"] == "track"
    assert context["osm:road_match_status"] == "matched"
    assert context["osm:nearest_railway_distance_m"] == pytest.approx(111.2, abs=0.2)
    assert context["osm:nearest_railway_class"] == "rail"
    assert context["osm:railway_match_status"] == "matched"
    assert not any("anomaly" in field.lower() for field in context)


@pytest.mark.parametrize(
    "geometry",
    [
        {"type": "Point", "coordinates": [-70.0, 40.0]},
        {"type": "LineString", "coordinates": [[-70.01, 40.0], [-69.99, 40.0]]},
        {
            "type": "Polygon",
            "coordinates": [
                [[-70.01, 39.99], [-69.99, 39.99], [-69.99, 40.01], [-70.01, 40.01], [-70.01, 39.99]]
            ],
        },
        {
            "type": "MultiLineString",
            "coordinates": [[[-70.01, 40.0], [-69.99, 40.0]]],
        },
        {
            "type": "MultiPolygon",
            "coordinates": [
                [
                    [[-70.01, 39.99], [-69.99, 39.99], [-69.99, 40.01], [-70.01, 40.01], [-70.01, 39.99]]
                ]
            ],
        },
    ],
)
def test_distance_to_supported_geometry_types(geometry):
    assert distance_to_feature_m(-70.0, 40.0, geometry) == pytest.approx(0.0)


def test_build_fix_osm_context_records_radius_censored_no_match():
    context = build_fix_osm_context(
        [{"fix_key": "fix-1", "lon": -70.0, "lat": 40.0}],
        {
            "road": [
                _feature(
                    "way/road",
                    {"type": "Point", "coordinates": [-70.0, 40.01]},
                    {"highway": "residential"},
                )
            ],
            "railway": [],
        },
        search_radius_m=50.0,
    )["fix-1"]

    assert context == {
        "osm:nearest_road_distance_m": None,
        "osm:nearest_road_class": "",
        "osm:road_match_status": "not_found_within_radius",
        "osm:nearest_railway_distance_m": None,
        "osm:nearest_railway_class": "",
        "osm:railway_match_status": "not_found_within_radius",
    }


def test_build_fix_osm_context_limits_distance_checks_to_spatial_candidates(monkeypatch):
    road_features = [
        _feature(
            f"way/far_{index}",
            {"type": "Point", "coordinates": [-60.0 + (index / 10000.0), 35.0]},
            {"highway": "residential"},
        )
        for index in range(1000)
    ]
    road_features.append(
        _feature(
            "way/near",
            {"type": "Point", "coordinates": [-70.0, 40.0]},
            {"highway": "track"},
        )
    )
    calls = 0
    original = osm_context.distance_to_feature_m

    def counting_distance(lon, lat, geometry):
        nonlocal calls
        calls += 1
        return original(lon, lat, geometry)

    monkeypatch.setattr(osm_context, "distance_to_feature_m", counting_distance)
    context = build_fix_osm_context(
        [{"fix_key": "fix-1", "lon": -70.0, "lat": 40.0}],
        {"road": road_features, "railway": []},
        search_radius_m=50.0,
    )["fix-1"]

    assert context["osm:nearest_road_class"] == "track"
    assert calls == 1


def test_build_fix_osm_context_can_use_an_additional_layer_spec_without_new_distance_logic():
    context = build_fix_osm_context(
        [{"fix_key": "fix-1", "lon": -70.0, "lat": 40.0}],
        {
            "test_layer": [
                _feature(
                    "way/test",
                    {"type": "Point", "coordinates": [-70.0, 40.0]},
                    {"test_class": "example"},
                )
            ]
        },
        search_radius_m=50.0,
        layer_specs=[
            {
                "layer_name": "test_layer",
                "selectors": [{"tags": [{"key": "test_class", "op": "exists"}]}],
                "element_types": ["way"],
                "class_tag": "test_class",
                "output_columns": {
                    "distance_m": "osm:test_distance_m",
                    "class": "osm:test_class",
                    "match_status": "osm:test_match_status",
                },
            }
        ],
    )["fix-1"]

    assert context == {
        "osm:test_distance_m": pytest.approx(0.0),
        "osm:test_class": "example",
        "osm:test_match_status": "matched",
    }


def test_osm_context_is_pure_and_not_coupled_to_anomaly_modules():
    source = (REPO_ROOT / "examples" / "movement" / "osm_context.py").read_text(encoding="utf-8")

    assert "fetch_osm_features" not in source
    assert "anomaly_ranking" not in source
    assert "anomaly_analysis_template" not in source


def test_maximum_enrichment_radius_produces_valid_broad_exists_fetch_scope():
    scope = build_osm_fetch_scopes(
        [{"fix_key": "fix-1", "lon": -70.0, "lat": 40.0}],
        search_radius_m=MAX_CONTEXT_SEARCH_RADIUS_M,
    )[0]

    normalized = normalize_osm_request(
        {
            "scope": scope,
            "selectors": [{"tags": [{"key": "highway", "op": "exists"}]}],
            "element_types": ["way"],
        }
    )

    assert normalized["scope"] == scope


def test_osm_enrichment_template_is_compilable():
    template_text = OSM_ENRICHMENT_TEMPLATE_PATH.read_text(encoding="utf-8").strip() + "\n"

    assert OSM_ENRICHMENT_SCRIPT == template_text
    compile(OSM_ENRICHMENT_SCRIPT, str(OSM_ENRICHMENT_TEMPLATE_PATH), "exec")
    assert "fetch_osm_features" not in template_text


def test_frontend_does_not_expose_temporary_preprocessing_osm_enrichment_control():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert 'data-role="osm-enrichment-radius"' not in source
    assert 'data-role="test-osm-enrichment"' not in source
    assert "Test OSM context enrichment" not in source
    assert "testOsmContextEnrichment" not in source


def test_osm_enrichment_route_creates_derived_artifact_and_exposes_attributes(monkeypatch, tmp_path):
    client, study_dir, dataset_id = _create_enrichment_client(tmp_path)
    original_path = study_dir / "movement.csv"
    original_text = original_path.read_text(encoding="utf-8")
    server, thread = _start_geofabrik_server(tmp_path)
    monkeypatch.setenv(
        "VIBECLEANING_GEOFABRIK_INDEX_URL",
        f"http://127.0.0.1:{server.server_port}/index-v1.json",
    )
    monkeypatch.setenv(
        "VIBECLEANING_OVERPASS_URL",
        f"http://127.0.0.1:{server.server_port}/should-not-be-called",
    )
    try:
        response = client.post(
            "/api/apps/movement/family/movement_clean/study/test_study/actions/enrich-osm-context",
            json={
                "dataset_id": dataset_id,
                "logical_name": "movement.csv",
                "search_radius_m": 50,
                "user": "reviewer",
            },
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert response.status_code == 200
    assert server.overpass_request_count == 0
    step = response.json()["step"]
    summary = step["summary"]
    output_dataset_id = step["output_dataset_id"]
    dataset = load_dataset(study_dir, output_dataset_id)
    assert dataset["parent_dataset_id"] == dataset_id
    assert {artifact["logical_name"] for artifact in dataset["artifacts"]} == {
        "movement.csv",
        "movement_osm_context.csv",
    }
    assert original_path.read_text(encoding="utf-8") == original_text
    _, output_path = get_dataset_artifact(study_dir, output_dataset_id, "movement_osm_context.csv")
    with original_path.open("r", newline="", encoding="utf-8") as source_handle:
        source_rows = list(csv.DictReader(source_handle))
    with output_path.open("r", newline="", encoding="utf-8") as output_handle:
        output_reader = csv.DictReader(output_handle)
        output_fields = list(output_reader.fieldnames or [])
        output_rows = list(output_reader)

    appended_columns = [
        "osm:nearest_road_distance_m",
        "osm:nearest_road_class",
        "osm:road_match_status",
        "osm:nearest_railway_distance_m",
        "osm:nearest_railway_class",
        "osm:railway_match_status",
    ]
    assert output_fields == [*list(source_rows[0]), *appended_columns]
    assert len(output_rows) == len(source_rows)
    for source_row, output_row in zip(source_rows, output_rows):
        assert {field: output_row[field] for field in source_row} == source_row
    assert output_rows[0]["osm:nearest_road_class"] == "track"
    assert output_rows[0]["osm:road_match_status"] == "matched"
    assert output_rows[0]["osm:nearest_railway_class"] == "rail"
    assert output_rows[0]["osm:railway_match_status"] == "matched"
    assert float(output_rows[0]["osm:nearest_road_distance_m"]) == pytest.approx(0.0)
    assert float(output_rows[0]["osm:nearest_railway_distance_m"]) < 50.0
    assert output_rows[1]["osm:nearest_road_distance_m"] == ""
    assert output_rows[1]["osm:nearest_road_class"] == ""
    assert output_rows[1]["osm:road_match_status"] == "not_found_within_radius"
    assert output_rows[1]["osm:nearest_railway_distance_m"] == ""
    assert output_rows[1]["osm:nearest_railway_class"] == ""
    assert output_rows[1]["osm:railway_match_status"] == "not_found_within_radius"
    assert not any("anomaly" in field.lower() for field in output_fields)

    assert step["parameters"]["action"] == "enrich_osm_context"
    assert step["parameters"]["search_radius_m"] == 50.0
    assert summary["run_status"] == "completed"
    assert summary["source_type"] == "local_extract"
    assert summary["search_radius_m"] == 50.0
    assert summary["input_row_count"] == summary["output_row_count"] == 2
    assert summary["appended_columns"] == appended_columns
    assert summary["coverage_model"] == "occupied_grid_footprints_v1"
    assert summary["footprint_count"] == 1
    assert summary["initial_region_ids"] == ["test-region"]
    assert summary["selected_region_ids"] == ["test-region"]
    assert list(summary["footprint_region_assignments"]) == ["test-region"]
    assert summary["overall_reporting_extent"]["valid_fix_count"] == 2
    assert summary["coverage_validation"]["validated"] is True
    assert summary["coverage_validation"]["region_id"] == "test-region"
    assert summary["coverage_validation"]["selected_region_ids"] == ["test-region"]
    assert summary["registry_cache"]["cache_hit"] is False
    assert summary["source_cache"]["cache_hit"] is False
    assert summary["source_cache"]["source_revision"]
    assert summary["feature_cache"]["cache_hit"] is False
    assert summary["feature_cache"]["complete"] is True
    assert summary["feature_cache"]["feature_counts"] == {"road": 1, "railway": 1}
    assert [layer["layer_name"] for layer in summary["layers"]] == ["road", "railway"]
    assert [layer["feature_count"] for layer in summary["layers"]] == [1, 1]
    assert summary["merged_duplicate_counts"] == {"road": 0, "railway": 0, "total": 0}
    assert [cache["region_id"] for cache in summary["source_caches"]] == ["test-region"]
    assert [cache["region_id"] for cache in summary["feature_caches"]] == ["test-region"]

    fixes_response = client.get(
        f"/api/apps/movement/family/movement_clean/study/test_study/dataset/{output_dataset_id}/fixes",
        params={"logical_name": "movement_osm_context.csv"},
    )
    assert fixes_response.status_code == 200
    fixes = {fix["fix_key"]: fix for fix in fixes_response.json()["fixes"]}
    first_attributes = fixes["id:fix_1#row:1"]["attributes"]
    second_attributes = fixes["id:fix_2#row:2"]["attributes"]
    assert first_attributes["osm:nearest_road_distance_m"] == pytest.approx(0.0)
    assert first_attributes["osm:nearest_road_class"] == "track"
    assert first_attributes["osm:road_match_status"] == "matched"
    assert first_attributes["osm:nearest_railway_class"] == "rail"
    assert second_attributes["osm:road_match_status"] == "not_found_within_radius"
    assert second_attributes["osm:railway_match_status"] == "not_found_within_radius"


def test_osm_enrichment_route_supports_dispersed_multi_region_extracts(monkeypatch, tmp_path):
    client, study_dir, dataset_id = _create_enrichment_client(tmp_path)
    (study_dir / "movement.csv").write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_2,beta,2024-01-01T01:00:00Z,12.51,55.71,train
""",
        encoding="utf-8",
    )
    server, thread = _start_geofabrik_server(tmp_path, "two_region")
    monkeypatch.setenv(
        "VIBECLEANING_GEOFABRIK_INDEX_URL",
        f"http://127.0.0.1:{server.server_port}/index-v1.json",
    )
    monkeypatch.setenv(
        "VIBECLEANING_OVERPASS_URL",
        f"http://127.0.0.1:{server.server_port}/should-not-be-called",
    )
    try:
        response = client.post(
            "/api/apps/movement/family/movement_clean/study/test_study/actions/enrich-osm-context",
            json={
                "dataset_id": dataset_id,
                "logical_name": "movement.csv",
                "search_radius_m": 50,
                "user": "reviewer",
            },
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert response.status_code == 200
    assert server.overpass_request_count == 0
    assert server.pbf_get_count == 2
    step = response.json()["step"]
    summary = step["summary"]
    _, output_path = get_dataset_artifact(
        study_dir,
        step["output_dataset_id"],
        "movement_osm_context.csv",
    )
    with output_path.open("r", newline="", encoding="utf-8") as output_handle:
        output_rows = list(csv.DictReader(output_handle))

    assert [row["osm:nearest_road_class"] for row in output_rows] == ["track", "track"]
    assert [row["osm:nearest_railway_class"] for row in output_rows] == ["rail", "rail"]
    assert summary["coverage_model"] == "occupied_grid_footprints_v1"
    assert summary["footprint_count"] == 2
    assert summary["coverage_validation"]["region_id"] is None
    assert summary["selected_region_ids"] == ["denmark-region", "test-region"]
    assert summary["initial_region_ids"] == ["denmark-region", "test-region"]
    assert set(summary["footprint_region_assignments"]) == {"denmark-region", "test-region"}
    assert len(summary["source_caches"]) == 2
    assert len(summary["feature_caches"]) == 2
    assert summary["feature_cache"]["feature_counts"] == {"road": 2, "railway": 2}
    assert summary["feature_cache"]["cache_hit"] is None
    assert summary["merged_duplicate_counts"] == {"road": 0, "railway": 0, "total": 0}


def test_osm_enrichment_route_reuses_completed_equivalent_artifact(monkeypatch, tmp_path):
    client, study_dir, dataset_id = _create_enrichment_client(tmp_path)
    server, thread = _start_geofabrik_server(tmp_path)
    monkeypatch.setenv(
        "VIBECLEANING_GEOFABRIK_INDEX_URL",
        f"http://127.0.0.1:{server.server_port}/index-v1.json",
    )
    request_payload = {
        "dataset_id": dataset_id,
        "logical_name": "movement.csv",
        "search_radius_m": 50,
        "user": "reviewer",
    }
    try:
        initial_response = client.post(
            "/api/apps/movement/family/movement_clean/study/test_study/actions/enrich-osm-context",
            json=request_payload,
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert initial_response.status_code == 200
    initial_result = initial_response.json()
    initial_step_id = initial_result["step"]["step_id"]
    initial_dataset_id = initial_result["dataset"]["dataset_id"]

    repeated_response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/enrich-osm-context",
        json=request_payload,
    )

    assert repeated_response.status_code == 200
    repeated_result = repeated_response.json()
    assert repeated_result["reused"] is True
    assert repeated_result["step"]["step_id"] == initial_step_id
    assert repeated_result["dataset"]["dataset_id"] == initial_dataset_id
    assert len(list_history(study_dir)["steps"]) == 1


def test_osm_enrichment_route_requires_explicit_search_radius(tmp_path):
    client, _study_dir, dataset_id = _create_enrichment_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/enrich-osm-context",
        json={"dataset_id": dataset_id, "logical_name": "movement.csv", "user": "reviewer"},
    )

    assert response.status_code == 400
    assert response.json()["error"] == "search_radius_m is required"


def test_local_osm_enrichment_accepts_radius_above_overpass_fetch_bound(monkeypatch, tmp_path):
    client, _study_dir, dataset_id = _create_enrichment_client(tmp_path)
    server, thread = _start_geofabrik_server(tmp_path)
    monkeypatch.setenv(
        "VIBECLEANING_GEOFABRIK_INDEX_URL",
        f"http://127.0.0.1:{server.server_port}/index-v1.json",
    )
    try:
        response = client.post(
            "/api/apps/movement/family/movement_clean/study/test_study/actions/enrich-osm-context",
            json={
                "dataset_id": dataset_id,
                "logical_name": "movement.csv",
                "search_radius_m": MAX_CONTEXT_SEARCH_RADIUS_M + 1000,
                "user": "reviewer",
            },
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert response.status_code == 200
    assert response.json()["step"]["summary"]["search_radius_m"] == MAX_CONTEXT_SEARCH_RADIUS_M + 1000


def test_local_osm_enrichment_requires_confirmation_before_large_uncached_download(monkeypatch, tmp_path):
    client, study_dir, dataset_id = _create_enrichment_client(tmp_path)
    server, thread = _start_geofabrik_server(tmp_path, "large_unconfirmed")
    monkeypatch.setenv(
        "VIBECLEANING_GEOFABRIK_INDEX_URL",
        f"http://127.0.0.1:{server.server_port}/index-v1.json",
    )
    try:
        response = client.post(
            "/api/apps/movement/family/movement_clean/study/test_study/actions/enrich-osm-context",
            json={
                "dataset_id": dataset_id,
                "logical_name": "movement.csv",
                "search_radius_m": 50,
                "user": "reviewer",
            },
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert response.status_code == 400
    assert "requires confirmation before download" in response.json()["error"]
    assert "confirmed_large_download=true" in response.json()["error"]
    assert server.pbf_get_count == 0
    assert not list(study_dir.rglob("movement_osm_context.csv"))


def test_multi_region_osm_enrichment_requires_confirmation_before_any_download(monkeypatch, tmp_path):
    client, study_dir, dataset_id = _create_enrichment_client(tmp_path)
    (study_dir / "movement.csv").write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_2,beta,2024-01-01T01:00:00Z,12.51,55.71,train
""",
        encoding="utf-8",
    )
    server, thread = _start_geofabrik_server(tmp_path, "two_region_large_unconfirmed")
    monkeypatch.setenv(
        "VIBECLEANING_GEOFABRIK_INDEX_URL",
        f"http://127.0.0.1:{server.server_port}/index-v1.json",
    )
    try:
        response = client.post(
            "/api/apps/movement/family/movement_clean/study/test_study/actions/enrich-osm-context",
            json={
                "dataset_id": dataset_id,
                "logical_name": "movement.csv",
                "search_radius_m": 50,
                "user": "reviewer",
            },
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert response.status_code == 400
    assert "requires confirmation before download" in response.json()["error"]
    assert "Aggregate uncached OSM download size exceeds" in response.json()["error"]
    assert server.pbf_get_count == 0
    assert not list(study_dir.rglob("movement_osm_context.csv"))


def test_summary_exposes_high_cardinality_persisted_osm_attributes(tmp_path):
    csv_path = tmp_path / "movement_osm_context.csv"
    rows = [
        f"fix_{index},alpha,2024-01-01T00:{index:02d}:00Z,-70.0,40.0,class_{index},matched"
        for index in range(13)
    ]
    csv_path.write_text(
        (
            "eventid,individual,timestamp,longitude,latitude,"
            "osm:nearest_road_class,osm:road_match_status\n"
            + "\n".join(rows)
            + "\n"
        ),
        encoding="utf-8",
    )

    payload = build_movement_fixes(csv_path, limit=None)

    assert {
        fix["attributes"]["osm:nearest_road_class"] for fix in payload["fixes"]
    } == {f"class_{index}" for index in range(13)}
    assert all(
        fix["attributes"]["osm:road_match_status"] == "matched"
        for fix in payload["fixes"]
    )


@pytest.mark.parametrize("mode", ["coverage_failure", "download_failure", "invalid_pbf"])
def test_osm_enrichment_local_source_failure_does_not_complete_artifact(monkeypatch, tmp_path, mode):
    client, study_dir, dataset_id = _create_enrichment_client(tmp_path)
    server, thread = _start_geofabrik_server(tmp_path, mode)
    monkeypatch.setenv(
        "VIBECLEANING_GEOFABRIK_INDEX_URL",
        f"http://127.0.0.1:{server.server_port}/index-v1.json",
    )
    try:
        response = client.post(
            "/api/apps/movement/family/movement_clean/study/test_study/actions/enrich-osm-context",
            json={
                "dataset_id": dataset_id,
                "logical_name": "movement.csv",
                "search_radius_m": 50,
                "user": "reviewer",
            },
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert response.status_code == 400
    assert load_project_state(study_dir)["current_dataset_id"] == dataset_id
    assert not list(study_dir.rglob("movement_osm_context.csv"))
