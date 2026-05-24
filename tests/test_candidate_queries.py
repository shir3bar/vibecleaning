import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import sys
import threading

from fastapi.testclient import TestClient
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.query_library import get_query, list_queries, query_library_path, save_query
from app.state import ProjectStateError, load_project_state, project_paths
from app.web import create_app, get_project_dir
from examples.movement import candidate_queries
from examples.movement.catalog import get_study_dir, validate_catalog_part
from examples.movement.candidate_queries import run_candidate_query, run_fix_numeric_candidate_query
from examples.movement.routes import register_movement_routes


MOVEMENT_APP_JS = REPO_ROOT / "examples" / "movement" / "static" / "app.js"

FAST_MOVEMENT_CSV = """eventid,individual,timestamp,longitude,latitude,set
fix_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_2,alpha,2024-01-01T01:00:00Z,-68.0,40.0,train
fix_3,alpha,2024-01-01T02:00:00Z,-67.999,40.0,train
fix_4,beta,2024-01-01T00:00:00Z,-70.0,41.0,train
fix_5,beta,2024-01-01T01:00:00Z,-69.999,41.0,train
"""


def numeric_query_definition(field: str = "speed_mps", threshold: float = 120 / 3.6) -> dict:
    return {
        "app": "movement",
        "name": "Fast fixes",
        "description": "Find fixes above a speed threshold.",
        "candidate_kind": "fix",
        "evaluator": {"type": "fix_numeric_comparison"},
        "definition": {"field": field, "op": ">", "value": threshold},
        "parameters": {},
        "required_fields": [field],
    }


def parameterized_numeric_query_definition() -> dict:
    query = numeric_query_definition(threshold=0)
    query["definition"] = {"field": "speed_mps", "op": ">", "value": "$threshold"}
    query["parameters"] = {"threshold": {"type": "number", "default": 120 / 3.6}}
    return query


def osm_proximity_query_definition(*, distance_m: object = 50, allow_whole_study: bool = False) -> dict:
    definition = {
        "osm": {
            "selectors": [{"tags": [{"key": "highway", "op": "exists"}]}],
            "element_types": ["way"],
        },
        "distance_m": distance_m,
    }
    if allow_whole_study:
        definition["allow_whole_study_osm"] = True
    return {
        "app": "movement",
        "name": "Near mapped roads",
        "description": "Find fixes near selected OSM features.",
        "candidate_kind": "fix",
        "evaluator": {"type": "fix_osm_proximity"},
        "definition": definition,
        "parameters": {},
        "required_fields": ["lon", "lat"],
    }


def mocked_osm_feature_collection(features, scope, **metadata):
    payload = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "scope": scope,
            "scope_signature": "scope_sig",
            "query_signature": "query_sig",
            "feature_count": len(features),
            "omitted_feature_count": 0,
            "unsupported_relation_count": 0,
            "unsupported_element_count": 0,
            "unsupported_geometry_count": 0,
            "truncated_feature_count": 0,
            "fetched_at": "2026-01-01T00:00:00+00:00",
        },
    }
    payload["metadata"].update(metadata)
    return payload


def create_candidate_query_client(tmp_path: Path) -> tuple[TestClient, Path, str]:
    data_root = tmp_path / "data"
    study_dir = data_root / "movement_clean" / "test_study"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(FAST_MOVEMENT_CSV, encoding="utf-8")
    app = create_app(
        data_root=data_root,
        static_root=REPO_ROOT / "examples" / "movement" / "static",
    )
    register_movement_routes(app, data_root=data_root)
    dataset_id = load_project_state(study_dir)["current_dataset_id"]
    return TestClient(app), study_dir, dataset_id


def test_query_library_crud_and_immutable_versions(tmp_path):
    data_root = tmp_path / "data"
    first = save_query(
        data_root,
        {
            **numeric_query_definition(),
            "query_id": "fast_fixes",
            "created_by": "reviewer",
        },
    )
    second = save_query(
        data_root,
        {
            **numeric_query_definition(threshold=30.0),
            "query_id": "fast_fixes",
            "name": "Very fast fixes",
            "created_by": "reviewer",
        },
    )

    assert query_library_path(data_root) == data_root.resolve() / ".vibecleaning" / "query_library.json"
    assert first["version"] == 1
    assert second["version"] == 2
    assert get_query(data_root, "fast_fixes", version=1)["name"] == "Fast fixes"
    assert get_query(data_root, "fast_fixes")["name"] == "Very fast fixes"
    assert [query["version"] for query in list_queries(data_root, app="movement")] == [1, 2]


def test_query_library_rejects_malformed_records(tmp_path):
    data_root = tmp_path / "data"
    path = query_library_path(data_root)
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "queries": [
                    {
                        **numeric_query_definition(),
                        "query_id": "bad_version",
                        "version": 0,
                        "created_by": "reviewer",
                        "created_at": "2026-01-01T00:00:00+00:00",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ProjectStateError, match="Invalid query version"):
        list_queries(data_root)


def test_query_library_rejects_hidden_query_ids(tmp_path):
    with pytest.raises(ProjectStateError, match="Invalid query id"):
        save_query(
            tmp_path / "data",
            {
                **numeric_query_definition(),
                "query_id": ".hidden_query",
                "created_by": "reviewer",
            },
        )


def test_query_library_routes(tmp_path):
    data_root = tmp_path / "data"
    app = create_app(
        data_root=data_root,
        static_root=REPO_ROOT / "examples" / "movement" / "static",
    )
    client = TestClient(app)

    response = client.post(
        "/api/query-library/queries",
        json={
            **numeric_query_definition(),
            "query_id": "fast_fixes",
            "created_by": "reviewer",
        },
    )
    assert response.status_code == 200
    assert response.json()["query"]["version"] == 1

    listing = client.get("/api/query-library/queries", params={"app": "movement"})
    assert listing.status_code == 200
    assert listing.json()["queries"][0]["query_id"] == "fast_fixes"

    fetched = client.get("/api/query-library/queries/fast_fixes")
    assert fetched.status_code == 200
    assert fetched.json()["query"]["query_id"] == "fast_fixes"


def test_hidden_project_name_rejected(tmp_path):
    data_root = tmp_path / "data"
    (data_root / ".vibecleaning").mkdir(parents=True)

    with pytest.raises(ValueError):
        get_project_dir(data_root, ".vibecleaning")

    app = create_app(
        data_root=data_root,
        static_root=REPO_ROOT / "examples" / "movement" / "static",
    )
    client = TestClient(app)
    response = client.get("/api/project/.vibecleaning/state")
    assert response.status_code == 404


def test_hidden_movement_catalog_parts_rejected(tmp_path):
    data_root = tmp_path / "data"
    hidden_study = data_root / "movement_clean" / ".hidden_study"
    hidden_study.mkdir(parents=True)
    (hidden_study / "movement.csv").write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid family"):
        validate_catalog_part(".hidden_family", label="family")
    with pytest.raises(ValueError, match="Invalid study"):
        validate_catalog_part(".hidden_study", label="study")
    with pytest.raises(ValueError, match="Invalid study"):
        get_study_dir(data_root, "movement_clean", ".hidden_study")


def test_numeric_candidate_query_evaluator_uses_movement_fixes(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    result = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=numeric_query_definition(),
        dataset_id="dataset_test",
        logical_name="movement.csv",
    )

    assert result["run_status"] == "success"
    assert result["execution_scope"]["requested"] == {"type": "whole_study"}
    assert result["execution_scope"]["resolved"]["type"] == "whole_study"
    assert result["scope_results"][0]["scope_id"] == "whole_study"
    assert result["run_digest"]
    assert result["candidate_count"] == 1
    assert result["returned_count"] == 1
    assert result["candidates"][0]["fix_key"] == "id:fix_2#row:2"
    assert result["candidates"][0]["scope_id"] == "whole_study"
    assert result["candidates"][0]["candidate_id"].startswith("cq:")
    evidence = result["candidates"][0]["evidence"]
    assert evidence["field"] == "speed_mps"
    assert evidence["unit"] == "m/s"
    assert evidence["display_unit"] == "km/h"
    assert evidence["threshold_display"] == "120 km/h"
    assert result["evaluator_provenance"]["implementation_version"] == "movement-candidate-query-v2"
    assert result["evaluator_provenance"]["source_digest"]


@pytest.mark.parametrize(
    ("execution_scope", "warning"),
    [
        ({}, "Execution scope type is missing."),
        ({"type": ""}, "Execution scope type is missing."),
        ({"type": "unsupported_scope"}, "Unsupported execution scope type: unsupported_scope"),
        ({"type": "individual"}, "Individual execution scope is missing an individual id."),
    ],
)
def test_malformed_candidate_query_scopes_are_unresolved_not_whole_study(tmp_path, execution_scope, warning):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    result = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=numeric_query_definition(),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope=execution_scope,
    )

    assert result["run_status"] == "unresolved"
    assert result["candidate_count"] == 0
    assert result["execution_scope"]["requested"].get("type", "") == str(execution_scope.get("type", "")).strip()
    assert result["execution_scope"]["resolved"]["type"] != "whole_study"
    assert warning in result["warnings"]


def test_numeric_candidate_query_individual_scope_filters_fixes(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    alpha = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=numeric_query_definition(),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "alpha"},
    )
    beta = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=numeric_query_definition(),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "beta"},
    )

    assert alpha["run_status"] == "success"
    assert alpha["execution_scope"]["resolved"]["type"] == "individual"
    assert alpha["execution_scope"]["resolved"]["individual"] == "alpha"
    assert alpha["candidate_count"] == 1
    assert alpha["candidates"][0]["scope_id"] == "individual:alpha"
    assert beta["run_status"] == "success"
    assert beta["candidate_count"] == 0
    assert beta["candidates"] == []


def test_numeric_candidate_query_current_individual_normalizes_to_individual(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    result = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=numeric_query_definition(),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "current_individual", "individual": "alpha"},
    )

    assert result["run_status"] == "success"
    assert result["execution_scope"]["requested"]["type"] == "current_individual"
    assert result["execution_scope"]["resolved"]["type"] == "individual"
    assert result["execution_scope"]["resolved"]["individual"] == "alpha"
    assert result["scope_results"][0]["scope_id"] == "individual:alpha"


def test_numeric_candidate_query_all_individuals_per_individual_scope_results(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    result = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=numeric_query_definition(),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "all_individuals_per_individual"},
    )

    assert result["run_status"] == "success"
    assert result["execution_scope"]["resolved"]["type"] == "all_individuals_per_individual"
    assert result["execution_scope"]["resolved"]["scope_ids"] == ["individual:alpha", "individual:beta"]
    assert [scope["scope_id"] for scope in result["scope_results"]] == ["individual:alpha", "individual:beta"]
    assert [scope["candidate_count"] for scope in result["scope_results"]] == [1, 0]
    assert result["candidate_count"] == 1
    assert result["candidates"][0]["scope_id"] == "individual:alpha"


def test_numeric_candidate_query_unknown_individual_is_unresolved(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    result = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=numeric_query_definition(),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "missing"},
    )

    assert result["run_status"] == "unresolved"
    assert result["candidate_count"] == 0
    assert result["execution_scope"]["resolved"]["type"] == "individual"
    assert result["scope_results"][0]["scope_id"] == "individual:missing"
    assert "Unknown individual for execution scope: missing" in result["warnings"]


def test_candidate_ids_are_stable_and_include_scope_identity(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    first = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=numeric_query_definition(),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "alpha"},
    )
    second = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=numeric_query_definition(),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "alpha"},
    )
    whole_study = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=numeric_query_definition(),
        dataset_id="dataset_test",
        logical_name="movement.csv",
    )

    assert first["candidates"][0]["candidate_id"] == second["candidates"][0]["candidate_id"]
    assert first["candidates"][0]["fix_key"] == whole_study["candidates"][0]["fix_key"]
    assert first["candidates"][0]["candidate_id"] != whole_study["candidates"][0]["candidate_id"]


def test_candidate_ids_include_query_parameters(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")
    query = parameterized_numeric_query_definition()

    low_threshold = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=query,
        parameters={"threshold": 30.0},
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "alpha"},
    )
    high_threshold = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=query,
        parameters={"threshold": 31.0},
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "alpha"},
    )

    assert low_threshold["candidates"][0]["fix_key"] == high_threshold["candidates"][0]["fix_key"]
    assert low_threshold["candidates"][0]["scope_id"] == high_threshold["candidates"][0]["scope_id"]
    assert low_threshold["query_digest"] == high_threshold["query_digest"]
    assert low_threshold["run_digest"] != high_threshold["run_digest"]
    assert low_threshold["candidates"][0]["candidate_id"] != high_threshold["candidates"][0]["candidate_id"]


def test_run_digest_includes_execution_scope(tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")
    query = numeric_query_definition()

    individual = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=query,
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "alpha"},
    )
    per_individual = run_fix_numeric_candidate_query(
        csv_path,
        query_definition=query,
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "all_individuals_per_individual"},
    )

    assert individual["query_digest"] == per_individual["query_digest"]
    assert individual["run_digest"] != per_individual["run_digest"]


def test_osm_proximity_individual_scope_returns_nearby_fix_candidates(monkeypatch, tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    def fake_fetch(query):
        return mocked_osm_feature_collection(
            [
                {
                    "type": "Feature",
                    "id": "way/10",
                    "geometry": {"type": "LineString", "coordinates": [[-70.0001, 39.999], [-70.0001, 40.001]]},
                    "properties": {"osm_type": "way", "osm_id": 10, "name": "Track Road", "tags": {"highway": "track"}},
                }
            ],
            query["scope"],
        )

    monkeypatch.setattr(candidate_queries, "fetch_osm_features", fake_fetch)

    result = run_candidate_query(
        csv_path,
        query_definition=osm_proximity_query_definition(distance_m=50),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "alpha"},
    )

    assert result["run_status"] == "success"
    assert result["candidate_count"] == 1
    assert result["candidates"][0]["fix_key"] == "id:fix_1#row:1"
    assert result["candidates"][0]["scope_id"] == "individual:alpha"
    evidence = result["candidates"][0]["evidence"]
    assert evidence["distance_m"] <= 50
    assert evidence["threshold_m"] == 50
    assert evidence["osm_feature_id"] == "way/10"
    assert evidence["osm_feature_type"] == "way"
    assert evidence["osm_feature_name"] == "Track Road"
    assert evidence["osm_tags"] == {"highway": "track"}
    assert evidence["threshold_display"] == "50 m"
    assert evidence["selectors"] == osm_proximity_query_definition()["definition"]["osm"]["selectors"]
    assert result["scope_results"][0]["osm"]["scope_signature"]
    assert result["scope_results"][0]["osm"]["subscope_count"] >= 1
    assert result["scope_results"][0]["osm"]["subscopes"][0]["scope_signature"] == "scope_sig"
    assert result["scope_results"][0]["osm"]["feature_count"] == 1


def test_osm_proximity_excludes_fixes_farther_than_distance(monkeypatch, tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    def fake_fetch(query):
        return mocked_osm_feature_collection(
            [
                {
                    "type": "Feature",
                    "id": "node/20",
                    "geometry": {"type": "Point", "coordinates": [-70.01, 40.01]},
                    "properties": {"osm_type": "node", "osm_id": 20, "tags": {"highway": "crossing"}},
                }
            ],
            query["scope"],
        )

    monkeypatch.setattr(candidate_queries, "fetch_osm_features", fake_fetch)

    result = run_candidate_query(
        csv_path,
        query_definition=osm_proximity_query_definition(distance_m=50),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "alpha"},
    )

    assert result["run_status"] == "success"
    assert result["candidate_count"] == 0
    assert result["candidates"] == []


def test_osm_proximity_all_individuals_continues_after_scope_failure(monkeypatch, tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    def fake_fetch(query):
        if query["scope"]["south"] > 40.5:
            raise candidate_queries.OSMValidationError("bbox area must be <= 2500 km^2")
        return mocked_osm_feature_collection(
            [
                {
                    "type": "Feature",
                    "id": "way/10",
                    "geometry": {"type": "LineString", "coordinates": [[-70.0001, 39.999], [-70.0001, 40.001]]},
                    "properties": {"osm_type": "way", "osm_id": 10, "tags": {"highway": "track"}},
                }
            ],
            query["scope"],
        )

    monkeypatch.setattr(candidate_queries, "fetch_osm_features", fake_fetch)

    result = run_candidate_query(
        csv_path,
        query_definition=osm_proximity_query_definition(distance_m=50),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "all_individuals_per_individual"},
    )

    assert result["run_status"] == "partial"
    assert [scope["scope_id"] for scope in result["scope_results"]] == ["individual:alpha", "individual:beta"]
    assert result["scope_results"][0]["run_status"] == "success"
    assert result["scope_results"][1]["run_status"] == "unresolved"
    assert "OSM scope could not run for individual:beta" in result["warnings"][0]
    failed_osm = result["scope_results"][1]["osm"]
    assert failed_osm["scope"]["type"] == "bbox"
    assert failed_osm["selectors"] == osm_proximity_query_definition()["definition"]["osm"]["selectors"]
    assert failed_osm["element_types"] == ["way"]
    assert failed_osm["distance_m"] == 50
    assert failed_osm["buffer_m"] == 50
    assert failed_osm["feature_count"] == 0
    assert failed_osm["scope_signature"]
    assert failed_osm["query_signature"]
    assert failed_osm["fetched_at"] is None
    assert "OSM subscope" in failed_osm["error"]
    assert result["candidate_count"] == 1
    assert result["candidates"][0]["fix_key"] == "id:fix_1#row:1"


def test_osm_proximity_continues_after_subscope_fetch_failure(monkeypatch, tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    def fake_fetch(query):
        if query["scope"]["east"] < -69:
            raise candidate_queries.OSMFetchError("Overpass request timed out")
        return mocked_osm_feature_collection(
            [
                {
                    "type": "Feature",
                    "id": "way/10",
                    "geometry": {"type": "LineString", "coordinates": [[-68.0001, 39.999], [-68.0001, 40.001]]},
                    "properties": {"osm_type": "way", "osm_id": 10, "tags": {"highway": "track"}},
                }
            ],
            query["scope"],
        )

    monkeypatch.setattr(candidate_queries, "fetch_osm_features", fake_fetch)

    result = run_candidate_query(
        csv_path,
        query_definition=osm_proximity_query_definition(distance_m=50),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "alpha"},
    )

    assert result["run_status"] == "partial"
    assert result["candidate_count"] == 1
    assert result["candidates"][0]["fix_key"] == "id:fix_2#row:2"
    assert result["scope_results"][0]["run_status"] == "partial"
    assert "OSM subscope 1/" in result["scope_results"][0]["warnings"][0]
    osm_metadata = result["scope_results"][0]["osm"]
    assert osm_metadata["feature_count"] == 1
    assert osm_metadata["subscope_count"] >= 2
    assert any(subscope.get("run_status") == "unresolved" for subscope in osm_metadata["subscopes"])
    assert any(subscope.get("run_status") == "success" for subscope in osm_metadata["subscopes"])


def test_osm_proximity_whole_study_requires_explicit_opt_in(monkeypatch, tmp_path):
    csv_path = tmp_path / "movement.csv"
    csv_path.write_text(FAST_MOVEMENT_CSV, encoding="utf-8")

    def fake_fetch(query):
        raise AssertionError("OSM should not be fetched without whole-study opt-in")

    monkeypatch.setattr(candidate_queries, "fetch_osm_features", fake_fetch)

    result = run_candidate_query(
        csv_path,
        query_definition=osm_proximity_query_definition(distance_m=50),
        dataset_id="dataset_test",
        logical_name="movement.csv",
        execution_scope={"type": "whole_study"},
    )

    assert result["run_status"] == "unresolved"
    assert result["candidate_count"] == 0
    assert result["scope_results"][0]["scope_id"] == "whole_study"
    assert result["warnings"][0].startswith("Whole-study OSM candidate queries require allow_whole_study_osm: true.")
    assert "Choose Current individual or All individuals separately" in result["warnings"][0]


def test_osm_proximity_evaluator_includes_osm_metadata(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    study_dir = data_root / "movement_clean" / "test_study"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(FAST_MOVEMENT_CSV, encoding="utf-8")
    dataset_id = "dataset_test"

    def fake_fetch(query):
        return mocked_osm_feature_collection(
            [
                {
                    "type": "Feature",
                    "id": "way/10",
                    "geometry": {"type": "LineString", "coordinates": [[-70.0001, 39.999], [-70.0001, 40.001]]},
                    "properties": {"osm_type": "way", "osm_id": 10, "tags": {"highway": "track"}},
                }
            ],
            query["scope"],
        )

    monkeypatch.setattr(candidate_queries, "fetch_osm_features", fake_fetch)

    result = run_candidate_query(
        study_dir / "movement.csv",
        query_definition=osm_proximity_query_definition(distance_m=50),
        dataset_id=dataset_id,
        logical_name="movement.csv",
        execution_scope={"type": "individual", "individual": "alpha"},
    )

    assert result["run_status"] == "success"
    assert result["scope_results"][0]["osm"]["query_signature"]
    assert result["scope_results"][0]["osm"]["subscopes"][0]["query_signature"] == "query_sig"
    assert result["candidates"][0]["fix_key"] == "id:fix_1#row:1"


def test_osm_proximity_route_creates_analysis_artifact_with_osm_metadata(monkeypatch, tmp_path):
    class FakeOverpassHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps(
                    {
                        "elements": [
                            {
                                "type": "way",
                                "id": 99,
                                "geometry": [
                                    {"lon": -70.0001, "lat": 40.999},
                                    {"lon": -70.0001, "lat": 41.001},
                                ],
                                "tags": {"highway": "track", "name": "Route Road"},
                            }
                        ]
                    }
                ).encode("utf-8")
            )

        def log_message(self, format, *args):
            return

    server = HTTPServer(("127.0.0.1", 0), FakeOverpassHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("VIBECLEANING_OVERPASS_URL", f"http://127.0.0.1:{server.server_port}/api/interpreter")
    try:
        client, study_dir, dataset_id = create_candidate_query_client(tmp_path)
        response = client.post(
            "/api/apps/movement/family/movement_clean/study/test_study/actions/run-candidate-query",
            json={
                "dataset_id": dataset_id,
                "logical_name": "movement.csv",
                "user": "reviewer",
                "query_definition": osm_proximity_query_definition(distance_m=50),
                "execution_scope": {"type": "individual", "individual": "beta"},
            },
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert response.status_code == 200
    payload = response.json()
    analysis = payload["analysis"]
    summary = payload["summary"]
    assert summary["run_status"] == "success"
    assert summary["candidate_count"] == 1
    assert summary["candidates"][0]["fix_key"] == "id:fix_4#row:4"
    assert summary["candidates"][0]["evidence"]["osm_feature_id"] == "way/99"
    assert summary["scope_results"][0]["osm"]["feature_count"] == 1
    assert summary["scope_results"][0]["osm"]["scope_signature"]
    assert summary["scope_results"][0]["osm"]["query_signature"]
    output_path = (
        project_paths(study_dir)["analyses"]
        / analysis["analysis_id"]
        / "outputs"
        / "candidate_query_results.json"
    )
    output_payload = json.loads(output_path.read_text())
    assert output_payload["scope_results"][0]["osm"]["feature_count"] == 1
    assert output_payload["candidates"][0]["fix_key"] == "id:fix_4#row:4"


def test_run_candidate_query_route_creates_analysis_artifact(tmp_path):
    client, study_dir, dataset_id = create_candidate_query_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-candidate-query",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
            "query_definition": numeric_query_definition(),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    analysis = payload["analysis"]
    summary = payload["summary"]
    assert summary["run_status"] == "success"
    assert summary["candidate_count"] == 1
    assert summary["run_digest"]
    assert summary["execution_scope"]["resolved"]["type"] == "whole_study"
    assert summary["scope_results"][0]["scope_id"] == "whole_study"
    assert summary["evaluator_provenance"]["implementation_version"] == "movement-candidate-query-v2"
    assert analysis["parameters"]["action"] == "run_candidate_query"
    assert analysis["parameters"]["execution_scope"] is None
    assert analysis["output_artifacts"] == ["candidate_query_results.json"]
    output_path = (
        project_paths(study_dir)["analyses"]
        / analysis["analysis_id"]
        / "outputs"
        / "candidate_query_results.json"
    )
    assert output_path.exists()
    output_payload = json.loads(output_path.read_text())
    assert output_payload["candidates"][0]["fix_key"] == "id:fix_2#row:2"
    assert output_payload["run_digest"] == summary["run_digest"]
    assert output_payload["execution_scope"]["resolved"]["type"] == "whole_study"
    assert output_payload["scope_results"][0]["scope_id"] == "whole_study"


def test_run_candidate_query_route_unknown_individual_creates_unresolved_analysis(tmp_path):
    client, study_dir, dataset_id = create_candidate_query_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-candidate-query",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
            "query_definition": numeric_query_definition(),
            "execution_scope": {"type": "individual", "individual": "missing"},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    analysis = payload["analysis"]
    summary = payload["summary"]
    assert summary["run_status"] == "unresolved"
    assert summary["candidate_count"] == 0
    assert summary["execution_scope"]["requested"] == {"type": "individual", "individual": "missing"}
    assert summary["scope_results"][0]["scope_id"] == "individual:missing"
    assert "Unknown individual for execution scope: missing" in summary["warnings"]
    output_path = (
        project_paths(study_dir)["analyses"]
        / analysis["analysis_id"]
        / "outputs"
        / "candidate_query_results.json"
    )
    assert json.loads(output_path.read_text())["run_status"] == "unresolved"


def test_run_candidate_query_route_malformed_scope_creates_unresolved_analysis(tmp_path):
    client, study_dir, dataset_id = create_candidate_query_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-candidate-query",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
            "query_definition": numeric_query_definition(),
            "execution_scope": {},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    analysis = payload["analysis"]
    summary = payload["summary"]
    assert summary["run_status"] == "unresolved"
    assert summary["candidate_count"] == 0
    assert summary["execution_scope"]["requested"] == {"type": ""}
    assert summary["execution_scope"]["resolved"]["type"] == "unresolved"
    assert "Execution scope type is missing." in summary["warnings"]
    output_path = (
        project_paths(study_dir)["analyses"]
        / analysis["analysis_id"]
        / "outputs"
        / "candidate_query_results.json"
    )
    assert json.loads(output_path.read_text())["execution_scope"]["resolved"]["type"] == "unresolved"


def test_run_candidate_query_unresolved_fields_still_create_analysis(tmp_path):
    client, study_dir, dataset_id = create_candidate_query_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-candidate-query",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "user": "reviewer",
            "query_definition": numeric_query_definition(field="missing_field"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    analysis = payload["analysis"]
    summary = payload["summary"]
    assert summary["run_status"] == "unresolved"
    assert summary["candidate_count"] == 0
    assert summary["unresolved_fields"] == ["missing_field"]
    output_path = (
        project_paths(study_dir)["analyses"]
        / analysis["analysis_id"]
        / "outputs"
        / "candidate_query_results.json"
    )
    assert output_path.exists()
    assert json.loads(output_path.read_text())["run_status"] == "unresolved"


def test_frontend_candidate_preview_reuses_selected_fix_flow():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")

    assert "candidateQueryPreview" in source
    assert "candidateQueryLibrary" in source
    assert 'fetchJSON("/api/query-library/queries?app=movement"' in source
    assert 'data-role="candidate-query-select"' in source
    assert 'data-role="candidate-query-scope"' in source
    assert "selectCandidateQueryExecutionScope(value)" in source
    assert "getCandidateQueryExecutionScope()" in source
    assert "defaultCandidateQueryExecutionScope(query)" in source
    assert 'query?.evaluator?.type === "fix_osm_proximity" ? "current_individual" : "whole_study"' in source
    assert "OSM scope: select one individual, or choose all individuals separately" in source
    assert "return selectedIndividuals.length === 1 ? selectedIndividuals[0] : \"\";" in source
    assert "detailIndividuals.length === 1" not in source
    assert "selectCandidateQuery(key)" in source
    assert "candidateQueryParameterDescriptors(query)" in source
    assert 'data-param-name="${escapeHtml(descriptor.name)}"' in source
    assert "getCandidateQueryParameterValues(selectedQuery)" in source
    assert "runSelectedCandidateQuery" in source
    assert "query_id: selectedQuery.query_id" in source
    assert "query_version: selectedQuery.version" in source
    assert "query_parameters:" in source
    assert "execution_scope:" in source
    assert "run-candidate-query" in source
    assert "movement-candidate-query-points" in source
    assert "movement-selected-candidate-query-points" in source
    assert "previewMatchKeys" not in source
    assert "Preview speed" not in source
    assert "Speed greater than 120" not in source
    assert "checkCandidateQueryPreview" in source
    assert "const fixKey = String(candidate?.fix_key || \"\")" in source
    assert "this.data.selectedFixKeys = nextSelected" in source
    assert 'this.refs.markSuspected.addEventListener("click", () => this.openIssueModal("suspected"))' in source
    assert "apply-candidate-query" not in source
