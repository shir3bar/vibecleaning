from pathlib import Path
import json
import sys

from fastapi.testclient import TestClient
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.state import load_project_state, project_paths
from app.web import create_app
from examples.movement.burst_feature_space import build_burst_feature_space
from examples.movement.dev_routes import (
    BURST_FEATURE_SPACE_ANALYSIS_SCRIPT,
    BURST_FEATURE_SPACE_ANALYSIS_TEMPLATE_PATH,
)
from examples.movement.routes import (
    register_movement_routes,
)

MOVEMENT_APP_JS = REPO_ROOT / "examples" / "movement" / "static" / "app.js"


FEATURE_SPACE_CSV = """eventid,individual,timestamp,longitude,latitude,set,gps:hdop,height-above-msl,osm:nearest_road_distance_m,osm:nearest_road_class,osm:road_match_status,osm:nearest_railway_distance_m,osm:nearest_railway_class,osm:railway_match_status
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0000,40.0,train,1,100,5,service,matched,100,rail,matched
fix_a_2,alpha,2024-01-01T00:00:10Z,-69.9999,40.0,train,2,101,6,service,matched,110,rail,matched
fix_a_3,alpha,2024-01-01T01:00:00Z,-70.0000,40.0,train,1,100,50,primary,matched,80,rail,matched
fix_a_4,alpha,2024-01-01T01:00:10Z,-69.9998,40.0,train,2,102,60,primary,matched,90,rail,matched
fix_b_1,beta,2024-01-01T00:00:00Z,-71.0000,41.0,test,1,110,500,,not_found_within_radius,5,rail,matched
fix_b_2,beta,2024-01-01T00:00:10Z,-70.9999,41.0,test,1,110,510,,not_found_within_radius,6,rail,matched
fix_b_3,beta,2024-01-01T01:00:00Z,-71.0000,41.0,test,1,110,900,,not_found_within_radius,20,rail,matched
fix_b_4,beta,2024-01-01T01:00:10Z,-70.9000,41.0,test,4,150,950,,not_found_within_radius,25,rail,matched
"""


def _feature_row(index: int, **features) -> dict:
    return {
        "burst_id": f"alpha:train:burst_{index:06d}",
        "individual": "alpha",
        "set_name": "train",
        "start_time_ms": index * 10_000,
        "end_time_ms": index * 10_000 + 5_000,
        "n_fixes": 2 + index,
        "fix_keys": [f"fix_{index}_0", f"fix_{index}_1"],
        **features,
    }


def _standardized_distance(left: dict, right: dict, rows_by_id: dict, metadata: dict) -> float:
    total = 0.0
    for feature in metadata["fitted_features"]:
        median = metadata["feature_medians"][feature]
        center = metadata["feature_means_or_centers"][feature]
        scale = metadata["feature_scales"][feature]
        left_value = rows_by_id[left["burst_id"]].get(feature)
        right_value = rows_by_id[right["burst_id"]].get(feature)
        left_numeric = median if left_value is None else float(left_value)
        right_numeric = median if right_value is None else float(right_value)
        left_standardized = (left_numeric - center) / scale
        right_standardized = (right_numeric - center) / scale
        total += (left_standardized - right_standardized) ** 2
    return total ** 0.5


def _create_feature_space_client(tmp_path: Path) -> tuple[TestClient, Path, str]:
    data_root = tmp_path / "data"
    study_dir = data_root / "movement_clean" / "test_study"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(FEATURE_SPACE_CSV, encoding="utf-8")
    app = create_app(
        data_root=data_root,
        static_root=REPO_ROOT / "examples" / "movement" / "static",
    )
    register_movement_routes(app, data_root=data_root)
    dataset_id = load_project_state(study_dir)["current_dataset_id"]
    return TestClient(app), study_dir, dataset_id


def test_burst_feature_space_uses_standardized_correlation_pca_and_stable_signs():
    rows = [
        _feature_row(0, feature_small=0.0, feature_large=0.0),
        _feature_row(1, feature_small=1.0, feature_large=100.0),
        _feature_row(2, feature_small=2.0, feature_large=200.0),
        _feature_row(3, feature_small=3.0, feature_large=300.0),
    ]

    first = build_burst_feature_space(rows, neighbor_count=2)
    second = build_burst_feature_space(rows, neighbor_count=2)

    assert first == second
    assert first["run_status"] == "completed"
    assert first["analysis_type"] == "burst_feature_space"
    assert first["projection_method"] == "pca"
    assert first["feature_matrix"]["standardization"] == "median_imputed_standardized_features"
    assert first["feature_matrix"]["pca_type"] == "correlation_pca"
    assert "n_fixes" in first["feature_matrix"]["requested_features"]
    assert "n_fixes" in first["feature_matrix"]["candidate_model_features"]
    assert "n_fixes" in first["feature_matrix"]["fitted_features"]
    assert first["feature_matrix"]["feature_medians"]["n_fixes"] == 3.5
    assert first["feature_matrix"]["feature_scales"]["n_fixes"] == pytest.approx(1.118033988749895)
    assert first["feature_matrix"]["feature_scales"]["feature_large"] == pytest.approx(111.80339887498948)
    assert first["feature_matrix"]["feature_scales"]["feature_small"] == pytest.approx(1.118033988749895)
    assert first["pca"]["n_components_fitted"] == 2
    assert first["pca"]["sign_convention"] == "largest_absolute_loading_positive_per_component"

    pc1_loadings = first["pca"]["components"][0]["loadings"]
    assert abs(pc1_loadings["feature_large"]) == pytest.approx(
        abs(pc1_loadings["feature_small"]),
        rel=1e-6,
    )
    assert abs(pc1_loadings["n_fixes"]) == pytest.approx(
        abs(pc1_loadings["feature_small"]),
        rel=1e-6,
    )
    for component in first["pca"]["components"]:
        loadings = list(component["loadings"].values())
        anchor = max(loadings, key=lambda value: abs(value))
        assert anchor >= 0.0


def test_burst_feature_space_imputes_missing_values_and_uses_standardized_neighbors():
    rows = [
        _feature_row(0, feature_a=0.0, feature_b=0.0, sparse_feature=1.0),
        _feature_row(1, feature_a=1.0, feature_b=0.0, sparse_feature=None),
        _feature_row(2, feature_a=0.0, feature_b=3.0, sparse_feature=5.0),
        _feature_row(3, feature_a=4.0, feature_b=3.0, sparse_feature=9.0),
    ]

    result = build_burst_feature_space(rows, neighbor_count=2)

    assert result["run_status"] == "completed"
    metadata = result["feature_matrix"]
    assert "n_fixes" in metadata["fitted_features"]
    assert metadata["feature_medians"]["n_fixes"] == 3.5
    assert metadata["feature_scales"]["n_fixes"] == pytest.approx(1.118033988749895)
    assert metadata["feature_medians"]["sparse_feature"] == 5.0
    assert metadata["imputed_value_counts"]["sparse_feature"] == 1
    rows_by_id = {row["burst_id"]: row for row in rows}
    points_by_id = {point["burst_id"]: point for point in result["points"]}
    for point in result["points"]:
        expected = [
            {
                "burst_id": candidate["burst_id"],
                "distance": _standardized_distance(point, candidate, rows_by_id, metadata),
            }
            for candidate in result["points"]
            if candidate["burst_id"] != point["burst_id"]
        ]
        expected.sort(key=lambda item: (item["distance"], item["burst_id"]))
        observed = points_by_id[point["burst_id"]]["nearest_neighbors"]
        assert [item["burst_id"] for item in observed] == [
            item["burst_id"] for item in expected[:2]
        ]
        assert [item["distance"] for item in observed] == pytest.approx(
            [item["distance"] for item in expected[:2]]
        )
        assert result["nearest_neighbors"][point["burst_id"]] == observed


def test_burst_feature_space_feature_set_controls_osm_features():
    rows = [
        _feature_row(0, path_length_m=10.0, **{"osm:nearest_road_distance_m__mean": 5.0}),
        _feature_row(1, path_length_m=20.0, **{"osm:nearest_road_distance_m__mean": 10.0}),
        _feature_row(2, path_length_m=30.0, **{"osm:nearest_road_distance_m__mean": 15.0}),
    ]

    movement_only = build_burst_feature_space(rows)
    movement_plus_context = build_burst_feature_space(
        rows,
        feature_set="movement_plus_context",
    )

    assert movement_only["feature_set"] == "movement_only"
    assert "osm:nearest_road_distance_m__mean" in movement_only["feature_matrix"]["requested_features"]
    assert "osm:nearest_road_distance_m__mean" not in movement_only["feature_matrix"]["fitted_features"]
    assert "n_fixes" in movement_only["feature_matrix"]["fitted_features"]
    assert movement_only["feature_matrix"]["excluded_by_feature_set"] == {
        "osm:nearest_road_distance_m__mean": "context_feature_excluded_from_movement_only",
    }
    assert movement_plus_context["feature_set"] == "movement_plus_context"
    assert movement_plus_context["feature_matrix"]["excluded_by_feature_set"] == {}
    assert "osm:nearest_road_distance_m__mean" in movement_plus_context["feature_matrix"]["fitted_features"]
    assert "n_fixes" in movement_plus_context["feature_matrix"]["fitted_features"]


def test_burst_feature_space_supports_one_component_projection():
    rows = [
        _feature_row(0, n_fixes=2, only_feature=1.0, constant_feature=8.0),
        _feature_row(1, n_fixes=2, only_feature=2.0, constant_feature=8.0),
        _feature_row(2, n_fixes=2, only_feature=3.0, constant_feature=8.0),
    ]

    result = build_burst_feature_space(rows)

    assert result["run_status"] == "completed"
    assert result["pca"]["n_components_fitted"] == 1
    assert all(point["pc2"] == 0.0 for point in result["points"])
    assert result["feature_matrix"]["dropped_features"] == {
        "constant_feature": "constant",
        "n_fixes": "constant",
    }


def test_burst_feature_space_returns_unresolved_for_too_few_rows_or_no_features():
    too_few = build_burst_feature_space([_feature_row(0, feature=1.0)])
    no_features = build_burst_feature_space(
        [
            _feature_row(0, n_fixes=2, constant_feature=8.0),
            _feature_row(1, n_fixes=2, constant_feature=8.0),
        ]
    )

    assert too_few["run_status"] == "unresolved"
    assert too_few["points"] == []
    assert any("At least 2 burst rows" in warning for warning in too_few["warnings"])
    assert no_features["run_status"] == "unresolved"
    assert no_features["feature_matrix"]["fitted_features"] == []
    assert any("No usable non-constant" in warning for warning in no_features["warnings"])


def test_burst_feature_space_template_matches_route_constant():
    template_text = BURST_FEATURE_SPACE_ANALYSIS_TEMPLATE_PATH.read_text(
        encoding="utf-8"
    ).strip() + "\n"

    assert BURST_FEATURE_SPACE_ANALYSIS_SCRIPT.endswith(template_text)
    assert "_VIBECLEANING_BUNDLED_SOURCES" in BURST_FEATURE_SPACE_ANALYSIS_SCRIPT
    assert "repo_root" not in BURST_FEATURE_SPACE_ANALYSIS_SCRIPT
    compile(
        BURST_FEATURE_SPACE_ANALYSIS_SCRIPT,
        str(BURST_FEATURE_SPACE_ANALYSIS_TEMPLATE_PATH),
        "exec",
    )


def test_burst_feature_space_route_creates_analysis_artifact_without_dataset_mutation(tmp_path):
    client, study_dir, dataset_id = _create_feature_space_client(tmp_path)
    initial_state = load_project_state(study_dir)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-burst-feature-space",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": 60,
            "burst_gap_quantile": 0.75,
            "feature_set": "movement_plus_context",
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    analysis = payload["analysis"]
    summary = payload["summary"]
    assert analysis["dataset_id"] == dataset_id
    assert analysis["parameters"]["action"] == "run_burst_feature_space"
    assert analysis["parameters"]["feature_set"] == "movement_plus_context"
    assert analysis["output_artifacts"] == ["burst_feature_space.json"]
    assert {artifact["logical_name"] for artifact in analysis["realized_output_artifacts"]} == {
        "burst_feature_space.json"
    }
    assert summary["run_status"] == "completed"
    assert summary["analysis_type"] == "burst_feature_space"
    assert summary["projection_method"] == "pca"
    assert summary["feature_set"] == "movement_plus_context"
    assert summary["input_artifact"]["dataset_id"] == dataset_id
    assert summary["input_artifact"]["logical_name"] == "movement.csv"
    assert summary["burst_gap"]["mode"] == "manual"
    assert summary["feature_matrix"]["standardization"] == "median_imputed_standardized_features"
    assert "points" not in summary
    assert "nearest_neighbors" not in summary

    output_path = (
        project_paths(study_dir)["analyses"]
        / analysis["analysis_id"]
        / "outputs"
        / "burst_feature_space.json"
    )
    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert output["run_status"] == "completed"
    assert len(output["points"]) == output["projected_burst_count"] == 4
    assert output["nearest_neighbors"]
    assert all(point["burst_id"] and point["fix_keys"] for point in output["points"])
    assert load_project_state(study_dir)["current_dataset_id"] == initial_state["current_dataset_id"]


def test_burst_feature_space_route_rejects_unknown_feature_set(tmp_path):
    client, _, dataset_id = _create_feature_space_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-burst-feature-space",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "feature_set": "context_only",
            "user": "reviewer",
        },
    )

    assert response.status_code == 400
    assert response.json()["error"] == "Invalid feature_set"


def test_frontend_exposes_read_only_burst_feature_space_with_bidirectional_focus():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    runner = source[
        source.index("  async runBurstFeatureSpace()"):
        source.index("  getBurstFeatureSpacePoint(")
    ]
    renderer = source[
        source.index("  getBurstFeatureSpaceNeighbors("):
        source.index("  normalizeRankingBurstRefs(")
    ]
    feature_space_handler = source[
        source.index("  async inspectBurstFeatureSpacePoint("):
        source.index("  normalizeRankingBurstRefs(")
    ]
    shared_focus_handler = source[
        source.index("  async inspectBurstRef("):
        source.index("  async handleAnomalyRankingClick(")
    ]

    assert 'data-role="run-burst-feature-space">Feature space</button>' in source
    assert 'data-role="side-tab-feature-space">Burst feature space</button>' in source
    assert 'data-role="side-sheet-feature-space"' in source
    assert 'data-role="burst-feature-space"' in source
    assert 'this.refs.sideTabFeatureSpace.addEventListener("click", () => this.setSideSheet("feature_space"))' in source
    assert "run-burst-feature-space" in runner
    assert "dataset_id: this.currentDatasetId" in runner
    assert "logical_name: this.currentArtifact" in runner
    assert "burst_gap_mode: this.getBurstGapMode()" in runner
    assert "burst_gap_seconds: this.getBurstGapSeconds()" in runner
    assert "burst_gap_quantile: this.getBurstGapQuantile()" in runner
    assert "feature_set: featureSet" in runner
    assert "/artifact/burst_feature_space.json" in runner
    assert 'this.setSideSheet("feature_space")' in runner
    assert '<svg viewBox="0 0 ${width} ${height}"' in renderer
    assert 'data-action="focus-feature-space-burst"' in renderer
    assert "point?.pc1" in renderer
    assert "point?.pc2" in renderer
    assert "movement-feature-space-point is-neighbor" not in source
    assert '"is-neighbor"' in renderer
    assert '"is-selected"' in renderer
    assert 'data-role="feature-space-selection"' in renderer
    assert 'data-role="feature-space-neighbors"' in renderer
    assert 'data-action="focus-feature-space-neighbor"' in renderer
    assert "nearest_neighbors" in renderer
    assert "await this.inspectBurstRef(point, {" in feature_space_handler
    assert "isolateIndividual: true" in feature_space_handler
    assert "inspectBurstFeatureSpaceNeighbor" in feature_space_handler
    assert "const point = this.getBurstFeatureSpacePoint(burstId)" in feature_space_handler
    assert "preserveFeatureSpaceSelection: true" in feature_space_handler
    assert "const fixKeys = Array.isArray(ref.fix_keys) ? ref.fix_keys : []" in shared_focus_handler
    assert "isolateIndividual = false" in shared_focus_handler
    assert "preserveFeatureSpaceSelection = false" in shared_focus_handler
    assert "const preservedFeatureSpaceBurstId = preserveFeatureSpaceSelection" in shared_focus_handler
    assert "this.burstFeatureSpace.selectedBurstId = preservedFeatureSpaceBurstId" in shared_focus_handler
    assert "this.data.selectedIndividuals = new Set([ref.individual])" in shared_focus_handler
    assert "this.setFocusedRankingBurst(ref)" in shared_focus_handler
    assert "this.zoomToPath(path)" in shared_focus_handler
    assert "this.burstFeatureSpace.selectedBurstId = this.focusedRankingBurst.burstId" in source
    assert "this.renderBurstFeatureSpace()" in source
    assert "getMapPickedFeatureSpaceBurst(event)" in source
    assert "this.overlay.pickMultipleObjects" in source
    assert 'this.refs?.sideSheetTabs?.dataset.activeSheet !== "feature_space"' in source
    assert "selectMapBurstInFeatureSpace(burst)" in source
    assert "this.setFocusedRankingBurst({" in source
    assert "fix_keys: burst.fixKeys" in source
    # Map-to-feature-space selection no longer depends on layer pickability;
    # bursts are always pickable and the handler keeps its own guards, running
    # ahead of both the burst-focus and fix-selection branches.
    assert "pickable: Boolean(this.burstFeatureSpace?.points?.length)" not in source
    assert "!(this.burstFeatureSpace?.points || []).length" in source
    click_handler = source[
        source.index("  handleMapClick(event) {"):
        source.index("  handleMapContextMenu(event) {")
    ]
    assert click_handler.index("getMapPickedFeatureSpaceBurst") < click_handler.index("pickObject")
    assert click_handler.index("selectMapBurstInFeatureSpace") < click_handler.index("getMapPickedObject")
    assert "focusMapBurst" not in click_handler
    assert "openIssueModal" not in feature_space_handler
    assert "openSegmentModal" not in feature_space_handler
    assert "run-candidate-query" not in feature_space_handler
    assert "enrich-osm-context" not in feature_space_handler
    assert "run-burst-anomaly-ranking" not in feature_space_handler
