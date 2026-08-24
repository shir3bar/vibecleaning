from pathlib import Path
import subprocess
import sys
import json

from fastapi.testclient import TestClient
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.state import load_project_state, project_paths
from app.web import create_app
from examples.movement.burst_feature_matrix import prepare_burst_feature_matrix
from examples.movement.anomaly_ranking import rank_individuals, score_bursts
from examples.movement.burst_features import build_burst_feature_rows
from examples.movement.routes import (
    ANOMALY_ANALYSIS_TEMPLATE_PATH,
    BURST_ANOMALY_ANALYSIS_SCRIPT,
    RDS_ANOMALY_ANALYSIS_TEMPLATE_PATH,
    register_movement_routes,
)
from examples.movement.script_bundle import build_self_contained_script

MOVEMENT_APP_JS = REPO_ROOT / "examples" / "movement" / "static" / "app.js"


def test_combined_isolation_forest_templates_score_bursts_once():
    for template_path in (
        ANOMALY_ANALYSIS_TEMPLATE_PATH,
        RDS_ANOMALY_ANALYSIS_TEMPLATE_PATH,
    ):
        source = template_path.read_text(encoding="utf-8")
        assert source.count("scoring = score_bursts(") == 1
        assert 'config={"aggregation": "maximum_anomaly_score"}' in source
        assert 'config={"aggregation": "sum_outlier_margin"}' in source


def test_bundled_analysis_delegates_compiled_scipy_imports(tmp_path):
    entry_path = tmp_path / "compiled_import_check.py"
    entry_path.write_text(
        "from examples.movement.anomaly_ranking import rank_individuals\n"
        "from sklearn.ensemble import IsolationForest\n"
        "from scipy.sparse.csgraph import minimum_spanning_tree\n"
        "assert rank_individuals and IsolationForest and minimum_spanning_tree\n",
        encoding="utf-8",
    )
    script = build_self_contained_script(
        entry_path,
        (
            "examples.movement.burst_feature_matrix",
            "examples.movement.anomaly_ranking",
        ),
    )
    script_path = tmp_path / "analysis.py"
    script_path.write_text(script, encoding="utf-8")
    completed = subprocess.run(
        [sys.executable, str(script_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


ANOMALY_ANALYSIS_CSV = """eventid,individual,timestamp,longitude,latitude,set,gps:hdop,height-above-msl
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0000,40.0,train,1,100
fix_a_2,alpha,2024-01-01T00:00:10Z,-69.9999,40.0,train,2,101
fix_a_3,alpha,2024-01-01T01:00:00Z,-70.0000,40.0,train,1,100
fix_a_4,alpha,2024-01-01T01:00:10Z,-69.9998,40.0,train,2,102
fix_b_1,beta,2024-01-01T00:00:00Z,-71.0000,41.0,test,1,110
fix_b_2,beta,2024-01-01T00:00:10Z,-70.9999,41.0,test,1,110
fix_b_3,beta,2024-01-01T01:00:00Z,-71.0000,41.0,test,1,110
fix_b_4,beta,2024-01-01T01:00:10Z,-70.9000,41.0,test,4,150
"""

ANOMALY_ANALYSIS_OSM_CSV = """eventid,individual,timestamp,longitude,latitude,set,gps:hdop,height-above-msl,osm:nearest_road_distance_m,osm:nearest_road_class,osm:road_match_status,osm:nearest_railway_distance_m,osm:nearest_railway_class,osm:railway_match_status
fix_a_1,alpha,2024-01-01T00:00:00Z,-70.0000,40.0,train,1,100,5,service,matched,100,rail,matched
fix_a_2,alpha,2024-01-01T00:00:10Z,-69.9999,40.0,train,2,101,6,service,matched,110,rail,matched
fix_a_3,alpha,2024-01-01T01:00:00Z,-70.0000,40.0,train,1,100,50,primary,matched,80,rail,matched
fix_a_4,alpha,2024-01-01T01:00:10Z,-69.9998,40.0,train,2,102,60,primary,matched,90,rail,matched
fix_b_1,beta,2024-01-01T00:00:00Z,-71.0000,41.0,test,1,110,500,,not_found_within_radius,5,rail,matched
fix_b_2,beta,2024-01-01T00:00:10Z,-70.9999,41.0,test,1,110,510,,not_found_within_radius,6,rail,matched
fix_b_3,beta,2024-01-01T01:00:00Z,-71.0000,41.0,test,1,110,900,,not_found_within_radius,20,rail,matched
fix_b_4,beta,2024-01-01T01:00:10Z,-70.9000,41.0,test,4,150,950,,not_found_within_radius,25,rail,matched
"""


def _feature_row(index: int, path_length_m: float, mean_speed_mps: float) -> dict:
    return {
        "burst_id": f"alpha:train:burst_{index:06d}",
        "individual": "alpha",
        "set_name": "train",
        "start_time_ms": index * 10_000,
        "end_time_ms": index * 10_000 + 5_000,
        "n_fixes": 2 + index,
        "fix_keys": [f"fix_{index}_0", f"fix_{index}_1"],
        "path_length_m": path_length_m,
        "mean_speed_mps": mean_speed_mps,
    }


def _outlier_feature_rows() -> list[dict]:
    values = [
        (10.0, 1.00),
        (10.1, 1.01),
        (9.9, 0.99),
        (10.2, 1.02),
        (9.8, 0.98),
        (10.05, 1.005),
        (9.95, 0.995),
        (1000.0, 100.0),
    ]
    return [_feature_row(index, *value) for index, value in enumerate(values)]


def _create_anomaly_analysis_client(tmp_path: Path) -> tuple[TestClient, Path, str]:
    data_root = tmp_path / "data"
    study_dir = data_root / "movement_clean" / "test_study"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(ANOMALY_ANALYSIS_CSV, encoding="utf-8")
    app = create_app(
        data_root=data_root,
        static_root=REPO_ROOT / "examples" / "movement" / "static",
    )
    register_movement_routes(app, data_root=data_root)
    dataset_id = load_project_state(study_dir)["current_dataset_id"]
    return TestClient(app), study_dir, dataset_id


def _create_context_anomaly_analysis_client(tmp_path: Path) -> tuple[TestClient, Path, str]:
    data_root = tmp_path / "data"
    study_dir = data_root / "movement_clean" / "test_study"
    study_dir.mkdir(parents=True)
    (study_dir / "movement_osm_context.csv").write_text(ANOMALY_ANALYSIS_OSM_CSV, encoding="utf-8")
    app = create_app(
        data_root=data_root,
        static_root=REPO_ROOT / "examples" / "movement" / "static",
    )
    register_movement_routes(app, data_root=data_root)
    dataset_id = load_project_state(study_dir)["current_dataset_id"]
    return TestClient(app), study_dir, dataset_id


def test_score_bursts_adds_scores_only_to_burst_rows_and_prioritizes_outlier():
    input_rows = _outlier_feature_rows()

    result = score_bursts(input_rows)

    assert result["run_status"] == "completed"
    assert result["model"] == "IsolationForest"
    assert result["model_config"] == {
        "n_estimators": 1000,
        "contamination": "auto",
        "random_state": 0,
    }
    assert result["feature_set"] == "movement_only"
    assert result["preprocessing"]["scaling"] == "none"
    assert result["preprocessing"]["missing_value_strategy"] == (
        "median_imputation_per_fitted_feature"
    )
    assert result["scored_burst_count"] == len(input_rows)
    assert "fixes" not in result
    assert "anomaly_score" not in input_rows[-1]
    scored_outlier = result["scored_bursts"][-1]
    assert scored_outlier["burst_id"] == input_rows[-1]["burst_id"]
    assert scored_outlier["fix_keys"] == input_rows[-1]["fix_keys"]
    assert scored_outlier["path_length_m"] == input_rows[-1]["path_length_m"]
    assert scored_outlier["anomaly_score"] == max(
        row["anomaly_score"] for row in result["scored_bursts"]
    )
    assert scored_outlier["anomaly_score"] > result["scored_bursts"][0]["anomaly_score"]
    assert result["decision_boundary"] == 0.0
    assert isinstance(result["score_offset"], float)
    assert all(
        row["decision_function"]
        == pytest.approx(-row["anomaly_score"] - result["score_offset"])
        for row in result["scored_bursts"]
    )
    assert all(
        row["outlier_margin"] == pytest.approx(max(0.0, -row["decision_function"]))
        and row["is_model_outlier"] is (row["decision_function"] < 0.0)
        for row in result["scored_bursts"]
    )
    assert all(
        not any("anomaly" in str(fix_key).lower() for fix_key in row["fix_keys"])
        for row in result["scored_bursts"]
    )


def test_score_bursts_is_deterministic_with_fixed_random_state():
    rows = _outlier_feature_rows()
    config = {"n_estimators": 64, "random_state": 17}

    first = score_bursts(rows, config=config)
    second = score_bursts(rows, config=config)

    assert [row["anomaly_score"] for row in first["scored_bursts"]] == [
        row["anomaly_score"] for row in second["scored_bursts"]
    ]


def test_prepare_burst_feature_matrix_filters_imputes_and_standardizes_deterministically():
    rows = [
        {
            **_feature_row(0, 1.0, 10.0),
            "constant_feature": 8.0,
            "all_null_feature": None,
            "mixed_feature": 1.0,
            "missing_feature": 1.0,
            "osm:nearest_road_distance_m__mean": 5.0,
        },
        {
            **_feature_row(1, 2.0, 20.0),
            "constant_feature": 8.0,
            "all_null_feature": None,
            "mixed_feature": "not numeric",
            "missing_feature": None,
            "osm:nearest_road_distance_m__mean": 10.0,
        },
        {
            **_feature_row(2, 3.0, 30.0),
            "constant_feature": 8.0,
            "all_null_feature": None,
            "mixed_feature": 3.0,
            "missing_feature": 5.0,
            "osm:nearest_road_distance_m__mean": 15.0,
        },
    ]

    prepared = prepare_burst_feature_matrix(rows)

    assert prepared["excluded_metadata"] == [
        "burst_id",
        "end_time_ms",
        "fix_keys",
        "individual",
        "n_fixes",
        "set_name",
        "start_time_ms",
    ]
    assert prepared["candidate_model_features"] == [
        "all_null_feature",
        "constant_feature",
        "mean_speed_mps",
        "missing_feature",
        "mixed_feature",
        "path_length_m",
    ]
    assert prepared["excluded_by_feature_set"] == {
        "osm:nearest_road_distance_m__mean": "context_feature_excluded_from_movement_only",
    }
    assert prepared["dropped_features"] == {
        "all_null_feature": "all_null",
        "constant_feature": "constant",
        "mixed_feature": "nonnumeric",
    }
    assert prepared["fitted_features"] == [
        "mean_speed_mps",
        "missing_feature",
        "path_length_m",
    ]
    assert prepared["feature_medians"] == {
        "mean_speed_mps": 20.0,
        "missing_feature": 3.0,
        "path_length_m": 2.0,
    }
    assert prepared["imputed_value_counts"] == {
        "mean_speed_mps": 0,
        "missing_feature": 1,
        "path_length_m": 0,
    }
    assert prepared["matrix"] == [
        [10.0, 1.0, 1.0],
        [20.0, 3.0, 2.0],
        [30.0, 5.0, 3.0],
    ]
    assert prepared["scaling"] == "none"
    assert prepared["imputed_matrix"] == prepared["matrix"]
    assert prepared["feature_means"] == {}
    assert prepared["feature_scales"] == {}

    standardized = prepare_burst_feature_matrix(rows, standardize=True)
    assert standardized["scaling"] == "standardize"
    assert standardized["imputed_matrix"] == prepared["matrix"]
    assert standardized["feature_means"] == {
        "mean_speed_mps": 20.0,
        "missing_feature": 3.0,
        "path_length_m": 2.0,
    }
    assert standardized["feature_scales"] == pytest.approx(
        {
            "mean_speed_mps": 8.16496580927726,
            "missing_feature": 1.632993161855452,
            "path_length_m": 0.816496580927726,
        }
    )
    for actual, expected in zip(
        standardized["matrix"],
        [
            [-1.224744871391589, -1.224744871391589, -1.224744871391589],
            [0.0, 0.0, 0.0],
            [1.224744871391589, 1.224744871391589, 1.224744871391589],
        ],
    ):
        assert actual == pytest.approx(expected)

    with_context = prepare_burst_feature_matrix(
        rows,
        feature_set="movement_plus_context",
    )
    assert with_context["excluded_by_feature_set"] == {}
    assert "osm:nearest_road_distance_m__mean" in with_context["candidate_model_features"]
    assert "osm:nearest_road_distance_m__mean" in with_context["fitted_features"]


def test_score_bursts_excludes_metadata_and_records_dropped_and_imputed_features():
    rows = [
        {
            **_feature_row(0, 1.0, 1.0),
            "constant_feature": 8.0,
            "all_null_feature": None,
            "mixed_feature": 1.0,
            "missing_feature": 1.0,
        },
        {
            **_feature_row(1, 2.0, 2.0),
            "constant_feature": 8.0,
            "all_null_feature": None,
            "mixed_feature": "not numeric",
            "missing_feature": None,
        },
        {
            **_feature_row(2, 3.0, 3.0),
            "constant_feature": 8.0,
            "all_null_feature": None,
            "mixed_feature": 3.0,
            "missing_feature": 5.0,
        },
    ]

    result = score_bursts(rows, config={"n_estimators": 32})

    assert result["run_status"] == "completed"
    assert "start_time_ms" in result["excluded_metadata"]
    assert "end_time_ms" in result["excluded_metadata"]
    assert "n_fixes" in result["excluded_metadata"]
    assert "fix_keys" in result["excluded_metadata"]
    assert "start_time_ms" not in result["requested_features"]
    assert result["candidate_model_features"] == [
        "all_null_feature",
        "constant_feature",
        "mean_speed_mps",
        "missing_feature",
        "mixed_feature",
        "path_length_m",
    ]
    assert result["excluded_by_feature_set"] == {}
    assert "start_time_ms" not in result["fitted_features"]
    assert result["dropped_features"] == {
        "all_null_feature": "all_null",
        "constant_feature": "constant",
        "mixed_feature": "nonnumeric",
    }
    assert result["fitted_features"] == [
        "mean_speed_mps",
        "missing_feature",
        "path_length_m",
    ]
    assert result["feature_medians"]["missing_feature"] == 3.0
    assert result["imputed_value_counts"]["missing_feature"] == 1
    assert result["scored_bursts"][1]["missing_feature"] is None


def test_score_bursts_adds_observed_quantile_explanations_without_imputed_display_values():
    rows = [
        {
            **_feature_row(0, 10.0, 1.0),
            "observed_feature": 1.0,
            "missing_feature": 1.0,
        },
        {
            **_feature_row(1, 20.0, 2.0),
            "observed_feature": 2.0,
            "missing_feature": None,
        },
        {
            **_feature_row(2, 30.0, 3.0),
            "observed_feature": 3.0,
            "missing_feature": 5.0,
        },
        {
            **_feature_row(3, 40.0, 4.0),
            "observed_feature": 4.0,
            "missing_feature": 9.0,
        },
    ]

    result = score_bursts(rows, config={"n_estimators": 32, "random_state": 5})

    assert result["run_status"] == "completed"
    assert result["explanation_method"] == "observed_empirical_quantile_among_scored_bursts"
    assert result["explanation_value_source"] == "observed_burst_feature_values"
    assert result["missing_value_display"] == "NA"
    assert "Median imputation is used only" in result["model_imputation_note"]
    assert "explanation_top_n" not in result
    assert result["feature_medians"]["missing_feature"] == 5.0
    assert result["imputed_value_counts"]["missing_feature"] == 1

    scored_missing = result["scored_bursts"][1]
    assert "anomaly_score" in scored_missing
    assert set(item["feature"] for item in scored_missing["feature_quantiles"]) == set(
        result["fitted_features"]
    )
    missing_item = next(
        item
        for item in scored_missing["feature_quantiles"]
        if item["feature"] == "missing_feature"
    )
    assert missing_item == {
        "feature": "missing_feature",
        "value": None,
        "display_value": "NA",
        "percentile": None,
        "median": 5.0,
        "direction": "missing",
        "imputed_for_model": True,
    }
    assert missing_item in scored_missing["missing_features"]
    assert all(
        item["feature"] != "missing_feature"
        for item in scored_missing["top_high_quantile_features"]
        + scored_missing["top_low_quantile_features"]
    )

    observed_item = next(
        item
        for item in result["scored_bursts"][3]["feature_quantiles"]
        if item["feature"] == "observed_feature"
    )
    assert observed_item["value"] == 4.0
    assert observed_item["display_value"] == "4"
    assert observed_item["percentile"] == 100.0
    assert observed_item["median"] == 2.5
    assert observed_item["direction"] == "high"
    assert observed_item["imputed_for_model"] is False
    assert observed_item in result["scored_bursts"][3]["top_high_quantile_features"]


def test_score_bursts_observed_quantile_explanations_use_only_observed_values():
    rows = [
        {**_feature_row(0, 1.0, 1.0), "sparse_feature": 1.0},
        {**_feature_row(1, 2.0, 2.0), "sparse_feature": None},
        {**_feature_row(2, 3.0, 3.0), "sparse_feature": 9.0},
    ]

    result = score_bursts(rows, config={"n_estimators": 16})

    low_item = next(
        item
        for item in result["scored_bursts"][0]["feature_quantiles"]
        if item["feature"] == "sparse_feature"
    )
    high_item = next(
        item
        for item in result["scored_bursts"][2]["feature_quantiles"]
        if item["feature"] == "sparse_feature"
    )
    missing_item = next(
        item
        for item in result["scored_bursts"][1]["feature_quantiles"]
        if item["feature"] == "sparse_feature"
    )

    assert low_item["percentile"] == 0.0
    assert high_item["percentile"] == 100.0
    assert missing_item["percentile"] is None
    assert missing_item["display_value"] == "NA"
    assert missing_item["value"] is None
    assert missing_item["median"] == 5.0
    assert missing_item["display_value"] != str(missing_item["median"])


def test_score_bursts_returns_unresolved_for_insufficient_rows_or_features():
    one_row = score_bursts([_feature_row(0, 10.0, 1.0)])
    no_features = score_bursts(
        [
            {**_feature_row(0, 10.0, 1.0), "all_null_feature": None},
            {**_feature_row(1, 10.0, 1.0), "all_null_feature": None},
        ]
    )

    assert one_row["run_status"] == "unresolved"
    assert one_row["scored_burst_count"] == 0
    assert "anomaly_score" not in one_row["scored_bursts"][0]
    assert any("At least 2 burst rows" in warning for warning in one_row["warnings"])
    assert no_features["run_status"] == "unresolved"
    assert no_features["fitted_features"] == []
    assert all("anomaly_score" not in row for row in no_features["scored_bursts"])
    assert any("No usable" in warning for warning in no_features["warnings"])


def test_score_bursts_excludes_precomputed_osm_burst_summaries_by_default_without_fetching():
    fixes = []
    bursts = []
    for index, (road_distance_m, railway_distance_m) in enumerate(
        [(5.0, 90.0), (20.0, 40.0), (250.0, 3.0)]
    ):
        start_ms = index * 100_000
        first_key = f"fix_{index}_0"
        second_key = f"fix_{index}_1"
        fixes.extend(
            [
                {
                    "fix_key": first_key,
                    "individual": "alpha",
                    "time_ms": start_ms,
                    "lon": 0.0,
                    "lat": 0.0,
                    "attributes": {
                        "osm:nearest_road_distance_m": road_distance_m,
                        "osm:nearest_railway_distance_m": railway_distance_m,
                    },
                },
                {
                    "fix_key": second_key,
                    "individual": "alpha",
                    "time_ms": start_ms + 10_000,
                    "lon": 0.001,
                    "lat": 0.0,
                    "attributes": {
                        "step_length_m": 10.0 + index,
                        "speed_mps": 1.0 + index,
                        "time_delta_s": 10.0,
                        "osm:nearest_road_distance_m": road_distance_m + 1.0,
                        "osm:nearest_railway_distance_m": railway_distance_m + 1.0,
                    },
                },
            ]
        )
        bursts.append(
            {
                "burst_id": f"alpha:train:burst_{index:06d}",
                "burst_idx": index,
                "individual": "alpha",
                "set_name": "train",
                "start_time_ms": start_ms,
                "end_time_ms": start_ms + 10_000,
                "fix_keys": [first_key, second_key],
            }
        )

    result = score_bursts(
        build_burst_feature_rows(fixes, bursts),
        config={"n_estimators": 32},
    )

    assert result["run_status"] == "completed"
    assert result["feature_set"] == "movement_only"
    assert "osm:nearest_road_distance_m__mean" in result["requested_features"]
    assert "osm:nearest_railway_distance_m__min" in result["requested_features"]
    assert "osm:nearest_road_distance_m__mean" not in result["candidate_model_features"]
    assert "osm:nearest_railway_distance_m__min" not in result["candidate_model_features"]
    assert "osm:nearest_road_distance_m__mean" not in result["fitted_features"]
    assert "osm:nearest_railway_distance_m__min" not in result["fitted_features"]
    assert result["excluded_by_feature_set"]["osm:nearest_road_distance_m__mean"] == (
        "context_feature_excluded_from_movement_only"
    )
    assert result["excluded_by_feature_set"]["osm:nearest_railway_distance_m__min"] == (
        "context_feature_excluded_from_movement_only"
    )
    context_result = score_bursts(
        build_burst_feature_rows(fixes, bursts),
        config={"n_estimators": 32, "feature_set": "movement_plus_context"},
    )
    assert context_result["run_status"] == "completed"
    assert context_result["feature_set"] == "movement_plus_context"
    assert context_result["excluded_by_feature_set"] == {}
    assert "osm:nearest_road_distance_m__mean" in context_result["candidate_model_features"]
    assert "osm:nearest_railway_distance_m__min" in context_result["candidate_model_features"]
    assert "osm:nearest_road_distance_m__mean" in context_result["fitted_features"]
    assert "osm:nearest_railway_distance_m__min" in context_result["fitted_features"]
    assert all(
        not item["feature"].startswith("osm:")
        for row in result["scored_bursts"]
        for item in row["feature_quantiles"]
    )
    assert any(
        item["feature"].startswith("osm:")
        for row in context_result["scored_bursts"]
        for item in row["feature_quantiles"]
    )
    ranking_source = (REPO_ROOT / "examples" / "movement" / "anomaly_ranking.py").read_text(
        encoding="utf-8"
    )
    analysis_source = (
        REPO_ROOT / "examples" / "movement" / "anomaly_analysis_template.py"
    ).read_text(encoding="utf-8")
    assert "fetch_osm_features" not in ranking_source
    assert "fetch_osm_features" not in analysis_source


def test_rank_individuals_uses_highest_burst_score_globally_per_individual():
    scored_bursts = [
        {
            "burst_id": "alpha:train:burst_000000",
            "individual": "alpha",
            "set_name": "train",
            "path_length_m": 9999.0,
            "anomaly_score": 0.10,
            "fix_keys": ["alpha_train"],
        },
        {
            "burst_id": "alpha:test:burst_000000",
            "individual": "alpha",
            "set_name": "test",
            "path_length_m": 1.0,
            "anomaly_score": 0.95,
            "fix_keys": ["alpha_test"],
        },
        {
            "burst_id": "beta:train:burst_000000",
            "individual": "beta",
            "set_name": "train",
            "path_length_m": 1000000.0,
            "anomaly_score": 0.70,
            "fix_keys": ["beta_train"],
        },
        {
            "burst_id": "gamma:train:burst_000000",
            "individual": "gamma",
            "set_name": "train",
            "path_length_m": 0.1,
            "anomaly_score": 0.95,
            "fix_keys": ["gamma_train"],
        },
    ]

    result = rank_individuals(scored_bursts)

    assert result["run_status"] == "completed"
    assert result["ranking_scope"] == "individual"
    assert result["ranking_method"] == "maximum_burst_anomaly_score"
    assert "fixes" not in result
    assert [
        (row["rank"], row["individual"], row["top_burst_id"], row["top_burst_score"])
        for row in result["ranked_individuals"]
    ] == [
        (1, "alpha", "alpha:test:burst_000000", 0.95),
        (2, "gamma", "gamma:train:burst_000000", 0.95),
        (3, "beta", "beta:train:burst_000000", 0.70),
    ]
    alpha = result["ranked_individuals"][0]
    assert alpha["burst_count"] == 2
    assert alpha["scored_burst_count"] == 2
    assert [ref["burst_id"] for ref in alpha["ranked_burst_refs"]] == [
        "alpha:test:burst_000000",
        "alpha:train:burst_000000",
    ]
    assert alpha["ranked_burst_refs"][0]["set_name"] == "test"
    assert alpha["ranked_burst_refs"][0]["fix_keys"] == ["alpha_test"]
    assert alpha["individual_score"] == alpha["top_burst_score"]


def test_rank_individuals_can_sum_positive_isolation_forest_decision_margins():
    scored_bursts = [
        {
            "burst_id": "alpha:burst_1",
            "individual": "alpha",
            "anomaly_score": 0.70,
            "decision_function": -0.12,
            "outlier_margin": 0.12,
        },
        {
            "burst_id": "alpha:burst_2",
            "individual": "alpha",
            "anomaly_score": 0.65,
            "decision_function": -0.10,
            "outlier_margin": 0.10,
        },
        {
            "burst_id": "alpha:burst_3",
            "individual": "alpha",
            "anomaly_score": 0.20,
            "decision_function": 0.30,
            "outlier_margin": 0.0,
        },
        {
            "burst_id": "beta:burst_1",
            "individual": "beta",
            "anomaly_score": 0.95,
            "decision_function": -0.20,
            "outlier_margin": 0.20,
        },
    ]

    result = rank_individuals(
        scored_bursts,
        config={"aggregation": "sum_outlier_margin"},
    )

    assert result["aggregation"] == "sum_outlier_margin"
    assert result["ranking_method"] == "sum_positive_outlier_decision_margin"
    assert [row["individual"] for row in result["ranked_individuals"]] == [
        "alpha",
        "beta",
    ]
    alpha, beta = result["ranked_individuals"]
    assert alpha["individual_score"] == pytest.approx(0.22)
    assert alpha["contributing_burst_count"] == 2
    assert alpha["top_burst_score"] == 0.70
    assert beta["individual_score"] == pytest.approx(0.20)


def test_rank_individuals_can_sum_source_outlier_counts_across_bursts():
    scored_bursts = [
        {"burst_id": "alpha:burst_1", "individual": "alpha", "anomaly_score": 2},
        {"burst_id": "alpha:burst_2", "individual": "alpha", "anomaly_score": 3},
        {"burst_id": "beta:burst_1", "individual": "beta", "anomaly_score": 4},
    ]

    result = rank_individuals(
        scored_bursts,
        config={"aggregation": "sum_anomaly_score"},
    )

    assert result["aggregation"] == "sum_anomaly_score"
    assert result["ranking_method"] == "sum_burst_anomaly_score"
    assert [row["individual"] for row in result["ranked_individuals"]] == [
        "alpha",
        "beta",
    ]
    assert result["ranked_individuals"][0]["individual_score"] == 5.0
    assert result["ranked_individuals"][0]["top_burst_score"] == 3.0


def test_rank_individuals_breaks_equal_bursts_by_track_and_skips_unscored_rows():
    scored_bursts = [
        {
            "burst_id": "alpha:train:burst_000001",
            "individual": "alpha",
            "set_name": "train",
            "anomaly_score": 0.5,
        },
        {
            "burst_id": "alpha:test:burst_000001",
            "individual": "alpha",
            "set_name": "test",
            "anomaly_score": 0.5,
        },
        {
            "burst_id": "alpha:test:burst_unscored",
            "individual": "alpha",
            "set_name": "test",
        },
        {
            "burst_id": "beta:train:burst_unscored",
            "individual": "beta",
            "set_name": "train",
            "anomaly_score": None,
        },
    ]

    result = rank_individuals(scored_bursts)

    assert result["individual_count"] == 1
    assert result["scored_burst_count"] == 2
    alpha = result["ranked_individuals"][0]
    assert alpha["top_burst_id"] == "alpha:test:burst_000001"
    assert alpha["burst_count"] == 3
    assert alpha["scored_burst_count"] == 2
    assert any("Skipped 2 burst row(s)" in warning for warning in result["warnings"])


def test_rank_individuals_handles_empty_or_unresolved_scored_bursts():
    empty = rank_individuals([])
    unscored = rank_individuals(
        [{"burst_id": "alpha:train:burst_000000", "individual": "alpha"}]
    )

    assert empty["run_status"] == "unresolved"
    assert empty["ranked_individuals"] == []
    assert empty["input_burst_count"] == 0
    assert any("No scored automatic bursts" in warning for warning in empty["warnings"])
    assert unscored["run_status"] == "unresolved"
    assert unscored["ranked_individuals"] == []
    assert unscored["scored_burst_count"] == 0
    assert "fixes" not in unscored
    assert any("without a finite anomaly_score" in warning for warning in unscored["warnings"])


def test_burst_anomaly_analysis_uses_compilable_template_file():
    template_text = ANOMALY_ANALYSIS_TEMPLATE_PATH.read_text(encoding="utf-8").strip() + "\n"

    assert BURST_ANOMALY_ANALYSIS_SCRIPT.endswith(template_text)
    assert "_VIBECLEANING_BUNDLED_SOURCES" in BURST_ANOMALY_ANALYSIS_SCRIPT
    assert "repo_root" not in BURST_ANOMALY_ANALYSIS_SCRIPT
    compile(BURST_ANOMALY_ANALYSIS_SCRIPT, str(ANOMALY_ANALYSIS_TEMPLATE_PATH), "exec")


def test_frontend_exposes_read_only_burst_anomaly_ranking_panel():
    source = MOVEMENT_APP_JS.read_text(encoding="utf-8")
    individuals_sheet = source[
        source.index('<div class="movement-side-sheet individuals"'):
        source.index('<div class="movement-side-sheet table hidden"')
    ]
    ranking_sheet = source[
        source.index('<div class="movement-side-sheet ranking hidden"'):
        source.index('<div class="movement-slider-row">')
    ]
    handler = source[
        source.index("async runBurstAnomalyRanking({ openRankingSheet = true } = {})"):
        source.index("  renderAnomalyRanking() {")
    ]
    renderer = source[
        source.index("  renderAnomalyRanking() {"):
        source.index("async runSelectedCandidateQuery()")
    ]
    ranking_handler = source[
        source.index("  async inspectRankingBurst("):
        source.index("  renderAnomalyRanking() {")
    ]

    assert 'data-role="run-anomaly-ranking"' in source
    assert 'data-role="anomaly-feature-set"' in source
    assert '<option value="movement_only">Movement only</option>' in source
    assert "Movement + OSM context" in source
    assert "hasOsmContextFeatures()" in source
    assert "syncAnomalyFeatureSetOptions" in source
    assert 'key.startsWith("osm:") && key.endsWith("_distance_m")' in source
    assert "getAnomalyFeatureSet()" in source
    assert 'feature_set: featureSet' in source
    assert 'Feature set: ${String(modelFit.feature_set).replaceAll("_", " ")}' in source
    assert "anomalyFeatureSetLabel" in source
    assert "Running ${providers.length} burst ranking" in source
    assert "Created ${formatCount(this.anomalyRankings.size)} ranking views" in source
    assert '<option value="isolation_forest_decision_margin">Isolation forest — total decision margin</option>' in source
    assert '<label data-role="ranking-method-control">View ranking' in ranking_sheet
    assert 'this.refs.runAnomalyRanking.textContent = "Rank bursts"' in source
    assert "sum_positive_outlier_decision_margin" not in source
    assert "Total source outliers" in renderer
    assert "Total decision margin" in renderer
    assert "Top burst score" in renderer
    assert "decision margin" in source
    assert "modelFit.excluded_by_feature_set" in source
    assert "Fitted feature sample:" in source
    assert 'data-role="side-tab-ranking">Burst Ranking' in source
    assert 'data-role="side-sheet-ranking"' in source
    assert 'data-role="anomaly-ranking"' not in individuals_sheet
    assert 'data-role="anomaly-ranking"' in ranking_sheet
    assert 'this.refs.sideTabRanking.addEventListener("click", () => this.setSideSheet("ranking"))' in source
    assert "runBurstAnomalyRanking()" in source
    assert "run-burst-anomaly-ranking" in source
    assert "waitForAnomalyRankingJob" in handler
    assert "analysis-jobs" in handler
    assert "waitForAbortableDelay" in source
    assert 'this.setSideSheet("ranking")' in handler
    assert "ranked_individuals" in source
    assert "ranked_burst_refs" in source
    assert 'data-role="ranking-burst-refs"' in source
    assert 'data-action="zoom-ranking-burst"' in source
    assert 'data-action="check-ranking-burst"' in source
    assert 'data-role="ranking-burst-explanation"' in source
    assert "renderAnomalyWhy(ref)" in source
    assert "getCompactAnomalyWhyItems" in source
    assert "isCompactWhyItem" in source
    assert "whyDirectionLabel" in source
    assert "hasAnomalyExplanationData" in source
    assert "anomalyFeatureLabel" in source
    assert '<strong>Why:</strong>' in source
    assert "mixed feature pattern" in source
    assert "percentile >= 80" in source
    assert "percentile >= 95 ? \"very high\" : \"high\"" in source
    assert "percentile <= 20" in source
    assert "percentile <= 5 ? \"very low\" : \"low\"" in source
    assert ".filter(item => this.isCompactWhyItem(item))" in source
    assert "movement-anomaly-burst-rank-badge" in source
    assert "is-ranking-burst" in source
    assert "★" in source
    assert "focusedRankingBurst" in source
    assert "setFocusedRankingBurst(ref)" in source
    assert "this.setFocusedRankingBurst(ref)" in ranking_handler
    assert "getFocusedRankingBurstFixes" in source
    assert "clearFocusedRankingBurstIfHidden" in source
    # A focused burst is styled inside the shared burst layers rather than
    # drawn again by a parallel focused-burst layer stack.
    assert 'id: "movement-burst-casing"' in source
    assert 'id: "movement-bursts"' in source
    assert 'id: "movement-burst-focus-ring"' in source
    assert "movement-focused-ranking-burst" not in source
    assert "movement-auto-burst-endpoints" not in source
    assert "focusedRankingBurstPoints" in source
    assert "hasFocusedRankingBurst = focusedRankingBurstFixes.length > 0" in source
    assert "focusedBurstId = hasFocusedRankingBurst" in source
    assert "mutedRankingContextColor" in source
    assert 'id: "movement-temporal-focal-halos"' in source
    assert 'id: "movement-temporal-focal-points"' in source
    # Context bursts mute while another burst is focused, and source-flagged
    # context stays distinguishable from clean context.
    assert "this.mutedRankingContextColor(item.color, item?.sourceFlagged ? 22 : 36)" in source
    assert "const BURST_FOCUS_CASING_COLOR = [216, 180, 254, 255];" in source
    assert "const BURST_FOCUS_RING_COLOR = [216, 180, 254, 235];" in source
    assert "focus-ranking-burst" not in source
    assert "Focus burst" not in source
    assert "<summary>Details</summary>" in source
    assert "Observed feature-value quantiles, not SHAP/model attribution." in source
    assert "High observed quantiles" in source
    assert "Low observed quantiles" in source
    assert "Missing fitted features" in source
    assert "top_high_quantile_features" in source
    assert "top_low_quantile_features" in source
    assert "missing_features" in source
    assert "displayValue" in source
    assert "percentile" in source
    assert 'valueLabel = item.displayValue || (item.direction === "missing" ? "NA" : "n/a")' in source
    assert "very high" in source
    assert "very low" in source
    assert 'direction === "missing" ? 5 : 3' in source
    assert "+${escapeHtml(formatCount(moreCount))} more" in source
    assert "handleAnomalyRankingClick" in source
    assert "inspectRankingBurst" in source
    assert "isolateIndividual: true" in ranking_handler
    assert "getRankingBurstPath" in source
    assert "this.zoomToPath(path)" in ranking_handler
    assert "ref.fix_keys" in ranking_handler
    assert "this.data.selectedFixKeys" in ranking_handler
    assert "openSegmentModal" not in ranking_handler
    assert "openIssueModal" not in ranking_handler
    assert "run-candidate-query" not in ranking_handler
    assert "run-burst-anomaly-ranking" not in ranking_handler
    assert "enrich-osm-context" not in ranking_handler
    assert "top_burst_score" in source
    assert "top_burst_id" in source
    assert "burst_count" in source
    assert "scored_burst_count" in source
    assert "formatBurstGapMetadata(parseMovementBurstGap" in source
    assert "artifact?.model_fit" in source
    assert "Individuals are ranked by their highest-scoring burst" not in source
    assert "Mark " not in renderer


def test_burst_anomaly_route_creates_analysis_artifact_without_dataset_mutation(tmp_path):
    client, study_dir, dataset_id = _create_anomaly_analysis_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-burst-anomaly-ranking",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": 60,
            "burst_gap_quantile": 0.75,
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    analysis = payload["analysis"]
    summary = payload["summary"]
    assert analysis["dataset_id"] == dataset_id
    assert analysis["parameters"]["action"] == "run_burst_anomaly_ranking"
    assert analysis["parameters"]["feature_set"] == "movement_only"
    assert "(movement only)" in analysis["title"]
    assert analysis["output_artifacts"] == ["burst_anomaly_ranking.json"]
    assert {artifact["logical_name"] for artifact in analysis["realized_output_artifacts"]} == {
        "burst_anomaly_ranking.json"
    }
    assert summary["run_status"] == "completed"
    assert summary["input_artifact"]["dataset_id"] == dataset_id
    assert summary["input_artifact"]["logical_name"] == "movement.csv"
    assert summary["burst_gap"]["mode"] == "manual"
    assert summary["burst_gap"]["fallback_seconds"] == 60.0
    assert summary["burst_gap"]["effective_seconds"] == 60.0
    assert summary["burst_gap"]["quantile"] == 0.75
    assert summary["model_fit"]["model"] == "IsolationForest"
    assert summary["ranking_method"] == "isolation_forest"
    assert summary["ranking_provider"] == "isolation_forest"
    assert summary["ranking_schema_version"] == 2
    assert summary["model_fit"]["ranking_method"] == "isolation_forest"
    assert summary["model_fit"]["feature_set"] == "movement_only"
    assert summary["model_fit"]["scored_burst_count"] == 4
    assert summary["model_fit"]["preprocessing"]["scaling"] == "none"
    assert "path_length_m" in summary["model_fit"]["fitted_features"]
    assert "gps:hdop__mean" in summary["model_fit"]["fitted_features"]
    assert "height-above-msl__mean" in summary["model_fit"]["fitted_features"]
    assert summary["model_fit"]["excluded_by_feature_set"] == {}
    assert "scored_bursts" not in summary
    assert summary["individual_ranking_summary"]["ranking_scope"] == "individual"
    assert summary["individual_ranking_summary"]["aggregation"] == "maximum_anomaly_score"
    assert set(summary["individual_rankings"]) == {
        "isolation_forest",
        "isolation_forest_decision_margin",
    }
    assert summary["individual_rankings"]["isolation_forest"]["aggregation"] == (
        "maximum_anomaly_score"
    )
    assert summary["individual_rankings"]["isolation_forest_decision_margin"][
        "aggregation"
    ] == "sum_outlier_margin"
    assert len(summary["ranked_individuals"]) == 2
    assert all("ranked_burst_refs" in row for row in summary["ranked_individuals"])
    assert all(row["ranked_burst_refs"] for row in summary["ranked_individuals"])
    for row in summary["ranked_individuals"]:
        for ref in row["ranked_burst_refs"]:
            assert ref["individual"] == row["individual"]
            assert ref["burst_id"]
            assert "anomaly_score" in ref
            assert "fix_keys" in ref
            assert "top_high_quantile_features" in ref
            assert "top_low_quantile_features" in ref
            assert "missing_features" in ref
            assert "feature_quantiles" not in ref
            assert all(
                not item["feature"].startswith("osm:")
                for item in (
                    ref["top_high_quantile_features"]
                    + ref["top_low_quantile_features"]
                    + ref["missing_features"]
                )
            )
    assert "scorer" not in summary
    assert "individual_ranking" not in summary
    assert "fixes" not in summary

    output_path = (
        project_paths(study_dir)["analyses"]
        / analysis["analysis_id"]
        / "outputs"
        / "burst_anomaly_ranking.json"
    )
    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert len(output["scored_bursts"]) == 4
    assert all("anomaly_score" in burst for burst in output["scored_bursts"])
    assert all("ranked_burst_refs" in row for row in output["ranked_individuals"])
    assert output["input_artifact"] == summary["input_artifact"]
    assert output["model_fit"] == summary["model_fit"]
    assert not (
        project_paths(study_dir)["analyses"]
        / analysis["analysis_id"]
        / "outputs"
        / "fix_anomaly_scores.json"
    ).exists()

    state = client.get(
        "/api/apps/movement/family/movement_clean/study/test_study/state"
    ).json()
    assert state["project"]["current_dataset_id"] == dataset_id
    assert state["counts"]["analyses"] == 1
    assert state["counts"]["steps"] == 0


def test_burst_anomaly_route_supports_total_decision_margin_ranking(tmp_path):
    client, _, dataset_id = _create_anomaly_analysis_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-burst-anomaly-ranking",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": 60,
            "ranking_method": "isolation_forest_decision_margin",
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["analysis"]["parameters"]["ranking_method"] == "isolation_forest"
    assert payload["analysis"]["parameters"]["ranking_provider"] == "isolation_forest"
    assert payload["analysis"]["parameters"]["ranking_schema_version"] == 2
    summary = payload["summary"]
    assert summary["ranking_method"] == "isolation_forest"
    assert summary["model_fit"]["ranking_method"] == "isolation_forest"
    margin = summary["individual_rankings"]["isolation_forest_decision_margin"]
    assert margin["aggregation"] == "sum_outlier_margin"
    assert margin["ranking_method"] == (
        "sum_positive_outlier_decision_margin"
    )
    assert all("individual_score" in row for row in margin["ranked_individuals"])

    analysis_id = payload["analysis"]["analysis_id"]
    for ranking_method in ("isolation_forest", "isolation_forest_decision_margin"):
        history = client.get(
            "/api/apps/movement/family/movement_clean/study/test_study/analyses",
            params={
                "dataset_id": dataset_id,
                "logical_name": "movement.csv",
                "burst_gap_mode": "manual",
                "burst_gap_seconds": 60,
                "ranking_method": ranking_method,
            },
        )
        assert history.status_code == 200
        assert history.json()["latest_compatible_by_action"][
            "run_burst_anomaly_ranking"
        ] == analysis_id


def test_burst_anomaly_route_rejects_unknown_ranking_method(tmp_path):
    client, _, dataset_id = _create_anomaly_analysis_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-burst-anomaly-ranking",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "ranking_method": "mean_raw_score",
            "user": "reviewer",
        },
    )

    assert response.status_code == 400
    assert response.json()["error"] == "Invalid ranking_method"


def test_burst_anomaly_route_accepts_movement_plus_context_feature_set(tmp_path):
    client, study_dir, dataset_id = _create_context_anomaly_analysis_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-burst-anomaly-ranking",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement_osm_context.csv",
            "burst_gap_mode": "manual",
            "burst_gap_seconds": 60,
            "feature_set": "movement_plus_context",
            "user": "reviewer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    analysis = payload["analysis"]
    summary = payload["summary"]
    assert analysis["parameters"]["feature_set"] == "movement_plus_context"
    assert "(movement + OSM context)" in analysis["title"]
    assert summary["model_fit"]["feature_set"] == "movement_plus_context"
    assert summary["model_fit"]["excluded_by_feature_set"] == {}
    assert "osm:nearest_road_distance_m__mean" in summary["model_fit"]["candidate_model_features"]
    assert "osm:nearest_railway_distance_m__min" in summary["model_fit"]["candidate_model_features"]
    assert "osm:nearest_road_distance_m__mean" in summary["model_fit"]["fitted_features"]
    assert "osm:nearest_railway_distance_m__min" in summary["model_fit"]["fitted_features"]

    output_path = (
        project_paths(study_dir)["analyses"]
        / analysis["analysis_id"]
        / "outputs"
        / "burst_anomaly_ranking.json"
    )
    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert output["model_fit"]["feature_set"] == "movement_plus_context"
    assert output["model_fit"]["fitted_features"] == summary["model_fit"]["fitted_features"]


def test_burst_anomaly_route_rejects_unknown_feature_set(tmp_path):
    client, _, dataset_id = _create_anomaly_analysis_client(tmp_path)

    response = client.post(
        "/api/apps/movement/family/movement_clean/study/test_study/actions/run-burst-anomaly-ranking",
        json={
            "dataset_id": dataset_id,
            "logical_name": "movement.csv",
            "feature_set": "context_only",
            "user": "reviewer",
        },
    )

    assert response.status_code == 400
    assert response.json()["error"] == "Invalid feature_set"
