from pathlib import Path
import sys

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from examples.movement.synthetic_burst_similarity import (
    build_burst_labels,
    evaluate_stored_app_neighbors,
    parse_fix_key_reference,
)


RAW_SYNTHETIC_CSV = """event-id,individual-local-identifier,synthetic:is_anomaly,synthetic:anomaly_type,synthetic:injection_id
a1,alpha,false,normal,none
a2,alpha,true,gps_spike_single,gps_spike_single_001
a3,alpha,true,gps_spike_single,gps_spike_single_002
b1,beta,false,normal,none
b2,beta,true,gps_spike_single,gps_spike_single_003
c1,gamma,true,gps_spike_cluster,gps_spike_cluster_001
,delta,false,normal,none
"""


def _write_raw_csv(tmp_path: Path) -> Path:
    path = tmp_path / "synthetic.csv"
    path.write_text(RAW_SYNTHETIC_CSV, encoding="utf-8")
    return path


def _feature_space() -> dict:
    return {
        "points": [
            {
                "burst_id": "alpha:train:burst_000000",
                "individual": "alpha",
                "n_fixes": 2,
                "fix_keys": ["id:a1#row:1", "id:a2#row:2"],
                "nearest_neighbors": [
                    {"burst_id": "alpha:train:burst_000001", "distance": 0.1, "rank": 1},
                    {"burst_id": "beta:train:burst_000000", "distance": 0.2, "rank": 2},
                    {"burst_id": "beta:train:burst_000001", "distance": 0.3, "rank": 3},
                    {"burst_id": "gamma:train:burst_000000", "distance": 0.4, "rank": 4},
                ],
            },
            {
                "burst_id": "alpha:train:burst_000001",
                "individual": "alpha",
                "n_fixes": 1,
                "fix_keys": ["id:a3#row:3"],
                "nearest_neighbors": [
                    {"burst_id": "alpha:train:burst_000000", "distance": 0.1, "rank": 1},
                    {"burst_id": "beta:train:burst_000001", "distance": 0.2, "rank": 2},
                    {"burst_id": "beta:train:burst_000000", "distance": 0.3, "rank": 3},
                ],
            },
            {
                "burst_id": "beta:train:burst_000000",
                "individual": "beta",
                "n_fixes": 1,
                "fix_keys": ["id:b1#row:4"],
                "nearest_neighbors": [],
            },
            {
                "burst_id": "beta:train:burst_000001",
                "individual": "beta",
                "n_fixes": 1,
                "fix_keys": ["id:b2#row:5"],
                "nearest_neighbors": [
                    {"burst_id": "beta:train:burst_000000", "distance": 0.1, "rank": 1},
                    {"burst_id": "alpha:train:burst_000000", "distance": 0.2, "rank": 2},
                    {"burst_id": "gamma:train:burst_000000", "distance": 0.3, "rank": 3},
                ],
            },
            {
                "burst_id": "gamma:train:burst_000000",
                "individual": "gamma",
                "n_fixes": 1,
                "fix_keys": ["id:c1#row:6"],
                "nearest_neighbors": [
                    {"burst_id": "beta:train:burst_000000", "distance": 0.1, "rank": 1},
                    {"burst_id": "beta:train:burst_000001", "distance": 0.2, "rank": 2},
                ],
            },
            {
                "burst_id": "delta:train:burst_000000",
                "individual": "delta",
                "n_fixes": 1,
                "fix_keys": ["row:7|delta|1704067200000"],
                "nearest_neighbors": [],
            },
        ],
    }


def test_fix_key_mapping_by_event_id_and_row_index_fallback(tmp_path):
    raw_csv = _write_raw_csv(tmp_path)
    event_ref = parse_fix_key_reference("id:a2#row:2")
    row_ref = parse_fix_key_reference("row:7|delta|1704067200000")

    assert event_ref.event_id == "a2"
    assert event_ref.row_number == 2
    assert row_ref.event_id is None
    assert row_ref.row_number == 7

    labels = build_burst_labels(raw_csv, _feature_space())
    delta = labels[labels["burst_id"] == "delta:train:burst_000000"].iloc[0]
    assert delta["individual_id"] == "delta"
    assert delta["burst_type"] == "normal"


def test_burst_labels_construction(tmp_path):
    labels = build_burst_labels(_write_raw_csv(tmp_path), _feature_space())

    assert labels.columns.tolist() == [
        "burst_id",
        "individual_id",
        "burst_type",
        "n_fixes",
        "n_anomaly_fixes",
        "fraction_anomaly",
    ]
    alpha = labels[labels["burst_id"] == "alpha:train:burst_000000"].iloc[0]
    assert alpha["individual_id"] == "alpha"
    assert alpha["burst_type"] == "gps_spike_single"
    assert alpha["n_fixes"] == 2
    assert alpha["n_anomaly_fixes"] == 1
    assert alpha["fraction_anomaly"] == 0.5

    normal = labels[labels["burst_id"] == "beta:train:burst_000000"].iloc[0]
    assert normal["burst_type"] == "normal"
    assert normal["n_anomaly_fixes"] == 0


def test_similarity_eval_uses_stored_top_k_and_excludes_same_individual(tmp_path):
    tables = evaluate_stored_app_neighbors(
        _write_raw_csv(tmp_path),
        _feature_space(),
        top_k=3,
    )
    row = tables.similarity_eval[
        tables.similarity_eval["query_burst_id"] == "alpha:train:burst_000000"
    ].iloc[0]

    assert row["query_type"] == "gps_spike_single"
    assert row["nearest_same_type_rank"] == 2
    assert row["same_type_count_top_k"] == 1
    assert row["n_eligible_neighbors_top_k"] == 3
    assert "r_precision" not in tables.similarity_eval.columns


def test_include_same_individual_option_changes_rank_and_counts(tmp_path):
    tables = evaluate_stored_app_neighbors(
        _write_raw_csv(tmp_path),
        _feature_space(),
        include_same_individual=True,
        top_k=3,
    )
    row = tables.similarity_eval[
        tables.similarity_eval["query_burst_id"] == "alpha:train:burst_000000"
    ].iloc[0]

    assert row["nearest_same_type_rank"] == 1
    assert row["same_type_count_top_k"] == 2
    assert row["n_eligible_neighbors_top_k"] == 3


def test_neighbor_type_matrix_counts_top_k_neighbors(tmp_path):
    tables = evaluate_stored_app_neighbors(
        _write_raw_csv(tmp_path),
        _feature_space(),
        top_k=2,
    )
    matrix = tables.neighbor_type_matrix

    assert matrix.loc["gps_spike_single", "normal"] == 2
    assert matrix.loc["gps_spike_single", "gps_spike_single"] == 3
    assert matrix.loc["gps_spike_single", "gps_spike_cluster"] == 1
    assert matrix.loc["gps_spike_cluster", "normal"] == 1
    assert matrix.loc["gps_spike_cluster", "gps_spike_single"] == 1


def test_mixed_anomaly_burst_raises_clear_error(tmp_path):
    feature_space = {
        "points": [
            {
                "burst_id": "mixed",
                "individual": "alpha",
                "fix_keys": ["id:a2#row:2", "id:c1#row:6"],
                "nearest_neighbors": [],
            }
        ]
    }

    try:
        build_burst_labels(_write_raw_csv(tmp_path), feature_space)
    except ValueError as exc:
        assert "multiple synthetic anomaly types" in str(exc)
    else:
        raise AssertionError("expected mixed anomaly burst to raise ValueError")


def test_no_same_type_neighbor_present_has_null_rank(tmp_path):
    tables = evaluate_stored_app_neighbors(
        _write_raw_csv(tmp_path),
        _feature_space(),
        top_k=2,
    )
    row = tables.similarity_eval[
        tables.similarity_eval["query_burst_id"] == "gamma:train:burst_000000"
    ].iloc[0]

    assert pd.isna(row["nearest_same_type_rank"])
    assert row["same_type_count_top_k"] == 0
    assert row["n_eligible_neighbors_top_k"] == 2
