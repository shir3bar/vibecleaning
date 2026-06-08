from math import isfinite

import numpy as np
from sklearn.decomposition import PCA
from sklearn.neighbors import NearestNeighbors

from examples.movement.burst_feature_matrix import (
    DEFAULT_FEATURE_SET,
    prepare_burst_feature_matrix,
)


DEFAULT_NEIGHBOR_COUNT = 10
MIN_BURST_ROWS = 2
SIGN_CONVENTION = "largest_absolute_loading_positive_per_component"
STANDARDIZATION_METHOD = "median_imputed_standardized_features"
PCA_TYPE = "correlation_pca"


def _normalize_neighbor_count(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("neighbor_count must be a positive integer")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("neighbor_count must be a positive integer") from exc
    if normalized <= 0:
        raise ValueError("neighbor_count must be a positive integer")
    return normalized


def _burst_metadata(row: dict) -> dict:
    metadata = {
        "burst_id": str(row.get("burst_id", "")),
        "individual": str(row.get("individual", "")),
        "n_fixes": int(row.get("n_fixes", 0) or 0),
        "fix_keys": [str(value) for value in (row.get("fix_keys") or [])],
    }
    for field in ("set_name", "start_time_ms", "end_time_ms"):
        if field in row:
            metadata[field] = row.get(field)
    if "anomaly_score" in row:
        score = row.get("anomaly_score")
        if isinstance(score, (int, float)) and not isinstance(score, bool) and isfinite(float(score)):
            metadata["anomaly_score"] = float(score)
    if "rank" in row:
        rank = row.get("rank")
        if isinstance(rank, int) and not isinstance(rank, bool):
            metadata["rank"] = rank
    return metadata


def _orient_components(
    coordinates: np.ndarray,
    components: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    oriented_coordinates = np.array(coordinates, dtype=float, copy=True)
    oriented_components = np.array(components, dtype=float, copy=True)
    for component_index in range(oriented_components.shape[0]):
        component = oriented_components[component_index]
        anchor_index = int(np.argmax(np.abs(component)))
        if component[anchor_index] < 0.0:
            oriented_components[component_index] *= -1.0
            oriented_coordinates[:, component_index] *= -1.0
    return oriented_coordinates, oriented_components


def _nearest_neighbors(
    matrix: np.ndarray,
    burst_ids: list[str],
    *,
    neighbor_count: int,
) -> dict[str, list[dict]]:
    if len(burst_ids) < 2:
        return {burst_id: [] for burst_id in burst_ids}

    requested_count = min(len(burst_ids), neighbor_count + 1)
    search = NearestNeighbors(
        n_neighbors=requested_count,
        algorithm="brute",
        metric="euclidean",
    )
    search.fit(matrix)
    distances, indexes = search.kneighbors(matrix)
    result: dict[str, list[dict]] = {}
    for row_index, burst_id in enumerate(burst_ids):
        candidates = [
            {
                "burst_id": burst_ids[int(candidate_index)],
                "distance": float(distance),
            }
            for distance, candidate_index in zip(distances[row_index], indexes[row_index])
            if int(candidate_index) != row_index
        ]
        candidates.sort(key=lambda item: (item["distance"], item["burst_id"]))
        result[burst_id] = [
            {
                **candidate,
                "rank": rank,
            }
            for rank, candidate in enumerate(candidates[:neighbor_count], start=1)
        ]
    return result


def build_burst_feature_space(
    feature_rows: list[dict],
    *,
    feature_set: str = DEFAULT_FEATURE_SET,
    neighbor_count: int = DEFAULT_NEIGHBOR_COUNT,
) -> dict:
    burst_rows = [dict(row) for row in feature_rows]
    normalized_neighbor_count = _normalize_neighbor_count(neighbor_count)
    prepared = prepare_burst_feature_matrix(
        burst_rows,
        feature_set=feature_set,
        standardize=True,
    )
    warnings = []
    if prepared["dropped_features"]:
        warnings.append(
            f"Dropped {len(prepared['dropped_features'])} non-usable candidate feature column(s)."
        )
    if len(burst_rows) < MIN_BURST_ROWS:
        warnings.append(
            f"At least {MIN_BURST_ROWS} burst rows are required for PCA feature-space projection."
        )
    if not prepared["fitted_features"]:
        warnings.append(
            "No usable non-constant numeric burst features are available for PCA projection."
        )

    feature_matrix_metadata = {
        "requested_features": prepared["requested_features"],
        "candidate_model_features": prepared["candidate_model_features"],
        "fitted_features": prepared["fitted_features"],
        "dropped_features": prepared["dropped_features"],
        "excluded_metadata": prepared["excluded_metadata"],
        "excluded_by_feature_set": prepared["excluded_by_feature_set"],
        "feature_medians": prepared["feature_medians"],
        "feature_means_or_centers": prepared["feature_means"],
        "feature_scales": prepared["feature_scales"],
        "imputed_value_counts": prepared["imputed_value_counts"],
        "standardization": STANDARDIZATION_METHOD,
        "pca_type": PCA_TYPE,
    }
    result = {
        "run_status": "unresolved",
        "analysis_type": "burst_feature_space",
        "projection_method": "pca",
        "feature_set": feature_set,
        "input_burst_count": len(burst_rows),
        "projected_burst_count": 0,
        "neighbor_count": normalized_neighbor_count,
        "feature_matrix": feature_matrix_metadata,
        "pca": {
            "n_components_requested": 2,
            "n_components_fitted": 0,
            "explained_variance_ratio": [],
            "components": [],
            "sign_convention": SIGN_CONVENTION,
        },
        "points": [],
        "nearest_neighbors": {},
        "warnings": warnings,
    }
    if len(burst_rows) < MIN_BURST_ROWS or not prepared["fitted_features"]:
        return result

    matrix = np.asarray(prepared["matrix"], dtype=float)
    n_components = min(2, matrix.shape[0], matrix.shape[1])
    pca = PCA(n_components=n_components, svd_solver="full")
    coordinates = pca.fit_transform(matrix)
    coordinates, components = _orient_components(coordinates, pca.components_)

    burst_ids = [str(row.get("burst_id", "")) for row in burst_rows]
    neighbors = _nearest_neighbors(
        matrix,
        burst_ids,
        neighbor_count=normalized_neighbor_count,
    )
    points = []
    for row_index, row in enumerate(burst_rows):
        burst_id = burst_ids[row_index]
        point = {
            **_burst_metadata(row),
            "pc1": float(coordinates[row_index, 0]),
            "pc2": (
                float(coordinates[row_index, 1])
                if n_components > 1
                else 0.0
            ),
            "nearest_neighbors": neighbors.get(burst_id, []),
        }
        points.append(point)

    component_rows = []
    for component_index in range(n_components):
        component_rows.append(
            {
                "component": f"pc{component_index + 1}",
                "loadings": {
                    field: float(components[component_index, feature_index])
                    for feature_index, field in enumerate(prepared["fitted_features"])
                },
            }
        )

    result.update(
        {
            "run_status": "completed",
            "projected_burst_count": len(points),
            "pca": {
                "n_components_requested": 2,
                "n_components_fitted": n_components,
                "explained_variance_ratio": [
                    float(value) for value in pca.explained_variance_ratio_
                ],
                "components": component_rows,
                "sign_convention": SIGN_CONVENTION,
            },
            "points": points,
            "nearest_neighbors": neighbors,
        }
    )
    return result
