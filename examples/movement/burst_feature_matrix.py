from math import isfinite, sqrt
from numbers import Real
from statistics import median


DEFAULT_FEATURE_SET = "movement_only"
FEATURE_SET_MOVEMENT_ONLY = "movement_only"
FEATURE_SET_MOVEMENT_PLUS_CONTEXT = "movement_plus_context"
SUPPORTED_FEATURE_SETS = {
    FEATURE_SET_MOVEMENT_ONLY,
    FEATURE_SET_MOVEMENT_PLUS_CONTEXT,
}
FEATURE_SET_EXCLUSION_CONTEXT = "context_feature_excluded_from_movement_only"
METADATA_COLUMNS = {
    "anomaly_score",
    "burst_gap_seconds",
    "burst_id",
    "burst_idx",
    "end_fix_key",
    "end_time",
    "end_time_ms",
    "fix_count",
    "fix_keys",
    "individual",
    "n_fixes",
    "path",
    "set",
    "set_name",
    "start_fix_key",
    "start_time",
    "start_time_ms",
}


def is_metadata_column(field: str) -> bool:
    normalized = str(field).lower()
    return (
        normalized in METADATA_COLUMNS
        or normalized.startswith("start_time")
        or normalized.startswith("end_time")
    )


def numeric_value(value: object) -> float | None:
    if value is None or isinstance(value, bool) or not isinstance(value, Real):
        return None
    number = float(value)
    return number if isfinite(number) else None


def is_missing_numeric(value: object) -> bool:
    return value is None or (
        isinstance(value, Real) and not isinstance(value, bool) and not isfinite(float(value))
    )


def excluded_by_feature_set(requested_features: list[str], feature_set: str) -> dict[str, str]:
    if feature_set == FEATURE_SET_MOVEMENT_PLUS_CONTEXT:
        return {}
    if feature_set == FEATURE_SET_MOVEMENT_ONLY:
        return {
            field: FEATURE_SET_EXCLUSION_CONTEXT
            for field in requested_features
            if field.lower().startswith("osm:")
        }
    raise ValueError(f"Unsupported burst feature_set: {feature_set}")


def _standardize_matrix(matrix: list[list[float]], fitted_features: list[str]) -> dict:
    if not matrix or not fitted_features:
        return {
            "feature_means": {},
            "feature_scales": {},
            "standardized_matrix": [[] for _ in matrix],
        }

    feature_means: dict[str, float] = {}
    feature_scales: dict[str, float] = {}
    column_count = len(fitted_features)
    row_count = len(matrix)
    for column_index, field in enumerate(fitted_features):
        column = [row[column_index] for row in matrix]
        feature_mean = float(sum(column) / row_count)
        variance = sum((value - feature_mean) ** 2 for value in column) / row_count
        feature_scale = float(sqrt(variance))
        if feature_scale == 0.0:
            feature_scale = 1.0
        feature_means[field] = feature_mean
        feature_scales[field] = feature_scale

    standardized_matrix = [
        [
            float((row[column_index] - feature_means[fitted_features[column_index]])
                  / feature_scales[fitted_features[column_index]])
            for column_index in range(column_count)
        ]
        for row in matrix
    ]
    return {
        "feature_means": feature_means,
        "feature_scales": feature_scales,
        "standardized_matrix": standardized_matrix,
    }


def prepare_burst_feature_matrix(
    feature_rows: list[dict],
    *,
    feature_set: str = DEFAULT_FEATURE_SET,
    standardize: bool = False,
) -> dict:
    if feature_set not in SUPPORTED_FEATURE_SETS:
        raise ValueError(f"Unsupported burst feature_set: {feature_set}")
    if not isinstance(standardize, bool):
        raise ValueError("standardize must be a boolean")

    columns = sorted({str(field) for row in feature_rows for field in row})
    excluded_metadata = [field for field in columns if is_metadata_column(field)]
    requested_features = [field for field in columns if field not in excluded_metadata]
    excluded_by_set = excluded_by_feature_set(requested_features, feature_set)
    candidate_model_features = [
        field for field in requested_features if field not in excluded_by_set
    ]
    fitted_features = []
    dropped_features: dict[str, str] = {}
    feature_medians: dict[str, float] = {}
    imputed_value_counts: dict[str, int] = {}

    for field in candidate_model_features:
        values = [row.get(field) for row in feature_rows]
        if any(not is_missing_numeric(value) and numeric_value(value) is None for value in values):
            dropped_features[field] = "nonnumeric"
            continue
        numeric_values = [
            numeric
            for value in values
            if (numeric := numeric_value(value)) is not None
        ]
        if not numeric_values:
            dropped_features[field] = "all_null"
            continue
        if min(numeric_values) == max(numeric_values):
            dropped_features[field] = "constant"
            continue
        feature_median = float(median(numeric_values))
        fitted_features.append(field)
        feature_medians[field] = feature_median
        imputed_value_counts[field] = sum(
            numeric_value(value) is None for value in values
        )

    imputed_matrix = [
        [
            numeric_value(row.get(field))
            if numeric_value(row.get(field)) is not None
            else feature_medians[field]
            for field in fitted_features
        ]
        for row in feature_rows
    ]
    standardized = (
        _standardize_matrix(imputed_matrix, fitted_features)
        if standardize
        else {
            "feature_means": {},
            "feature_scales": {},
            "standardized_matrix": [],
        }
    )
    return {
        "scaling": "standardize" if standardize else "none",
        "excluded_metadata": excluded_metadata,
        "requested_features": requested_features,
        "candidate_model_features": candidate_model_features,
        "excluded_by_feature_set": excluded_by_set,
        "fitted_features": fitted_features,
        "dropped_features": dropped_features,
        "feature_medians": feature_medians,
        "imputed_value_counts": imputed_value_counts,
        "imputed_matrix": imputed_matrix,
        "matrix": (
            standardized["standardized_matrix"]
            if standardize
            else imputed_matrix
        ),
        "feature_means": standardized["feature_means"],
        "feature_scales": standardized["feature_scales"],
    }
