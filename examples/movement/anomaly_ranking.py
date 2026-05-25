from math import isfinite
from numbers import Real
from statistics import median

from sklearn.ensemble import IsolationForest


DEFAULT_MODEL_CONFIG = {
    "n_estimators": 1000,
    "contamination": "auto",
    "random_state": 0,
}
MIN_BURST_ROWS = 2
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


def _is_metadata_column(field: str) -> bool:
    normalized = str(field).lower()
    return (
        normalized in METADATA_COLUMNS
        or normalized.startswith("start_time")
        or normalized.startswith("end_time")
    )


def _numeric_value(value: object) -> float | None:
    if value is None or isinstance(value, bool) or not isinstance(value, Real):
        return None
    number = float(value)
    return number if isfinite(number) else None


def _is_missing_numeric(value: object) -> bool:
    return value is None or (
        isinstance(value, Real) and not isinstance(value, bool) and not isfinite(float(value))
    )


def _normalize_config(config: dict | None) -> dict:
    if config is None:
        return dict(DEFAULT_MODEL_CONFIG)
    unsupported = sorted(set(config) - set(DEFAULT_MODEL_CONFIG))
    if unsupported:
        raise ValueError(f"Unsupported anomaly ranking config values: {unsupported}")
    normalized = {**DEFAULT_MODEL_CONFIG, **config}
    if int(normalized["n_estimators"]) <= 0:
        raise ValueError("n_estimators must be positive")
    normalized["n_estimators"] = int(normalized["n_estimators"])
    return normalized


def _prepare_features(feature_rows: list[dict]) -> dict:
    columns = sorted({str(field) for row in feature_rows for field in row})
    excluded_metadata = [field for field in columns if _is_metadata_column(field)]
    requested_features = [field for field in columns if field not in excluded_metadata]
    fitted_features = []
    dropped_features: dict[str, str] = {}
    feature_medians: dict[str, float] = {}
    imputed_value_counts: dict[str, int] = {}

    for field in requested_features:
        values = [row.get(field) for row in feature_rows]
        if any(not _is_missing_numeric(value) and _numeric_value(value) is None for value in values):
            dropped_features[field] = "nonnumeric"
            continue
        numeric_values = [
            numeric
            for value in values
            if (numeric := _numeric_value(value)) is not None
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
            _numeric_value(value) is None for value in values
        )

    matrix = [
        [
            _numeric_value(row.get(field))
            if _numeric_value(row.get(field)) is not None
            else feature_medians[field]
            for field in fitted_features
        ]
        for row in feature_rows
    ]
    return {
        "excluded_metadata": excluded_metadata,
        "requested_features": requested_features,
        "fitted_features": fitted_features,
        "dropped_features": dropped_features,
        "feature_medians": feature_medians,
        "imputed_value_counts": imputed_value_counts,
        "matrix": matrix,
    }


def score_bursts(feature_rows: list[dict], config: dict | None = None) -> dict:
    """Score burst feature rows without creating fix- or individual-level outputs."""
    model_config = _normalize_config(config)
    burst_rows = [dict(row) for row in feature_rows]
    prepared = _prepare_features(burst_rows)
    warnings = []
    if prepared["dropped_features"]:
        warnings.append(
            f"Dropped {len(prepared['dropped_features'])} non-usable candidate feature column(s)."
        )
    if len(burst_rows) < MIN_BURST_ROWS:
        warnings.append(
            f"At least {MIN_BURST_ROWS} burst rows are required for anomaly scoring."
        )
    if not prepared["fitted_features"]:
        warnings.append("No usable non-constant numeric burst features are available for scoring.")

    result = {
        "run_status": "unresolved",
        "model": "IsolationForest",
        "model_config": model_config,
        "preprocessing": {
            "scaling": "none",
            "missing_value_strategy": "median_imputation_per_fitted_feature",
            "nonfinite_value_handling": "treated_as_missing",
            "feature_exclusions": ["metadata", "nonnumeric", "all_null", "constant"],
        },
        "input_burst_count": len(burst_rows),
        "scored_burst_count": 0,
        "requested_features": prepared["requested_features"],
        "fitted_features": prepared["fitted_features"],
        "dropped_features": prepared["dropped_features"],
        "excluded_metadata": prepared["excluded_metadata"],
        "feature_medians": prepared["feature_medians"],
        "imputed_value_counts": prepared["imputed_value_counts"],
        "warnings": warnings,
        "scored_bursts": burst_rows,
    }
    if len(burst_rows) < MIN_BURST_ROWS or not prepared["fitted_features"]:
        return result

    model = IsolationForest(**model_config)
    model.fit(prepared["matrix"])
    anomaly_scores = -model.score_samples(prepared["matrix"])
    result["run_status"] = "completed"
    result["scored_burst_count"] = len(burst_rows)
    result["scored_bursts"] = [
        {**row, "anomaly_score": float(score)}
        for row, score in zip(burst_rows, anomaly_scores)
    ]
    return result


def _burst_reference(row: dict, anomaly_score: float) -> dict:
    reference = {
        "burst_id": str(row.get("burst_id", "")),
        "anomaly_score": float(anomaly_score),
    }
    for field in (
        "set_name",
        "start_time_ms",
        "end_time_ms",
        "n_fixes",
        "fix_keys",
    ):
        if field in row:
            reference[field] = list(row[field]) if field == "fix_keys" else row[field]
    return reference


def rank_individuals(scored_bursts: list[dict], config: dict | None = None) -> dict:
    """Rank individuals globally by their highest scored automatic burst."""
    if config not in (None, {}):
        raise ValueError("Individual ranking does not accept configuration in v1")

    warnings = []
    burst_count_by_individual: dict[str, int] = {}
    scored_by_individual: dict[str, list[tuple[dict, float]]] = {}
    skipped_without_individual = 0
    skipped_without_score = 0
    for burst in scored_bursts:
        individual = str(burst.get("individual", "")).strip()
        if not individual:
            skipped_without_individual += 1
            continue
        burst_count_by_individual[individual] = burst_count_by_individual.get(individual, 0) + 1
        score = _numeric_value(burst.get("anomaly_score"))
        if score is None:
            skipped_without_score += 1
            continue
        scored_by_individual.setdefault(individual, []).append((burst, score))

    if skipped_without_individual:
        warnings.append(
            f"Skipped {skipped_without_individual} burst row(s) without an individual identifier."
        )
    if skipped_without_score:
        warnings.append(
            f"Skipped {skipped_without_score} burst row(s) without a finite anomaly_score."
        )

    ranked_individuals = []
    for individual, scored_rows in scored_by_individual.items():
        ranked_bursts = sorted(
            scored_rows,
            key=lambda item: (
                -item[1],
                str(item[0].get("set_name", "")),
                str(item[0].get("burst_id", "")),
                int(item[0].get("start_time_ms", 0)),
            ),
        )
        ranked_burst_refs = [
            _burst_reference(burst, score) for burst, score in ranked_bursts
        ]
        top_burst = ranked_burst_refs[0]
        ranked_individuals.append(
            {
                "individual": individual,
                "top_burst_id": top_burst["burst_id"],
                "top_burst_score": top_burst["anomaly_score"],
                "burst_count": burst_count_by_individual[individual],
                "scored_burst_count": len(ranked_burst_refs),
                "ranked_burst_refs": ranked_burst_refs,
            }
        )

    ranked_individuals.sort(
        key=lambda row: (
            -row["top_burst_score"],
            row["individual"],
            str(row["ranked_burst_refs"][0].get("set_name", "")),
            row["top_burst_id"],
        )
    )
    for rank, row in enumerate(ranked_individuals, start=1):
        row["rank"] = rank

    if not ranked_individuals:
        warnings.append("No scored automatic bursts are available for individual ranking.")
    return {
        "run_status": "completed" if ranked_individuals else "unresolved",
        "ranking_scope": "individual",
        "ranking_method": "maximum_burst_anomaly_score",
        "input_burst_count": len(scored_bursts),
        "scored_burst_count": sum(
            row["scored_burst_count"] for row in ranked_individuals
        ),
        "individual_count": len(ranked_individuals),
        "warnings": warnings,
        "ranked_individuals": ranked_individuals,
    }
