from examples.movement.burst_feature_matrix import (
    DEFAULT_FEATURE_SET,
    FEATURE_SET_EXCLUSION_CONTEXT,
    FEATURE_SET_MOVEMENT_ONLY,
    FEATURE_SET_MOVEMENT_PLUS_CONTEXT,
    SUPPORTED_FEATURE_SETS,
    numeric_value as _numeric_value,
    prepare_burst_feature_matrix,
)

DEFAULT_MODEL_CONFIG = {
    "n_estimators": 1000,
    "contamination": "auto",
    "random_state": 0,
}
MIN_BURST_ROWS = 2
EXPLANATION_METHOD = "observed_empirical_quantile_among_scored_bursts"
EXPLANATION_VALUE_SOURCE = "observed_burst_feature_values"
MISSING_VALUE_DISPLAY = "NA"
MODEL_IMPUTATION_NOTE = (
    "Median imputation is used only for Isolation Forest fitting/scoring, "
    "not as an observed explanation value."
)
EXPLANATION_LIST_LIMIT = 3
RANKING_AGGREGATION_MAX = "maximum_anomaly_score"
RANKING_AGGREGATION_MARGIN_SUM = "sum_outlier_margin"
RANKING_AGGREGATION_SCORE_SUM = "sum_anomaly_score"
SUPPORTED_RANKING_AGGREGATIONS = {
    RANKING_AGGREGATION_MAX,
    RANKING_AGGREGATION_MARGIN_SUM,
    RANKING_AGGREGATION_SCORE_SUM,
}


def _normalize_config(config: dict | None) -> dict:
    allowed_keys = {*DEFAULT_MODEL_CONFIG, "feature_set"}
    if config is None:
        return {
            "model_config": dict(DEFAULT_MODEL_CONFIG),
            "feature_set": DEFAULT_FEATURE_SET,
        }
    unsupported = sorted(set(config) - allowed_keys)
    if unsupported:
        raise ValueError(f"Unsupported anomaly ranking config values: {unsupported}")
    feature_set = str(config.get("feature_set", DEFAULT_FEATURE_SET)).strip()
    if feature_set not in SUPPORTED_FEATURE_SETS:
        raise ValueError(f"Unsupported anomaly ranking feature_set: {feature_set}")
    model_config = {
        key: config.get(key, default_value)
        for key, default_value in DEFAULT_MODEL_CONFIG.items()
    }
    normalized = {**DEFAULT_MODEL_CONFIG, **model_config}
    if int(normalized["n_estimators"]) <= 0:
        raise ValueError("n_estimators must be positive")
    normalized["n_estimators"] = int(normalized["n_estimators"])
    return {
        "model_config": normalized,
        "feature_set": feature_set,
    }


def _display_numeric(value: float) -> str:
    return format(float(value), ".6g")


def _empirical_percentile(value: float, sorted_values: list[float]) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return 50.0
    matching_indexes = [
        index for index, candidate in enumerate(sorted_values) if candidate == value
    ]
    if not matching_indexes:
        return None
    average_rank = sum(matching_indexes) / len(matching_indexes)
    return float((average_rank / (len(sorted_values) - 1)) * 100.0)


def _feature_direction(value: float, feature_median: float) -> str:
    if value > feature_median:
        return "high"
    if value < feature_median:
        return "low"
    return "typical"


def _build_observed_quantile_explanations(
    burst_rows: list[dict],
    *,
    fitted_features: list[str],
    feature_medians: dict[str, float],
) -> list[dict]:
    observed_values_by_feature = {
        field: sorted(
            numeric
            for row in burst_rows
            if (numeric := _numeric_value(row.get(field))) is not None
        )
        for field in fitted_features
    }

    explained_rows = []
    for row in burst_rows:
        feature_quantiles = []
        high_features = []
        low_features = []
        missing_features = []

        for field in fitted_features:
            feature_median = feature_medians[field]
            observed_value = _numeric_value(row.get(field))
            if observed_value is None:
                item = {
                    "feature": field,
                    "value": None,
                    "display_value": MISSING_VALUE_DISPLAY,
                    "percentile": None,
                    "median": feature_median,
                    "direction": "missing",
                    "imputed_for_model": True,
                }
                feature_quantiles.append(item)
                missing_features.append(item)
                continue

            percentile = _empirical_percentile(
                observed_value,
                observed_values_by_feature[field],
            )
            item = {
                "feature": field,
                "value": observed_value,
                "display_value": _display_numeric(observed_value),
                "percentile": percentile,
                "median": feature_median,
                "direction": _feature_direction(observed_value, feature_median),
                "imputed_for_model": False,
            }
            feature_quantiles.append(item)
            if item["direction"] == "high":
                high_features.append(item)
            elif item["direction"] == "low":
                low_features.append(item)

        high_features.sort(
            key=lambda item: (
                -(item["percentile"] if item["percentile"] is not None else -1.0),
                item["feature"],
            )
        )
        low_features.sort(
            key=lambda item: (
                item["percentile"] if item["percentile"] is not None else 101.0,
                item["feature"],
            )
        )
        missing_features.sort(key=lambda item: item["feature"])

        explained_rows.append(
            {
                **row,
                "feature_quantiles": feature_quantiles,
                "top_high_quantile_features": high_features[:EXPLANATION_LIST_LIMIT],
                "top_low_quantile_features": low_features[:EXPLANATION_LIST_LIMIT],
                "missing_features": missing_features,
            }
        )

    return explained_rows


def score_bursts(feature_rows: list[dict], config: dict | None = None) -> dict:
    """Score burst feature rows without creating fix- or individual-level outputs."""
    # Keep scikit-learn (and its compiled SciPy dependency) isolated to the
    # Isolation Forest provider. Source is_outlier ranking imports this module
    # only for the shared individual-ranking presentation and does not need the
    # model stack.
    from sklearn.ensemble import IsolationForest

    normalized_config = _normalize_config(config)
    model_config = normalized_config["model_config"]
    feature_set = normalized_config["feature_set"]
    burst_rows = [dict(row) for row in feature_rows]
    prepared = prepare_burst_feature_matrix(
        burst_rows,
        feature_set=feature_set,
        standardize=False,
    )
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
        "feature_set": feature_set,
        "preprocessing": {
            "scaling": "none",
            "missing_value_strategy": "median_imputation_per_fitted_feature",
            "nonfinite_value_handling": "treated_as_missing",
            "feature_exclusions": ["metadata", "feature_set", "nonnumeric", "all_null", "constant"],
        },
        "input_burst_count": len(burst_rows),
        "scored_burst_count": 0,
        "requested_features": prepared["requested_features"],
        "candidate_model_features": prepared["candidate_model_features"],
        "excluded_by_feature_set": prepared["excluded_by_feature_set"],
        "fitted_features": prepared["fitted_features"],
        "dropped_features": prepared["dropped_features"],
        "excluded_metadata": prepared["excluded_metadata"],
        "feature_medians": prepared["feature_medians"],
        "imputed_value_counts": prepared["imputed_value_counts"],
        "explanation_method": EXPLANATION_METHOD,
        "explanation_value_source": EXPLANATION_VALUE_SOURCE,
        "missing_value_display": MISSING_VALUE_DISPLAY,
        "model_imputation_note": MODEL_IMPUTATION_NOTE,
        "warnings": warnings,
        "scored_bursts": burst_rows,
    }
    if len(burst_rows) < MIN_BURST_ROWS or not prepared["fitted_features"]:
        return result

    model = IsolationForest(**model_config)
    model.fit(prepared["matrix"])
    sample_scores = model.score_samples(prepared["matrix"])
    decision_scores = model.decision_function(prepared["matrix"])
    anomaly_scores = -sample_scores
    result["score_offset"] = float(model.offset_)
    result["decision_boundary"] = 0.0
    result["run_status"] = "completed"
    result["scored_burst_count"] = len(burst_rows)
    scored_rows = [
        {
            **row,
            "anomaly_score": float(anomaly_score),
            "decision_function": float(decision_score),
            "outlier_margin": float(max(0.0, -decision_score)),
            "is_model_outlier": bool(decision_score < 0.0),
        }
        for row, anomaly_score, decision_score in zip(
            burst_rows,
            anomaly_scores,
            decision_scores,
            strict=True,
        )
    ]
    result["scored_bursts"] = _build_observed_quantile_explanations(
        scored_rows,
        fitted_features=prepared["fitted_features"],
        feature_medians=prepared["feature_medians"],
    )
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
        "fix_count",
        "n_fixes",
        "fix_keys",
        "decision_function",
        "outlier_margin",
        "is_model_outlier",
        "is_outlier_count",
        "top_high_quantile_features",
        "top_low_quantile_features",
        "missing_features",
    ):
        if field in row:
            reference[field] = list(row[field]) if field == "fix_keys" else row[field]
    return reference


def rank_individuals(scored_bursts: list[dict], config: dict | None = None) -> dict:
    """Rank individuals using an explicit aggregation of their burst scores."""
    normalized_config = dict(config or {})
    unsupported = sorted(set(normalized_config) - {"aggregation"})
    if unsupported:
        raise ValueError(f"Unsupported individual ranking config values: {unsupported}")
    aggregation = str(
        normalized_config.get("aggregation") or RANKING_AGGREGATION_MAX
    ).strip()
    if aggregation not in SUPPORTED_RANKING_AGGREGATIONS:
        raise ValueError(f"Unsupported individual ranking aggregation: {aggregation}")

    warnings = []
    burst_count_by_individual: dict[str, int] = {}
    scored_by_individual: dict[str, list[tuple[dict, float, float]]] = {}
    skipped_without_individual = 0
    skipped_without_score = 0
    skipped_without_aggregate = 0
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
        if aggregation == RANKING_AGGREGATION_MARGIN_SUM:
            aggregate_value = _numeric_value(burst.get("outlier_margin"))
            if aggregate_value is None:
                skipped_without_aggregate += 1
                continue
            aggregate_value = max(0.0, aggregate_value)
        else:
            aggregate_value = score
        scored_by_individual.setdefault(individual, []).append(
            (burst, score, aggregate_value)
        )

    if skipped_without_individual:
        warnings.append(
            f"Skipped {skipped_without_individual} burst row(s) without an individual identifier."
        )
    if skipped_without_score:
        warnings.append(
            f"Skipped {skipped_without_score} burst row(s) without a finite anomaly_score."
        )
    if skipped_without_aggregate:
        warnings.append(
            f"Skipped {skipped_without_aggregate} burst row(s) without a finite outlier_margin."
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
            _burst_reference(burst, score) for burst, score, _aggregate in ranked_bursts
        ]
        top_burst = ranked_burst_refs[0]
        individual_score = (
            top_burst["anomaly_score"]
            if aggregation == RANKING_AGGREGATION_MAX
            else sum(item[2] for item in ranked_bursts)
        )
        contributing_burst_count = sum(
            1 for _burst, _score, aggregate_value in ranked_bursts
            if aggregate_value > 0.0
        )
        ranked_individuals.append(
            {
                "individual": individual,
                "individual_score": float(individual_score),
                "top_burst_id": top_burst["burst_id"],
                "top_burst_score": top_burst["anomaly_score"],
                "contributing_burst_count": contributing_burst_count,
                "burst_count": burst_count_by_individual[individual],
                "scored_burst_count": len(ranked_burst_refs),
                "ranked_burst_refs": ranked_burst_refs,
            }
        )

    ranked_individuals.sort(
        key=lambda row: (
            -row["individual_score"],
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
    ranking_method = {
        RANKING_AGGREGATION_MAX: "maximum_burst_anomaly_score",
        RANKING_AGGREGATION_MARGIN_SUM: "sum_positive_outlier_decision_margin",
        RANKING_AGGREGATION_SCORE_SUM: "sum_burst_anomaly_score",
    }[aggregation]
    return {
        "run_status": "completed" if ranked_individuals else "unresolved",
        "ranking_scope": "individual",
        "ranking_method": ranking_method,
        "aggregation": aggregation,
        "input_burst_count": len(scored_bursts),
        "scored_burst_count": sum(
            row["scored_burst_count"] for row in ranked_individuals
        ),
        "individual_count": len(ranked_individuals),
        "warnings": warnings,
        "ranked_individuals": ranked_individuals,
    }
