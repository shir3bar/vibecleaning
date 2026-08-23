import json
import os
from pathlib import Path


OUTPUT_ARTIFACT_NAME = "burst_anomaly_ranking.json"
SUMMARY_BURST_REF_LIMIT = 3


def _declared_artifact(spec: dict, artifact_list: str, logical_name: str) -> dict | None:
    for artifact in spec.get(artifact_list, []):
        if artifact.get("logical_name") == logical_name:
            return artifact
    return None


def main():
    spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    params = dict(spec["analysis"].get("parameters") or {})
    from examples.movement.anomaly_ranking import rank_individuals, score_bursts
    from examples.movement.burst_features import build_burst_feature_rows
    from examples.movement.review_annotations import confirmed_exclusion_scopes, load_review_annotations
    from examples.movement.summary import build_movement_fixes

    target_artifact = str(params.get("target_artifact") or "").strip()
    dataset_id = str(
        spec.get("dataset", {}).get("dataset_id") or params.get("dataset_id") or ""
    ).strip()
    source = _declared_artifact(spec, "input_artifacts", target_artifact)
    output = _declared_artifact(spec, "output_artifacts", OUTPUT_ARTIFACT_NAME)
    if output is None:
        raise SystemExit("Burst anomaly ranking output artifact was not declared")
    if source is None:
        raise SystemExit("Target movement artifact was not provided as an input")
    sidecar = _declared_artifact(spec, "input_artifacts", "movement_review_annotations.json")
    annotations = load_review_annotations(Path(sidecar["path"]) if sidecar else None)
    confirmed_fix_keys, confirmed_individual_tracks = confirmed_exclusion_scopes(
        annotations,
        source_artifact=target_artifact,
    )

    movement = build_movement_fixes(
        Path(source["path"]),
        limit=None,
        confirmed_fix_keys=confirmed_fix_keys,
        confirmed_individual_tracks=confirmed_individual_tracks,
        burst_gap_mode=params.get("burst_gap_mode"),
        burst_gap_seconds=params.get("burst_gap_seconds"),
        burst_gap_quantile=params.get("burst_gap_quantile"),
    )
    eligible_fixes = [
        fix for fix in movement["fixes"] if not fix.get("analytically_excluded")
    ]
    feature_rows = build_burst_feature_rows(eligible_fixes, movement["auto_bursts"])
    feature_set = str(params.get("feature_set") or "movement_only").strip()
    scoring = score_bursts(feature_rows, config={"feature_set": feature_set})
    ranking_method = str(
        params.get("ranking_method") or "isolation_forest"
    ).strip()
    ranking_aggregation = (
        "sum_outlier_margin"
        if ranking_method == "isolation_forest_decision_margin"
        else "maximum_anomaly_score"
    )
    individual_ranking = rank_individuals(
        scoring["scored_bursts"],
        config={"aggregation": ranking_aggregation},
    )
    warnings = [*scoring["warnings"], *individual_ranking["warnings"]]

    result = {
        "run_status": (
            scoring["run_status"]
            if scoring["run_status"] != "completed"
            else individual_ranking["run_status"]
        ),
        "ranking_method": ranking_method,
        "input_artifact": {
            "dataset_id": dataset_id,
            "logical_name": target_artifact,
            "content_type": source.get("content_type"),
        },
        "burst_gap": movement["burst_gap"],
        "confirmed_exclusion_count": len(confirmed_fix_keys),
        "burst_feature_count": len(feature_rows),
        "model_fit": {
            **{key: value for key, value in scoring.items() if key != "scored_bursts"},
            "ranking_method": ranking_method,
        },
        "individual_ranking_summary": {
            key: value
            for key, value in individual_ranking.items()
            if key != "ranked_individuals"
        },
        "scored_bursts": scoring["scored_bursts"],
        "ranked_individuals": individual_ranking["ranked_individuals"],
        "warnings": warnings,
    }
    output_path = Path(output["path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    response_summary = {
        key: value
        for key, value in result.items()
        if key not in {"scored_bursts", "ranked_individuals"}
    }
    response_summary["ranked_individuals"] = [
        {
            **{
                key: row.get(key)
                for key in (
                    "rank",
                    "individual",
                    "individual_score",
                    "top_burst_score",
                    "top_burst_id",
                    "contributing_burst_count",
                    "burst_count",
                    "scored_burst_count",
                )
            },
            "ranked_burst_refs": [
                {
                    **ref,
                    "individual": row.get("individual"),
                }
                for ref in (row.get("ranked_burst_refs") or [])[:SUMMARY_BURST_REF_LIMIT]
            ],
        }
        for row in result["ranked_individuals"]
    ]
    summary_path.write_text(
        json.dumps(response_summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
