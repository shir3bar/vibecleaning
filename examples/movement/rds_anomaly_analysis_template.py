import hashlib
import json
import os
from pathlib import Path


OUTPUT_ARTIFACT_NAME = "burst_anomaly_ranking.json"
INPUT_ATTACHMENT_NAME = "rds_burst_features.json"
SUMMARY_BURST_REF_LIMIT = 3


def _declared(spec: dict, section: str, logical_name: str) -> dict | None:
    return next(
        (item for item in spec.get(section, []) if item.get("logical_name") == logical_name),
        None,
    )


def main():
    from examples.movement.anomaly_ranking import rank_individuals, score_bursts

    spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text(encoding="utf-8"))
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    params = dict(spec["analysis"].get("parameters") or {})
    attachment = _declared(spec, "input_attachments", INPUT_ATTACHMENT_NAME)
    output = _declared(spec, "output_artifacts", OUTPUT_ARTIFACT_NAME)
    if attachment is None or output is None:
        raise SystemExit("RDS burst feature input or ranking output was not declared")
    attachment_bytes = Path(attachment["path"]).read_bytes()
    if hashlib.sha256(attachment_bytes).hexdigest() != attachment.get("sha256"):
        raise SystemExit("RDS burst feature attachment checksum mismatch")
    source = json.loads(attachment_bytes)
    feature_rows = list(source.get("feature_rows") or [])
    scoring = score_bursts(
        feature_rows,
        config={"feature_set": str(params.get("feature_set") or "movement_only")},
    )
    ranking_method = str(
        params.get("ranking_method") or "isolation_forest"
    ).strip()
    ranking_aggregation = (
        "sum_outlier_margin"
        if ranking_method == "isolation_forest_decision_margin"
        else "maximum_anomaly_score"
    )
    ranking = rank_individuals(
        scoring["scored_bursts"],
        config={"aggregation": ranking_aggregation},
    )
    warnings = [*scoring["warnings"], *ranking["warnings"]]
    result = {
        "run_status": scoring["run_status"] if scoring["run_status"] != "completed" else ranking["run_status"],
        "ranking_method": ranking_method,
        "source_bundle_signature": source.get("source_bundle_signature"),
        "burst_definition_signature": source.get("burst_definition_signature"),
        "review_exclusion_signature": source.get("review_exclusion_signature"),
        "burst_gap": {"mode": "source", "source": "burst_"},
        "confirmed_exclusion_count": int(source.get("confirmed_exclusion_count") or 0),
        "burst_feature_count": len(feature_rows),
        "model_fit": {
            **{key: value for key, value in scoring.items() if key != "scored_bursts"},
            "ranking_method": ranking_method,
        },
        "individual_ranking_summary": {key: value for key, value in ranking.items() if key != "ranked_individuals"},
        "scored_bursts": scoring["scored_bursts"],
        "ranked_individuals": ranking["ranked_individuals"],
        "warnings": warnings,
    }
    Path(output["path"]).write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    response = {key: value for key, value in result.items() if key not in {"scored_bursts", "ranked_individuals"}}
    response["ranked_individuals"] = [
        {
            **{key: row.get(key) for key in (
                "rank", "individual", "individual_score", "top_burst_score",
                "top_burst_id", "contributing_burst_count", "burst_count",
                "scored_burst_count",
            )},
            "ranked_burst_refs": [
                {**ref, "individual": row.get("individual")}
                for ref in (row.get("ranked_burst_refs") or [])[:SUMMARY_BURST_REF_LIMIT]
            ],
        }
        for row in result["ranked_individuals"]
    ]
    summary_path.write_text(json.dumps(response, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
