import hashlib
import json
import os
from pathlib import Path


OUTPUT_ARTIFACT_NAME = "burst_anomaly_ranking.json"
INPUT_ATTACHMENT_NAME = "source_outlier_bursts.json"
SUMMARY_BURST_REF_LIMIT = 3


def _declared(spec: dict, section: str, logical_name: str) -> dict | None:
    return next(
        (
            item
            for item in spec.get(section, [])
            if item.get("logical_name") == logical_name
        ),
        None,
    )


def main():
    from examples.movement.anomaly_ranking import rank_individuals

    spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    attachment = _declared(spec, "input_attachments", INPUT_ATTACHMENT_NAME)
    output = _declared(spec, "output_artifacts", OUTPUT_ARTIFACT_NAME)
    if attachment is None or output is None:
        raise SystemExit("Source-outlier ranking inputs or output were not declared")
    attachment_bytes = Path(attachment["path"]).read_bytes()
    if hashlib.sha256(attachment_bytes).hexdigest() != attachment.get("sha256"):
        raise SystemExit("Source-outlier attachment checksum mismatch")
    source = json.loads(attachment_bytes)
    scored_bursts = list(source.get("scored_bursts") or [])
    ranking = rank_individuals(
        scored_bursts,
        config={"aggregation": "sum_anomaly_score"},
    )
    result = {
        "run_status": ranking["run_status"],
        "ranking_schema_version": 1,
        "ranking_provider": "source_is_outlier",
        "ranking_method": "source_is_outlier",
        "source_bundle_signature": source.get("source_bundle_signature"),
        "burst_definition_signature": source.get("burst_definition_signature"),
        "review_exclusion_signature": source.get("review_exclusion_signature"),
        "burst_feature_count": len(scored_bursts),
        "model_fit": {
            "run_status": "completed",
            "ranking_method": "source_is_outlier",
            "feature_fields": ["is_outlier_count"],
        },
        "individual_ranking_summary": {
            key: value for key, value in ranking.items() if key != "ranked_individuals"
        },
        "scored_bursts": scored_bursts,
        "ranked_individuals": ranking["ranked_individuals"],
        "warnings": list(ranking.get("warnings") or []),
    }
    output_path = Path(output["path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    response = {
        key: value
        for key, value in result.items()
        if key not in {"scored_bursts", "ranked_individuals"}
    }
    response["ranked_individuals"] = [
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
                {**ref, "individual": row.get("individual")}
                for ref in (row.get("ranked_burst_refs") or [])[:SUMMARY_BURST_REF_LIMIT]
            ],
        }
        for row in result["ranked_individuals"]
    ]
    summary_path.write_text(
        json.dumps(response, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
