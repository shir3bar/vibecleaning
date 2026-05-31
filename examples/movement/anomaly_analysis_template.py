import json
import os
import sys
from pathlib import Path


OUTPUT_ARTIFACT_NAME = "burst_anomaly_ranking.json"


def add_repo_root(project_dir: Path):
    for candidate in [project_dir, *project_dir.parents]:
        if (candidate / "examples" / "movement" / "anomaly_ranking.py").exists():
            sys.path.insert(0, str(candidate))
            return


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
    repo_root = str(params.get("repo_root") or "").strip()
    if repo_root:
        sys.path.insert(0, repo_root)
    else:
        add_repo_root(Path(spec["project_dir"]))

    from examples.movement.anomaly_ranking import rank_individuals, score_bursts
    from examples.movement.burst_features import build_burst_feature_rows
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

    movement = build_movement_fixes(
        Path(source["path"]),
        limit=None,
        burst_gap_mode=params.get("burst_gap_mode"),
        burst_gap_seconds=params.get("burst_gap_seconds"),
        burst_gap_quantile=params.get("burst_gap_quantile"),
    )
    feature_rows = build_burst_feature_rows(movement["fixes"], movement["auto_bursts"])
    scoring = score_bursts(feature_rows, config={"feature_set": "movement_only"})
    individual_ranking = rank_individuals(scoring["scored_bursts"])
    warnings = [*scoring["warnings"], *individual_ranking["warnings"]]

    result = {
        "run_status": (
            scoring["run_status"]
            if scoring["run_status"] != "completed"
            else individual_ranking["run_status"]
        ),
        "input_artifact": {
            "dataset_id": dataset_id,
            "logical_name": target_artifact,
            "content_type": source.get("content_type"),
        },
        "burst_gap": movement["burst_gap"],
        "burst_feature_count": len(feature_rows),
        "model_fit": {
            key: value for key, value in scoring.items() if key != "scored_bursts"
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
            key: row.get(key)
            for key in (
                "rank",
                "individual",
                "top_burst_score",
                "top_burst_id",
                "burst_count",
                "scored_burst_count",
            )
        }
        for row in result["ranked_individuals"]
    ]
    summary_path.write_text(
        json.dumps(response_summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
