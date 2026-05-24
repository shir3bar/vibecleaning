import json
import os
import sys
from pathlib import Path


def add_repo_root(project_dir: Path):
    for candidate in [project_dir, *project_dir.parents]:
        if (candidate / "examples" / "movement" / "candidate_queries.py").exists():
            sys.path.insert(0, str(candidate))
            return


def main():
    spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text())
    params = dict(spec["analysis"].get("parameters") or {})
    repo_root = str(params.get("repo_root") or "").strip()
    if repo_root:
        sys.path.insert(0, repo_root)
    else:
        add_repo_root(Path(spec["project_dir"]))

    from examples.movement.candidate_queries import (
        run_candidate_query,
        unresolved_candidate_query_result,
    )

    target_artifact = str(params.get("target_artifact") or "").strip()
    query_definition = dict(params.get("query_definition") or {})
    query_parameters = dict(params.get("query_parameters") or {})
    execution_scope = params.get("execution_scope")
    preview_limit = params.get("preview_limit")
    dataset_id = str(spec.get("dataset", {}).get("dataset_id") or params.get("dataset_id") or "").strip()

    source = None
    for artifact in spec.get("input_artifacts", []):
        if artifact.get("logical_name") == target_artifact:
            source = artifact
            break

    output = None
    for artifact in spec.get("output_artifacts", []):
        if artifact.get("logical_name") == "candidate_query_results.json":
            output = artifact
            break

    if output is None:
        raise SystemExit("Candidate query output artifact was not declared")

    if source is None:
        result = unresolved_candidate_query_result(
            query_definition,
            query_parameters,
            dataset_id=dataset_id,
            logical_name=target_artifact,
            execution_scope=execution_scope,
            unresolved_fields=[target_artifact or "target_artifact"],
            warnings=["Target artifact was not provided as an input."],
        )
    else:
        result = run_candidate_query(
            Path(source["path"]),
            query_definition=query_definition,
            parameters=query_parameters,
            dataset_id=dataset_id,
            logical_name=target_artifact,
            preview_limit=preview_limit,
            execution_scope=execution_scope,
        )

    output_path = Path(output["path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
