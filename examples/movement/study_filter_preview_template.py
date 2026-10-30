"""Saved, exploratory calculation; never changes the dataset head."""
import json
import os
from pathlib import Path


def main():
    from examples.movement.study_filters import StudyFilters
    spec = json.loads(Path(os.environ["SCRUBDATA_SPEC_PATH"]).read_text(encoding="utf-8"))
    params = spec["analysis"]["parameters"]
    context = StudyFilters(spec["project_dir"], spec["dataset"]["dataset_id"],
                           rds=params["rds"], logical_name=params["logical_name"])
    result = context.preview(params["stage"])
    for output in spec["output_artifacts"]:
        if output["logical_name"] == "study_filter_preview.json":
            Path(output["path"]).write_text(json.dumps(result, allow_nan=False) + "\n", encoding="utf-8")
    Path(os.environ["SCRUBDATA_SUMMARY_PATH"]).write_text(json.dumps({
        "stage": params["stage"], "added_count": result["added_count"], "removed_count": result["removed_count"]}), encoding="utf-8")


if __name__ == "__main__":
    main()
