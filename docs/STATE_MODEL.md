# State Model

Each project may contain:

```text
data/<project>/
  raw top-level input files
  .vibecleaning/
    project.json
    datasets/<dataset_id>.json
    analyses/<analysis_id>/
      analysis.json
      analysis.py
      spec.json
      summary.json
      outputs/<artifact files>
    steps/<step_id>/
      step.json
      transform.py
      spec.json
      summary.json
    outputs/<dataset_id>/<artifact files>
```

## project.json

```json
{
  "project_name": "example",
  "root_dataset_id": "dataset_...",
  "current_dataset_id": "dataset_...",
  "created_at": "2026-03-12T12:00:00+00:00",
  "updated_at": "2026-03-12T12:00:00+00:00"
}
```

## Dataset Manifest

```json
{
  "dataset_id": "dataset_...",
  "user": "system",
  "created_at": "2026-03-12T12:00:00+00:00",
  "parent_dataset_id": null,
  "note": "Initial dataset from project inputs",
  "artifacts": [
    {
      "logical_name": "example.bin",
      "path": "example.bin",
      "storage_type": "raw",
      "size": 1234,
      "content_type": "application/octet-stream",
      "metadata": {}
    }
  ]
}
```

`logical_name` is the dataset-level artifact identifier. It is how steps, analyses, and plugins refer to an artifact inside a dataset bundle.

## Step Record

```json
{
  "step_id": "step_...",
  "user": "agent user",
  "title": "Normalize payload archive",
  "kind": "python",
  "created_at": "2026-03-12T12:00:00+00:00",
  "parent_dataset_id": "dataset_...",
  "output_dataset_id": "dataset_...",
  "script_path": ".vibecleaning/steps/step_.../transform.py",
  "spec_path": ".vibecleaning/steps/step_.../spec.json",
  "summary_path": ".vibecleaning/steps/step_.../summary.json",
  "input_artifacts": ["archive.zip"],
  "output_artifacts": ["archive_manifest.json"],
  "removed_artifacts": [],
  "set_as_head": true,
  "summary": {}
}
```

## Analysis Record

```json
{
  "analysis_id": "analysis_...",
  "dataset_id": "dataset_...",
  "user": "agent user",
  "title": "Inspect archive manifest",
  "kind": "python",
  "created_at": "2026-03-12T12:00:00+00:00",
  "script_path": ".vibecleaning/analyses/analysis_.../analysis.py",
  "spec_path": ".vibecleaning/analyses/analysis_.../spec.json",
  "summary_path": ".vibecleaning/analyses/analysis_.../summary.json",
  "input_artifacts": ["archive.zip"],
  "output_artifacts": ["archive_summary.json"]
}
```
