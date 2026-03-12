# Execution Contract

The execution harness is generic and intentionally small.

## Supported Script Kind

The current scaffold supports:

- `python`

The agent may call libraries or external tools from Python as needed.

## Environment Variables

Scripts receive:

- `VIBECLEANING_SPEC_PATH`
- `VIBECLEANING_SUMMARY_PATH`

## Step Spec Shape

```json
{
  "mode": "step",
  "project_name": "example",
  "project_dir": "/abs/path/to/data/example",
  "parent_dataset": {...},
  "input_artifacts": [
    {
      "logical_name": "raw.bin",
      "path": "/abs/path/to/data/example/raw.bin",
      "content_type": "application/octet-stream",
      "metadata": {}
    }
  ],
  "output_artifacts": [
    {
      "logical_name": "derived.json",
      "path": "/abs/path/to/data/example/.vibecleaning/outputs/dataset_x/derived.json"
    }
  ],
  "step": {
    "step_id": "step_...",
    "title": "Derive JSON summary",
    "user": "agent user",
    "parameters": {},
    "remove_artifacts": []
  }
}
```

## Analysis Spec Shape

```json
{
  "mode": "analysis",
  "project_name": "example",
  "project_dir": "/abs/path/to/data/example",
  "dataset": {...},
  "input_artifacts": [...],
  "output_artifacts": [...],
  "analysis": {
    "analysis_id": "analysis_...",
    "title": "Inspect raw payload",
    "user": "agent user",
    "parameters": {}
  }
}
```

## Script Responsibilities

- read the spec from `VIBECLEANING_SPEC_PATH`
- write any declared output artifacts to the declared paths
- write a machine-readable summary to `VIBECLEANING_SUMMARY_PATH`
- fail with a non-zero exit code when the requested work cannot be completed safely

## Framework Responsibilities

- persist script, spec, and summary files
- verify output artifacts exist before adding them to the resulting dataset
- reuse unchanged parent artifacts by reference
- never mutate raw inputs
