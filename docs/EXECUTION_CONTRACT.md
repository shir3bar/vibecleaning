# Execution Contract

Supported script kind:

- `python`

Generic create-analysis request:

```json
{
  "user": "agent user",
  "title": "Inspect artifact",
  "kind": "python",
  "script": "...python...",
  "dataset_id": "dataset_...",
  "parameters": {},
  "input_artifacts": ["input.csv"],
  "output_artifacts": ["summary.json"]
}
```

Generic create-step request:

```json
{
  "user": "agent user",
  "title": "Transform artifact",
  "kind": "python",
  "script": "...python...",
  "parent_dataset_id": "dataset_...",
  "parameters": {},
  "input_artifacts": ["input.csv"],
  "output_artifacts": ["cleaned.csv"],
  "remove_artifacts": [],
  "set_as_head": true
}
```

Request rules:

- `dataset_id` and `parent_dataset_id` default to the current head if omitted.
- `input_artifacts: []` means all artifacts in the source dataset.
- A step must have at least one `output_artifact` or `remove_artifact`.
- `output_artifacts` and `remove_artifacts` must not overlap.
- `user`, `title`, `kind`, and `script` are required.

Authenticated movement requests derive both attribution fields from the session:
`user` is the legacy readable author label (the display name); `actor` is the
identity snapshot containing `user_id`, `username`, `display_name`, and `role`.
Both are retained so existing scripts can read `user` while audit records and
authorization use the stable authenticated identity. A client cannot override
either field by sending a different value in its request.

Script environment:

- `SCRUBDATA_SPEC_PATH`
- `SCRUBDATA_SUMMARY_PATH`

The runner also supplies the legacy `VIBECLEANING_SPEC_PATH` and
`VIBECLEANING_SUMMARY_PATH` names with the same values so saved scripts remain
executable without changing their recorded source.

Script responsibilities:

- Read the spec JSON from `SCRUBDATA_SPEC_PATH`.
- Write declared outputs to the declared paths.
- Write a machine-readable summary to `SCRUBDATA_SUMMARY_PATH`.
- Exit non-zero on failure.

Framework responsibilities:

- Persist script, spec, and summary files.
- Verify declared outputs exist before using them in a new dataset.
- Reuse unchanged parent artifacts by reference.
- Never mutate raw inputs.

Head actions:

- `POST /api/project/{project}/head` moves `current_dataset_id` to a chosen dataset.
- `POST /api/project/{project}/undo` moves `current_dataset_id` to the current head's parent.
- Neither action deletes datasets.
