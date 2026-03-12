# Trajectory Example

This example overlay demonstrates how a domain-specific application can sit on top of the generic Vibecleaning scaffold.

It adds:

- a server-side summary route for large trajectory CSVs
- a trajectory-specific plugin mounted through the generic host
- a concrete UI action, `Delete checked`, that creates a generic DAG step

## Run

```bash
python examples/trajectory/server.py
```

## Routes

- `GET /api/project/{project}/apps/trajectory/dataset/{dataset_id}/summary`
- `POST /api/project/{project}/apps/trajectory/actions/delete-checked`

## Delete Checked Flow

1. The plugin gathers the checked individuals from the current file.
2. It posts the action request to the trajectory overlay route.
3. The overlay translates that request into a generic `create_step(...)` call.
4. The generated step script streams the CSV, removes the selected individuals, and writes a replacement artifact with the same `logical_name`.
5. The core execution harness records the script, spec, summary, outputs, step record, and new dataset node.

This is the example pattern to copy for future applications: UI action in the overlay, generic persistence in the core.
