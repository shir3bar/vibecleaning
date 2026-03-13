# Architecture

Vibecleaning is organized around five layers.

## 1. Lineage Core

The lineage core stores project state on disk under `data/<project>/.vibecleaning/`.

It owns:

- project bootstrap
- dataset nodes
- step edges
- analysis records
- the active project head
- artifact lookup by `logical_name`

It does not own domain logic.

## 2. Execution Harness

The execution harness accepts an analysis or step request, writes the stored script, spec, and summary files, runs the script, and records the result.

The harness is intentionally generic:

- no built-in CSV transforms
- no built-in column operations
- no built-in domain action catalog
- no SQL engine assumptions

The current implementation supports Python scripts as the minimal execution substrate.

## 3. Generic HTTP App

The core HTTP layer exposes reusable generic routes for:

- listing projects
- reading project state
- reading graph structure
- reading dataset manifests
- reading artifact metadata
- reading artifact bytes
- reading lightweight previews
- creating analyses
- creating steps
- changing the current head
- undoing to the parent dataset

The core app is created through an app factory in `app/web.py`.

## 4. Default Starter App

The default starter app lives at the repo root:

- `server.py`
- `static/index.html`
- `static/app.js`

This is the default thing a new user should extend with Codex.

The starter app should stay small. Its job is to provide an editable baseline UI on top of the generic APIs, not to become a second core framework.

If the starter app needs domain-specific routes or mutation actions, add them at the app layer, not in `app/`.

## 5. Reference Apps

`examples/` contains richer apps that demonstrate what can be built on top of the scaffold.

These apps may own:

- their own frontend
- their own backend routes
- large-file summary logic
- named UI actions that compile down to generic `analysis` or `step` executions

Dependency direction is one-way:

1. `app/` knows nothing about the starter app or examples
2. the starter app and examples import or call the core
3. app-specific behavior stays outside `app/`

The trajectory app under `examples/trajectory/` is the first reference app in this model.
