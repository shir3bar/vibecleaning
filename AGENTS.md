# Vibecleaning Agent Guide

Default behavior:

- Extend the starter app at `server.py` and `static/`.
- Use `examples/trajectory/` only as a reference or when the user explicitly wants a separate app.
- Keep `app/` generic.

Project model:

- A project lives under `data/<project>/`.
- Top-level non-hidden files are the raw inputs and become the initial dataset.
- Vibecleaning state lives under `data/<project>/.vibecleaning/`.
- A dataset is a bundle of artifacts addressed by `logical_name`.

Valid DAG rules:

- Never mutate raw files in `data/<project>/`.
- Never write lineage state outside `.vibecleaning/`.
- Use an `analysis` for exploratory work.
- Use a `step` only for a persistent change.
- Every analysis and step must persist `user`, script, spec, and summary.
- A step must create a new dataset node.
- Reuse unchanged artifacts by reference; only materialize changed or new artifacts.
- Keep app-specific meaning in `artifact.metadata`, `step.parameters`, `step.summary`, or analysis outputs.

Where to put code:

- Generic lineage or execution behavior: `app/`
- Starter app routes: `server.py`
- Starter app UI: `static/`
- Reference app code: `examples/<app>/`

Backend wiring:

- Frontends should call the generic `/api/...` routes directly when possible.
- If the UI needs a domain-specific summary or named action, add an app-owned route in the starter app or example app.
- App-owned routes should translate persistent work into `create_analysis(...)` or `create_step(...)`, not modify lineage files directly.

OpenStreetMap (OSM) development:
- All OSM-related work should use the shared OSM interaction layer.
- The OSM layer is generic infrastructure for spatial context, not a cleaning-rule engine.
- OSM context is ephemeral by default and must not touch review, flagging, or lineage state unless explicitly requested.
- Future features should define selectors and spatial scope, call queryOsmContext or the backend route, and render temporary overlays through existing map patterns.
- Broad exists queries must remain constrained and should use explicit element_types.
- Use scope_signature/query_signature to detect stale overlays when relevant.
- OSM results may omit unsupported relations or partial geometries; callers should surface metadata when useful.
- Do not hard-code biological conclusions from OSM matches.
