# Vibecleaning

Vibecleaning is a minimal, agent-centered framework for local data work backed by a reproducible dataset graph.

The repository is split into:

- a generic core scaffold
- example or application overlays layered on top of that scaffold

The core intentionally does not assume CSVs, tables, or any domain-specific transforms. Its responsibilities are:

- persist project state under `data/<project>/.vibecleaning/`
- represent dataset lineage as dataset nodes plus step edges
- run agent-authored analysis and step scripts with a stable execution contract
- expose generic project, dataset, artifact, and preview APIs
- mount browser plugins through a thin plugin host

Applications built on top of the scaffold are expected to modify data. They may add domain-specific backend routes and frontend components, but that logic should live outside the core.

This repository ships a trajectory example overlay under `examples/trajectory/`. It demonstrates:

- a server-side summary route for large CSVs
- a domain-specific frontend built on top of the generic host
- a concrete UI action, `Delete checked`, that creates a DAG step through the generic execution harness

## Run

```bash
python server.py
```

This starts the generic host. It intentionally does not ship example plugins by default.

```bash
python examples/trajectory/server.py
```

This starts the trajectory example app.

The default URL is `http://127.0.0.1:8420` in both cases.

## Layout

```text
app/                  generic backend state, execution, previews, and app factory
static/               generic plugin host
plugins/              generic host plugin manifest
examples/             example application overlays
data/<project>/       raw project inputs plus .vibecleaning metadata
docs/                 architecture and contract docs
```

## Key Concepts

- `analysis`: exploratory execution that records code, spec, and summary without creating a new dataset node
- `step`: persistent execution that creates a new dataset node in the graph
- `logical_name`: dataset-level artifact identifier, separate from physical storage path
- `current_dataset_id`: the active head for a project; the full lineage can still branch
- application overlay: a domain-specific package that imports the core scaffold, adds routes and plugins, and compiles UI actions down to generic `analysis` or `step` executions

See [docs/ARCHITECTURE.md](/Users/justinkay/vibecleaning/docs/ARCHITECTURE.md), [docs/STATE_MODEL.md](/Users/justinkay/vibecleaning/docs/STATE_MODEL.md), and [AGENTS.md](/Users/justinkay/vibecleaning/AGENTS.md) for the normative contracts.
