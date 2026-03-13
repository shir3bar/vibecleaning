# Vibecleaning

Vibecleaning is a minimal substrate for agent-authored local data apps backed by a reproducible dataset DAG.

The repository is organized around three layers:

- `app/`: scaffold internals for lineage, execution, previews, and generic HTTP routes
- `server.py` + `static/`: the default starter app that users are expected to extend in place
- `examples/`: richer reference apps that show what can be built on top of the scaffold

## Default Workflow

1. Install dependencies.

```bash
pip install -r requirements.txt
```

2. Create a project under `data/<project>/` and add top-level input files there.
3. Run the starter app.

```bash
python server.py
```

4. Open Codex in this repo and describe the app or transformation you want.
5. By default, extend the root starter files:
   `server.py`, `static/index.html`, and `static/app.js`.

On first use of a project, the top-level non-hidden files under `data/<project>/` become the initial dataset automatically.

## Reference Example

This repository also ships one richer example app:

```bash
python examples/trajectory/server.py
```

The trajectory app is a reference implementation, not the default starting point. It demonstrates:

- a domain-specific frontend
- app-owned routes for large-file summaries
- a named UI action, `Delete checked`, that compiles down to a generic DAG step
- direct use of generic core controls like `Undo`

## Layout

```text
app/                  scaffold internals
server.py             default starter app entrypoint
static/               default starter app frontend
examples/trajectory/  richer reference app
data/<project>/       raw inputs plus .vibecleaning state
docs/                 architecture and contract docs
```

## Core Concepts

- `analysis`: exploratory execution that records code, spec, and summary without creating a new dataset node
- `step`: persistent execution that creates a new dataset node in the graph
- `logical_name`: dataset-level artifact identifier, separate from physical storage path
- `current_dataset_id`: the active head for a project; older and alternate datasets still remain in the DAG

See [docs/ARCHITECTURE.md](/Users/justinkay/vibecleaning/docs/ARCHITECTURE.md), [docs/APP_CONTRACT.md](/Users/justinkay/vibecleaning/docs/APP_CONTRACT.md), [docs/STATE_MODEL.md](/Users/justinkay/vibecleaning/docs/STATE_MODEL.md), and [AGENTS.md](/Users/justinkay/vibecleaning/AGENTS.md) for the repo contracts.
