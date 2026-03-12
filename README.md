# Vibecleaning

Vibecleaning is a minimal, agent-centered framework for local data work backed by a reproducible dataset graph.

The framework intentionally does not assume CSVs, tables, or domain-specific transforms. Its core responsibilities are:

- persist project state under `data/<project>/.vibecleaning/`
- represent dataset lineage as dataset nodes plus step edges
- run agent-authored analysis and step scripts with a stable execution contract
- expose generic read APIs for projects, datasets, graph traversal, artifacts, and previews
- mount bespoke browser plugins that consume those generic APIs

The included frontend is intentionally small. It only loads plugins. The included example plugin at `plugins/examples/trajectories/` is read-only and demonstrates how a domain-specific interface can sit on top of the generic DAG/artifact APIs.

## Run

```bash
python server.py
```

The default URL is `http://127.0.0.1:8420`.

## Layout

```text
app/                  backend state and execution harness
static/               minimal plugin host
plugins/              example plugins and plugin manifest
data/<project>/       raw project inputs plus .vibecleaning metadata
docs/                 agent-facing architecture and contract docs
```

## Key Concepts

- `analysis`: exploratory execution that records code, spec, and summary without creating a new dataset node
- `step`: persistent execution that creates a new dataset node in the graph
- `logical_name`: dataset-level artifact identifier, separate from physical storage path
- `current_dataset_id`: the active head for a project; the full lineage can still branch

See [docs/ARCHITECTURE.md](/Users/justinkay/vibecleaning/docs/ARCHITECTURE.md), [docs/STATE_MODEL.md](/Users/justinkay/vibecleaning/docs/STATE_MODEL.md), and [AGENTS.md](/Users/justinkay/vibecleaning/AGENTS.md) for the normative contracts.
