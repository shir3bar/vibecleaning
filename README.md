# Vibecleaning

Vibecleaning is a small scaffold for local data apps backed by a reproducible dataset DAG.

The default workflow is:

1. Put input files in `data/<project>/`.
2. Run `python server.py`.
3. Ask Codex to extend the starter app in `server.py` and `static/`.
4. Use `examples/trajectory/` only as a reference.

Key rules:

- Top-level non-hidden files under `data/<project>/` become the initial dataset automatically.
- Raw input files are immutable.
- `analysis` records exploratory work and does not create a dataset.
- `step` creates a new dataset node.
- Domain logic belongs in the starter app or an example app, not in `app/`.

Repository layout:

```text
app/                  generic lineage, execution, preview, and HTTP internals
server.py             default starter app server
static/               default starter app frontend
examples/trajectory/  richer reference app
data/<project>/       project inputs plus .vibecleaning state
docs/                 minimal contracts for agents
```

Reference example:

```bashv1
python examples/trajectory/server.py
```

Contracts:

- [AGENTS.md](AGENTS.md)
- [docs/ARCHITECTURE.md](ARCHITECTURE.md)
- [docs/STATE_MODEL.md](STATE_MODEL.md)
- [docs/EXECUTION_CONTRACT.md](EXECUTION_CONTRACT.md)
