# Vibecleaning

Vibecleaning is a small scaffold for local data apps backed by a reproducible dataset DAG.

The default workflow is:

1. Put input files in `data/<project>/`.
2. Run `python server.py`.
3. Ask Codex to extend the starter app in `server.py` and `static/`.
4. Use `examples/trajectory/` only as a reference.

Environment installation:

```bash
python -m pip install uv
uv sync --locked
```

See [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md) for uv commands, standard venv,
Conda, minimal `move_viz`, verification, and configuration instructions.

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
examples/trajectory/   richer reference app
examples/movement/     full movement development playground
examples/slim_movement/ user-facing movement review app
examples/move_viz/      lightweight SQLite movement viewer
data/<project>/        project inputs plus .vibecleaning state
docs/                  minimal contracts for agents
```

Reference example:

```bash
python examples/trajectory/server.py
```

Slim movement review:

```bash
uv run python -m app.auth_cli bootstrap admin --display-name "Review Administrator"
uv run python -m app.auth_cli add reviewer1 --display-name "Taylor Reviewer" --role reviewer
uv run python examples/slim_movement/server.py
```

See [examples/slim_movement/README.md](examples/slim_movement/README.md) for
account management, reviewer/editor workflow, and secure HTTPS deployment guidance.

SQLite movement viewer:

```bash
python examples/move_viz/server.py
```

Contracts:

- [AGENTS.md](AGENTS.md)
- [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md)
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [docs/STATE_MODEL.md](docs/STATE_MODEL.md)
- [docs/EXECUTION_CONTRACT.md](docs/EXECUTION_CONTRACT.md)
- [docs/multi-user-movement-review-design.md](docs/multi-user-movement-review-design.md)
