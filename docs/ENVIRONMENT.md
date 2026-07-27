# Environment setup

Vibecleaning is a Python application. It has no Node/npm build step; the map
library and frontend assets used by the examples are served from the repository.

Python 3.11 is the reference version in `environment.yml`. Newer Python
versions may work, but use 3.11 when diagnosing environment-specific failures.

## Option 1: uv (recommended)

Install `uv` once if it is not already available:

```bash
python -m pip install uv
```

From the repository root, create the locked environment:

```bash
uv sync --locked
```

`uv` reads `.python-version`, creates `.venv/`, and installs the application
dependencies plus the default `dev` dependency group from `pyproject.toml`.
Run commands without manually activating the environment:

```bash
uv run python examples/move_viz/server.py
uv run pytest -q tests/test_move_viz.py
```

Use these commands when changing dependencies:

```bash
uv add package-name
uv add --dev package-name
uv lock
uv sync --locked
```

Commit `pyproject.toml` and `uv.lock` together. CI and handoff verification
should use `uv sync --locked`; do not regenerate the lockfile incidentally.
`pyproject.toml` is the dependency source of truth for uv. When maintaining the
legacy pip workflow, mirror direct dependency additions or removals in
`requirements.txt` as well.

The uv lock covers Python dependencies used by this repository. It does not
replace Conda when work specifically requires system-level GDAL or HDF4
libraries.

## Option 2: Python virtual environment

From the repository root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

On Windows PowerShell, activate with:

```powershell
.venv\Scripts\Activate.ps1
```

This installs the application, testing, movement-analysis, and OSM dependencies.
The packages are not currently lock-file pinned, so record the resolved
environment with `python -m pip freeze` when exact reproduction is required.

## Option 3: Conda geospatial environment

The tracked Conda environment provides Python and the heavier geospatial base.
The Python application requirements are installed as a second explicit step:

```bash
conda env create -f environment.yml
conda activate movebench
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The environment is named `movebench` for historical reasons. Do not assume the
Conda file alone installs FastAPI, pytest, or the movement-analysis packages.

## Minimal move_viz environment

To run and test only the lightweight SQLite viewer, the smallest practical
environment is:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install fastapi uvicorn httpx pytest
```

SQLite support comes from Python's standard library. The `sqlite3` command-line
program is useful for inspecting a new database but is not required to run the
app.

Start the viewer with:

```bash
python examples/move_viz/server.py
```

Then open `http://127.0.0.1:8422`. Basemap tiles require network access; the
SQLite review and data graph remain local.

## Verify the installation

For `move_viz`:

```bash
uv sync --locked
uv run python -c "import sqlite3, fastapi, uvicorn; print(sqlite3.sqlite_version)"
uv run python -m compileall -q examples/move_viz tests/test_move_viz.py
uv run pytest -q tests/test_move_viz.py
```

Or, inside an activated venv/Conda environment:

```bash
python -c "import sqlite3, fastapi, uvicorn; print(sqlite3.sqlite_version)"
python -m compileall -q examples/move_viz tests/test_move_viz.py
pytest -q tests/test_move_viz.py
```

For the broader movement regression set:

```bash
pytest -q \
  tests/test_move_viz.py \
  tests/test_slim_movement.py \
  tests/test_movement_fixes.py \
  tests/test_burst_features.py \
  tests/test_candidate_queries.py
```

Run the complete suite with `pytest -q`. Import errors for `numpy`, `pandas`,
`sklearn`, `shapely`, or `osmium` mean the full `requirements.txt` installation
was skipped or failed.

## Configuration

The example servers accept `HOST` and `PORT`. `slim_movement` refuses to start
unless both of these are set:

```text
SLIM_MOVEMENT_USERNAME
SLIM_MOVEMENT_PASSWORD  minimum 12 characters
```

Basic authentication must be placed behind HTTPS when the app is shared beyond
localhost. See `examples/slim_movement/README.md`.

`move_viz` also recognizes:

```text
MOVE_VIZ_MAX_UPLOAD_BYTES  default 512 MiB
MOVE_VIZ_MAX_ROWS          default 100,000 rows per map page
MOVE_VIZ_MAX_REVIEW_ROWS   default 250,000 fixes per graph step
```

Example:

```bash
HOST=0.0.0.0 PORT=8422 MOVE_VIZ_MAX_ROWS=100000 \
  python examples/move_viz/server.py
```

## Environment hygiene

- Do not commit `.venv/`, Conda environments, caches, or generated kernels.
- Do not commit large generated SQLite samples unless the user explicitly asks.
- Keep local graph projects under `data/` out of source-control commits.
- Inspect `git status --short` before staging environment-related changes.
- Treat untracked Docker/devcontainer files as user-owned unless they are
  explicitly brought into scope.
