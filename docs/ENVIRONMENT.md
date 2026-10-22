# Environment and startup reference

**Use uv for installation, startup, development and tests.** It creates and
manages the Python virtual environment; there is no separate venv activation
step. Python 3.11 is selected by `.python-version`, direct dependencies live in
`pyproject.toml`, and `uv.lock` records the resolved versions. There is no
Node/npm build step.

## Open the movement review app

Follow the guide for your computer:

- [Windows: install, create a login, start and update](START_HERE_WINDOWS.md)
- [Mac: install, create a login, start and update](START_HERE_MAC.md)
- Linux: follow the commands below after installing Git and
  [uv](https://docs.astral.sh/uv/getting-started/installation/).

The workflow is the same: clone the repository locally, install the locked app
packages, create an account once per data folder, then use the launcher each
time. The launchers synchronize dependencies before starting the server. They
select the RDS or CSV app, set the data/cache paths and check the local port.

For Linux, from the repository root, prepare the included data and account once:

```sh
export UV_PROJECT_ENVIRONMENT="$PWD/.venv"
uv sync --locked --no-dev
uv run --no-sync python -m app.auth_cli --data-root "./data" bootstrap editor
```

Start it each time with:

```sh
bash scripts/start-vibecleaning.sh --profile Rds --data-root "./data"
```

Open `http://127.0.0.1:8422`. Use `--profile Csv` for CSV review on port 8421.
Stop with Ctrl+C. To update, stop the app, run `git pull --ff-only` in the
repository, then run the launcher again. For shared storage, accounts and ZIP
releases, follow the [IT guide](IT_DEPLOYMENT_REQUIREMENTS.md).

## Where the environment lives

All of these environments are managed by uv. Their location is selected by
`UV_PROJECT_ENVIRONMENT`; it is independent of the folder containing review data.

| Use | Python environment |
| --- | --- |
| Mac/Linux launcher | `.venv` inside the local checkout or extracted release |
| Windows launcher, Git checkout | `%LOCALAPPDATA%\Vibecleaning\venvs\development` |
| Windows launcher, packaged release | `%LOCALAPPDATA%\Vibecleaning\venvs\<release-id>` |
| Development and tests on any OS | `.venv` inside the local checkout |

The Windows launcher deliberately keeps its environment under local app data.
Its quick-start guide selects that same path for account setup. A packaged
release uses the ID from `release-manifest.json`; follow the IT guide for that
case. Windows Git checkouts share the `development` environment, so stop one
checkout's app before starting another version.

Keep the application checkout, environments and caches on the local computer.
Point `--data-root` / `-DataRoot` to the data folder or mounted share. Use the
same data root for account creation and startup; otherwise the app will look
for a different user registry.

## Development and tests

Run commands from the repository root. To select this checkout's environment,
use one of these shell-specific commands, including when switching from a
Windows reviewer setup:

```sh
# Mac/Linux
export UV_PROJECT_ENVIRONMENT="$PWD/.venv"
```

```powershell
# Windows PowerShell
$env:UV_PROJECT_ENVIRONMENT = Join-Path (Get-Location).Path ".venv"
```

Then install the app and development tools on any OS:

```sh
uv sync --locked
uv run --locked python -m pytest -q tests/test_move_viz.py
```

[Running the tests](TESTING.md) covers Chromium installation and the full suite.
Development launch commands use the same environment:

| App | Command | Default port |
| --- | --- | --- |
| Generic framework starter | `uv run --locked python server.py` | 8420 |
| Full movement development playground | `uv run --locked python examples/movement/server.py` | 8420 |
| CSV review | `uv run --locked python examples/slim_movement/server.py` | 8421 |
| RDS review | `uv run --locked python examples/rds_movement/server.py` | 8422 |
| SQLite viewer | `uv run --locked python examples/move_viz/server.py` | 8422 |
| Trajectory reference | `uv run --locked python examples/trajectory/server.py` | 8420 |

Run one app per port. The movement apps require a login registry before startup;
for the repository's default `data` folder:

```sh
uv run --locked python -m app.auth_cli --data-root "./data" bootstrap editor
uv run --locked python -m app.auth_cli --data-root "./data" add reviewer1 --role reviewer
```

Run `bootstrap` only for a new registry. New display names default to normalized
usernames; `--display-name "Taylor Reviewer"` provides an explicit override.
Restart running apps after account changes. Set `VIBECLEANING_DATA_ROOT` when
launching a server directly against a different data folder; see the IT guide.

### What the uv flags mean here

- `uv sync --locked --no-dev`: install the locked app packages for reviewers.
- `uv sync --locked`: also install the development/test tools.
- `uv run --locked ...`: synchronize as needed without changing the lockfile;
  use this for development commands.
- `uv run --no-sync ...`: use an environment already synchronized by the
  preceding setup command or launcher. This keeps reviewer commands from
  installing development tools.

These flags follow uv's [locking and syncing rules](https://docs.astral.sh/uv/concepts/projects/sync/).
The Mac/Linux launcher shares the checkout's `.venv` and synchronizes with
`--no-dev`, which can remove test tools. Run `uv sync --locked` again before
returning to tests. Browser installation is only needed for browser tests, not
for opening the app in your normal browser.

### Changing dependencies

```sh
uv add package-name
uv add --dev package-name
uv sync --locked
```

Commit `pyproject.toml` and `uv.lock` together. Use `uv lock` when intentionally
updating the lock after editing dependency declarations. Routine setup and CI
must use `--locked` so an outdated lock is reported instead of silently changed.

## Existing pip, venv or Conda setups

`requirements.txt` is a legacy, unpinned dependency list. `environment.yml` is a
legacy `movebench` geospatial environment with GDAL/HDF4 tools. Neither is the
installation path for the review app, and neither defines its locked versions.
They remain for older external workflows; new app dependencies belong in
`pyproject.toml` and `uv.lock`. The SQLite viewer uses the same uv setup as the
other apps; it does not need a separate minimal environment.

To move an existing installation to uv, stop the app and open a fresh terminal
without an activated project venv or Conda environment. Install uv, select the
environment path above, and follow the appropriate setup guide. If an existing
`.venv` must be retained for another workflow, rename that environment folder
before syncing. Keep the data folder and its `scrubdata` history in place.

R and its `sf`/`move2` packages are optional for R-native RDS export; they are
separate from the Python environment. See the IT guide for deployment details.

## Configuration and housekeeping

Movement deployments use:

```text
VIBECLEANING_DATA_ROOT       authoritative projects, accounts and review history
VIBECLEANING_CACHE_ROOT      machine-local disposable caches
VIBECLEANING_SHARED_LOCKING  required (default) or disabled
```

Explicit constructor/CLI paths override environment variables, which override
platform defaults. Cache defaults are `%LOCALAPPDATA%\Vibecleaning\cache` on
Windows, `~/Library/Caches/Vibecleaning` on Mac, and
`${XDG_CACHE_HOME:-~/.cache}/vibecleaning` on Linux. Cache/account commands and
shared-folder deployment instructions live in the [IT guide](IT_DEPLOYMENT_REQUIREMENTS.md).

The servers accept `HOST` and `PORT`. The SQLite viewer also accepts
`MOVE_VIZ_MAX_UPLOAD_BYTES` (default 512 MiB), `MOVE_VIZ_MAX_ROWS` (100,000) and
`MOVE_VIZ_MAX_REVIEW_ROWS` (250,000).

Do not commit environments, caches, generated kernels, user registries or live
review data. Inspect `git status --short` before staging setup changes.
