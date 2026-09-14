# Vibecleaning Movement Review: Deployment Requirements

## Proposed deployment

Each user runs Vibecleaning locally on their university-managed computer. All
instances connect to one central university file share containing the movement
data and shared review history.

No central web server, public URL, HTTPS certificate, or reverse proxy is
required. Each application server listens only on the user's own computer at
`127.0.0.1`.

## User computers

- A 64-bit Windows, macOS, or Linux release still supported by university IT
- Python 3.11
- `uv` Python package manager
- 2 CPU cores and 8 GB RAM minimum
- 4 CPU cores and 16 GB RAM recommended for large studies
- Current Chrome, Edge, or Firefox with JavaScript,  and WebGL enabled
- At least 5 GB of free local space for the application environment and caches

Each computer must have its own local Python environment. The environment must
not be stored in the shared application or data folder.

Windows PowerShell:

```powershell
$env:UV_PROJECT_ENVIRONMENT="$env:LOCALAPPDATA\Vibecleaning\venv"
uv sync --locked --no-dev
```

macOS or Linux:

```bash
export UV_PROJECT_ENVIRONMENT="${XDG_DATA_HOME:-$HOME/.local/share}/vibecleaning/venv"
uv sync --locked --no-dev
```

## Central file share

- A university-managed network file share accessible by every approved user
- One shared, backed-up directory for all Vibecleaning data and review history
- Read and write permission for approved reviewers and editors
- Support for cross-computer file locking and atomic file replacement
- At least three times the size of the source datasets available
- The share should not be managed through OneDrive or another desktop-sync tool

Example data locations:

```text
Windows:       \\university-server\research\movement-data
macOS:         /Volumes/research/movement-data
Linux:         /mnt/research/movement-data
```

## Application files

The application may be installed separately on each computer or placed in one
versioned, read-only shared folder. All users must run the same application
version.

The central data location will be configured separately. For example, on
Windows:

```powershell
$env:VIBECLEANING_DATA_ROOT="\\university-server\research\movement-data"
```

On macOS or Linux:

```bash
export VIBECLEANING_DATA_ROOT="/path/to/mounted/movement-data"
```

The local application will normally be available at:

- `http://127.0.0.1:8422` for RDS data
- `http://127.0.0.1:8421` for CSV data

These ports do not need to be opened to the university network.

## Optional network access

Internet access is required to enable OpenStreetMap, CARTO, Esri, and OpenTopoMap features but is optional for basemap tiles. The application can operate with a blank map if these services
are blocked.

## Accounts and coordination

- Reviewer and editor accounts are stored in the central data directory.
- Local instances must be restarted after an administrator changes the account
  list or resets a password.
- The application permits one assigned reviewer per project.
- An editor can take control, which temporarily prevents the reviewer from
  editing.
- Concurrent changes are protected by a shared project lock and dataset/review
  revision checks.
- Users must not run more than one local Vibecleaning process at a time.


R and the R packages `sf` and `move2` are optional and needed for RDS export.
