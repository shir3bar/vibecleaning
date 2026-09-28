# Scrub Data: IT deployment reference

**To install and open the app, follow [Windows](START_HERE_WINDOWS.md) or
[Mac](START_HERE_MAC.md).** Each guide covers Git installation, login and startup.

This longer document is for whoever manages installation, shared storage and
backups. For the controls inside the app, use the
[review manual](movement-review-manual.md).

## Deployment model

Each reviewer runs the same versioned Scrub Data release locally on a
university-managed Windows, macOS, or Linux computer. Every instance connects to
one university file share containing authoritative inputs, lineage, review state,
accounts, and exports. The server listens only on `127.0.0.1`; no central web
server or public port is required.

RDS SQLite indexes, Python environments, and other rebuildable caches stay on
each user's computer. Raw inputs remain immutable, and the existing lineage
schema is unchanged.

## Target clients and readiness

Windows and Mac are release targets, not a claim that this candidate has passed
on both. Complete `docs/P0_ACCEPTANCE.md` on the actual machines before rollout.
Linux is optional. GitHub CI is optional; the commands below run locally.

- Windows 11 x86-64, macOS, or Linux supported by university IT
- Python 3.11 and `uv`
- Current Chrome or Edge with JavaScript and WebGL
- 2 CPU cores and 8 GB RAM minimum; 4 cores and 16 GB recommended
- At least 5 GB of local free space, plus capacity for the largest RDS indexes
- A local, versioned application installation is preferred over executing code
  from the shared drive

R and the R packages `sf` and `move2` are optional and needed only for R-native
RDS export.

## Central share

The share must provide read/write access to approved users, atomic same-directory
replacement, and—in required mode—cross-client advisory file locking. It should
be backed up, have at least three times the source-data size available, and not
be mirrored by OneDrive or another desktop-sync client.

Example roots:

```text
Windows:  \\university-server\research\movement-data
macOS:    /Volumes/research/movement-data
Linux:    /mnt/research/movement-data
```

University share ACLs protect `<data-root>/scrubdata/users.json`. A POSIX
`chmod` value is not a Windows security boundary.

## Runtime configuration

```text
VIBECLEANING_DATA_ROOT       authoritative shared data
VIBECLEANING_CACHE_ROOT      local disposable cache
VIBECLEANING_SHARED_LOCKING  required or disabled; default required
```

Explicit constructor and CLI values override environment variables. Environment
variables override platform defaults. On Windows, environments and caches belong
beneath `%LOCALAPPDATA%\Vibecleaning`.

Start an RDS client from a local release directory:

```powershell
.\scripts\start-vibecleaning.ps1 `
  -Profile Rds `
  -DataRoot "\\university-server\research\movement-data" `
  -CacheRoot "$env:LOCALAPPDATA\Vibecleaning\cache" `
  -Port 8422 `
  -SharedLocking required
```

Use `-Profile Csv` for CSV review (default port 8421). The launcher validates
the roots, detects occupied ports, creates the local locked environment, and
binds to `127.0.0.1`.

macOS (use `--profile Csv` for CSV):

```bash
bash scripts/start-vibecleaning.sh --profile Rds \
  --data-root "/Volumes/research/movement-data" \
  --cache-root "$HOME/Library/Caches/Vibecleaning" \
  --port 8422 --shared-locking required
```

Linux:

```bash
bash scripts/start-vibecleaning.sh --profile Rds \
  --data-root "/mnt/research/movement-data" \
  --cache-root "$HOME/.cache/vibecleaning" \
  --port 8422 --shared-locking required
```

Open `http://127.0.0.1:8422` (RDS) or `http://127.0.0.1:8421` (CSV) in the
browser on the same machine. Keep the terminal open. The Mac/Linux launcher
keeps its Python environment in the local release directory. The Windows
launcher uses `%LOCALAPPDATA%\Vibecleaning\venvs\<release-id>`.

## Optional ZIP handoff for IT

The normal installation uses Git, as shown in the quick-start guides above.
The following packaging procedure is an alternative for an IT-managed handoff.

Build a candidate with `python scripts/build_release.py`. This packages the
current app, including reviewed local changes, as `dist/scrubdata-p0-*.zip`.
It also creates a small synthetic test-data ZIP and a SHA-256 file. Research
data, accounts, caches, and live `scrubdata` or legacy metadata are excluded. The
`release-manifest.json` inside each ZIP identifies the candidate by its packaged
file hashes and records the base Git commit; a dirty branch name alone is not
the release identity. Rebuild after any code change, and test the new identity.

1. Copy the application ZIP, test-data ZIP, and matching SHA-256 JSON to each
   machine. Compare archive hashes using `Get-FileHash -Algorithm SHA256` on
   Windows, `shasum -a 256` on Mac, or `sha256sum` on Linux.
2. Extract the app into a new local directory and the test data into a separate
   disposable data directory. Do not overlay an older release. The test data
   contains `movement_raw/acceptance/movement.csv` and
   `movement_rds/acceptance/*.rds`: two synthetic animals, 24 fixes per profile.
   It needs no workspace paths, symlinks, R installation, or research data.
3. From the application directory, verify both extractions:
   `python scripts/verify_bundle.py .` and
   `python scripts/verify_bundle.py "<test-data-directory>"`.
4. Install Python 3.11 and `uv` through IT's approved process, then prepare the
   locked production environment as below. No development dependencies are
   required. Initial installation needs package access; retain the installed
   environment for use where the share is offline.

PowerShell setup (use the same terminal for account setup and diagnostics):

```powershell
$ReleaseId = (Get-Content .\release-manifest.json -Raw -Encoding UTF8 | ConvertFrom-Json).release_id
$env:UV_PROJECT_ENVIRONMENT = Join-Path $env:LOCALAPPDATA "Vibecleaning\venvs\$ReleaseId"
$env:PYTHONUTF8 = "1"
uv sync --locked --no-dev --python 3.11
```

Mac/Linux setup:

```bash
export UV_PROJECT_ENVIRONMENT="$PWD/.venv"
export PYTHONUTF8=1
uv sync --locked --no-dev --python 3.11
```

Run the portable automatic checks from that environment:

```text
uv run --no-sync python scripts/check_deployment.py --output acceptance-local.json
```

This copies the synthetic data to a temporary non-English path and checks both
formats, reports, exports, stale saves, cache rebuild and relocation/restore.
It writes the release ID, machine, timings, result and errors to JSON. It uses
the API in-process; browser, large-study and actual share checks are additional.

## Accounts and study layout

Create the first editor once per authoritative data root, before reviewer use.
Passwords are prompted interactively; do not put them in a command or log.

```text
uv run --no-sync python -m app.auth_cli --data-root "<data-root>" bootstrap editor --display-name "Study Editor"
uv run --no-sync python -m app.auth_cli --data-root "<data-root>" add reviewer1 --display-name "Reviewer One" --role reviewer
uv run --no-sync python -m app.auth_cli --data-root "<data-root>" list
```

An editor assigns studies to reviewers through the existing review interface.
Keep one registry at `<data-root>/scrubdata/users.json`; all clients must
point to that same root. Use `reset-password <username>` or `disable <username>`
with the same `--data-root` for account administration. Do not bootstrap over an
existing registry or distribute it with the public test bundle.

CSV studies live at `<data-root>/movement_raw/<study-name>/*.csv`; RDS studies
at `<data-root>/movement_rds/<study-name>/*.rds`. Keep each study's entire
`scrubdata` directory alongside its original inputs. The synthetic data root
already has both layouts. Use an independent copy for each personal acceptance
run, and a separate disposable copy for the two-machine share pilot.

## Save, shutdown and restart

Wait for the app to acknowledge each save. Close the browser, then press Ctrl+C
in the server terminal and wait for it to exit. Restart with the same release,
data root and local cache root, log in and verify the current review state.
Closing only the browser does not stop the local server. After an interruption,
check whether the last save is present before submitting it again.

Dismissed allegations remain in the review annotation history. They do not
contribute to active report counts or labels. For an overlapping fix, dismissing
one allegation does not dismiss the other. Individual reports include the saved
decision and notes, species metadata when supplied, and original source file.

## Concurrency and recovery

Every authoritative mutation retains local-process serialization, atomic
same-directory writes, authorization, expected dataset-head checks, and expected
review-revision checks.

In `required` mode, a Portalocker exclusive file lock adds cross-machine
serialization with a 15-second timeout. Each connected browser polls lightweight
dataset-head, revision, assignment, and editor-control fingerprints through the
event stream every three seconds. Movement data reloads only when the head
changes. Stale mutations are rejected and the browser refreshes state without
retrying the mutation.

RDS indexes are keyed by source-bundle signature under
`<cache-root>/movement/rds/`. Builds are locally serialized, integrity-checked,
and atomically promoted. Cache deletion loses no review state; clear it with:

```text
uv run --no-sync python -m app.cache_cli --cache-root "<local-cache-root>" clear-rds --all
```

Stop clients using that local cache before clearing it. Never delete a study's
`scrubdata` directory to fix a cache problem. The candidate rebuilds older
RDS indexes to include report metadata; the first load can therefore be slower.

## Troubleshooting

- No page: inspect the launch terminal. If the port is occupied, stop the old
  instance or select another port and open that exact loopback URL.
- Dependency installation fails: retain the terminal error and check Python
  3.11 and approved package access. Keep `--locked`; do not update dependencies
  on just one reviewer's machine.
- No studies: check the selected CSV/RDS profile, the root layout above, and
  share permissions. A root should contain `movement_raw` or `movement_rds`,
  not be the study directory itself.
- Slow load or blank map: note when loading starts, whether the individual list
  and map become ready, and any terminal/browser errors. First RDS loads build
  a local index. Test again with a warm cache and another individual. A slow
  successful load is a performance result; a request error or persistent blank
  map is a failed check. Do not remove review history to retry.
- Save blocked or stale: reload, inspect the current head and assignment, and
  check editor control. Historical versions are read-only. Reapply only a
  decision that is still needed; rejected mutations are not auto-retried.
- Share unavailable: restore the connection before editing again. Preserve the
  error and last acknowledged save for the pilot record.
- Report error: record the analysis ID, release ID, source file, selected
  individual/issues and terminal error. Unicode names and degree symbols are
  included in the automatic check.

## Backup, restore and rollback

Arrange a period with no writers and stop all clients before copying. Back up
the **entire data root**, including root account/workflow state, every raw input,
and every study's `scrubdata` directory (lineage, annotations, scripts,
specifications, summaries and generated exports). Back up the release ZIP and
checksum separately. Local Python environments and RDS caches are disposable.
Protect account files under the same access rules as the originals.

On Windows, `robocopy "<data-root>" "<new-backup-root>" /E /COPY:DAT /DCOPY:DAT`
includes hidden directories; inspect its result (exit code 8 or above means an
error). On Mac/Linux, `cp -a "<data-root>/." "<new-backup-root>/"` includes hidden
state; create the empty destination first. Verify file counts and hashes before
calling the backup complete. Do not use mirroring/deletion options for recovery.

Restore the complete backup to a different empty directory, use a fresh local
cache and the same release, then load both formats, inspect decisions and notes,
generate reports and export again. Record the restored head and compare source
hashes. A copied directory alone is not proof of a working restore.

For rollback, stop all clients, take another complete backup, and switch every
client to the previous versioned release and its environment. To return to a
version that uses `.vibecleaning`, follow the [metadata rollback steps](SCRUBDATA_MIGRATION.md#rollback).
Verify that older release against a
copy of current state before resuming writes; clear only disposable caches if
needed. Restore an earlier authoritative snapshot only as a coordinated recovery
decision, since doing so discards acknowledged work after that snapshot.

## Cooperative fallback

Filesystem locking over SMB or NFS depends on the actual share. If the pilot
shows that Windows and macOS/Linux clients do not mutually exclude reliably, set
`VIBECLEANING_SHARED_LOCKING=disabled` (or launch with
`-SharedLocking disabled`). This disables only the cross-machine lock. Every user
sees a cooperative-mode warning.

Operating procedure in cooperative mode:

1. Name one person as the active writer before editing begins.
2. Announce the writer and study in the team's agreed channel.
3. Everyone else may inspect data but must not submit mutations.
4. The writer finishes or closes the app, announces release, and hands write
   access to the next person.
5. Anyone seeing a stale-state conflict reloads and verifies the latest result;
   the app never automatically retries a mutation.

Atomic replacement prevents torn files, but cooperative mode cannot provide
cross-machine compare-and-swap. Simultaneous human writes may still conflict or
produce a last-writer result.

## Pilot and rollout gate

Test `required` mode on the actual university share with at least one Windows 11
client and one macOS or Linux client. Test simultaneous edits in both directions,
process and network interruption, editor takeover, large RDS builds/exports,
cache rebuilds, antivirus interaction, account protection, and observed refresh
latency. Record the share protocol and server configuration.

Deploy in required mode only if mutual exclusion succeeds. Otherwise deploy the
documented cooperative procedure and banner. Back up the central directory,
begin with one study and a small group, and keep every client on the same release.
Rollback is stopping the new clients and returning to the prior versioned
release; no lineage migration is required.

The P0 release gate additionally requires completed Windows **and Mac** browser
acceptance for CSV and RDS, matching reports/exports/saved decisions, and a
demonstrated restore. An alternate shared drive helps exercise the workflow but
cannot establish the university share's locking behavior. Record outstanding
checks explicitly in `docs/P0_ACCEPTANCE.md`.
