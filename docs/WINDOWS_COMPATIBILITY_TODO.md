# Windows Compatibility TODO

## Objective

Support this deployment model without weakening the existing review guards:

```text
University network file share
└── authoritative data and shared review history

Each user's Windows, macOS, or Linux computer
├── one local Vibecleaning server bound to 127.0.0.1
├── one local Python environment
├── local disposable caches
└── browser connected to the local server
```

The same project may be opened from multiple computers. The assigned-reviewer,
editor-control, expected-dataset-head, and expected-review-revision checks remain
authoritative.

## Scope

This update includes native Windows support and safe coordination between local
application processes using the same university file share.

It does not include:

- A centrally hosted web application
- Institutional SSO or MFA
- Preventing users with direct share access from editing files manually
- Multiple application processes on one user's computer
- Automatic merging of separate copies of the data directory

## Current blockers

- [ ] `app/edit_locks.py` imports Unix-only `fcntl`.
- [ ] Movement servers hard-code the repository's `data/` directory.
- [ ] RDS SQLite indexes are stored in the shared project cache and can be built
      or cleaned up concurrently.
- [ ] Live study events use a process-local `StudyEventBroker`, so changes made
      through another local server are not pushed to the current browser.
- [ ] Several atomic replacements do not retry Windows or network-share
      file-in-use failures.
- [ ] The account-file permission call uses POSIX mode `0600`, which is not a
      Windows ACL.
- [ ] Windows, UNC-path, mixed-platform, and real network-share tests are absent.

## Design decisions

### Authoritative versus local files

Keep these on the central share:

- Raw CSV or RDS inputs
- Project and dataset manifests
- Review assignments and editor-control state
- Review annotations
- Step and analysis records
- Generated exports and reports
- The shared account registry

Keep these local to each computer:

- Python virtual environment
- RDS SQLite indexes
- Temporary files
- Rebuildable caches
- Browser state

Local derived caches are preferred over shared SQLite caches. They avoid
cross-platform SQLite locking, improve performance, and do not need backup.

### Cross-computer lock

Use one cross-platform exclusive project lock for every authoritative mutation.
A candidate implementation is Portalocker, which supports Windows and POSIX.
Filesystem locking over SMB or NFS is implementation-dependent, so the lock must
be tested against the university's actual share.

Decision gate:

- If mixed Windows/macOS/Linux clients mutually exclude each other reliably on
  the university share, retain a filesystem lock.
- If they do not, use a small central lock service, such as Redis, or return to
  a single central application server.

Do not rely only on creating a lock file. The lock must be held by an open handle
and released automatically when the process exits.

## Implementation tasks

### 1. Configurable paths

- [ ] Add `VIBECLEANING_DATA_ROOT` to the Movement, Slim Movement, and RDS
      Movement server entry points.
- [ ] Precedence: explicit function/CLI argument, environment variable, then
      repository `data/` default.
- [ ] Add `VIBECLEANING_CACHE_ROOT` for local rebuildable caches.
- [ ] Default the local Windows cache beneath `%LOCALAPPDATA%\Vibecleaning`.
- [ ] Keep the existing project layout below the configured data root.
- [ ] Validate missing, inaccessible, and read-only paths at startup with a clear
      error.
- [ ] Test UNC paths, mapped drives, spaces, and non-ASCII path components.
- [ ] Make account-management commands use the same configured data root while
      preserving the existing `--data-root` override.

Primary files:

- `examples/movement/server.py`
- `examples/slim_movement/server.py`
- `examples/rds_movement/server.py`
- `app/auth_cli.py`
- `examples/movement/rds_index.py`

### 2. Cross-platform project locking

- [ ] Introduce one locking adapter rather than importing a platform API from
      application code.
- [ ] Replace `fcntl.flock` in `project_mutation_lock()`.
- [ ] Use an exclusive lock with a bounded timeout and a useful error message.
- [ ] Ensure a crash or terminated process releases the lock.
- [ ] Preserve re-entry behavior required by existing guarded operations.
- [ ] Confirm that all lineage and review mutations pass through the project
      lock.
- [ ] Add a separate lock for global mutable files such as the query library.
- [ ] Define an administrator-only procedure for account-file changes while
      local instances are stopped or being restarted.

Primary files:

- `app/edit_locks.py`
- `app/reviews.py`
- `app/query_library.py`
- `app/auth.py`

### 3. Windows-safe atomic writes

- [ ] Consolidate JSON writes behind a shared atomic-write helper.
- [ ] Create temporary files in the destination directory.
- [ ] Flush and close temporary files before replacement.
- [ ] Use `os.replace` only after all application-owned read handles are closed.
- [ ] Retry transient `PermissionError` and sharing violations with bounded
      exponential backoff.
- [ ] Never retry validation, authorization, or revision conflicts.
- [ ] Preserve the old destination if replacement ultimately fails.
- [ ] Verify directory archive/restore operations under Windows semantics.
- [ ] Do not treat `chmod(0o600)` as a Windows security boundary; document that
      the university share ACL protects the account registry.

Primary files:

- `app/state.py`
- `app/reviews.py`
- `app/auth.py`
- `app/query_library.py`
- `app/edit_locks.py`

### 4. Local RDS indexes and cache safety

- [ ] Move RDS index paths from the shared project cache to the configured local
      cache root.
- [ ] Key indexes by source-bundle signature so identical data can reuse a valid
      local index.
- [ ] Add a local per-signature build lock to prevent duplicate construction.
- [ ] Build into a temporary file, run SQLite integrity checks, close it, and
      replace the final cache entry.
- [ ] Remove automatic deletion of other signature-named indexes from request
      handling.
- [ ] Add an explicit local cache-cleanup command.
- [ ] Ensure cache loss never loses review state and causes only a rebuild.
- [ ] Keep authoritative RDS inputs and review annotations on the central share.

Primary file:

- `examples/movement/rds_index.py`

### 5. Cross-instance change detection

- [ ] Keep the existing in-process event broker for immediate local updates.
- [ ] Add lightweight polling of shared project and review revisions.
- [ ] Notify the local browser when another process changes the dataset head,
      review revision, assignment, or editor-control state.
- [ ] Target an update delay of no more than five seconds.
- [ ] Retain expected-head and expected-revision validation as the final write
      guard.
- [ ] If a stale screen submits a mutation, reject it and trigger an automatic
      state refresh.
- [ ] Confirm that polling does not scan or reload movement fixes unless the
      dataset head actually changes.

Primary files:

- `app/events.py`
- `examples/movement/routes.py`
- `examples/movement/static/app.js`

### 6. Windows launcher and installation

- [ ] Add a supported PowerShell launcher.
- [ ] Keep the Python environment outside a shared application directory.
- [ ] Bind only to `127.0.0.1`.
- [ ] Detect an already-running local instance and show a useful message.
- [ ] Document startup for RDS and CSV profiles.
- [ ] Decide whether the application code is installed locally or served from a
      versioned, read-only shared folder.
- [ ] Ensure all users run the same application release.
- [ ] Verify the locked dependencies install on Windows x86-64 with Python 3.11.
- [ ] Test optional R-native export separately when R, `sf`, and `move2` are
      required.

Example PowerShell environment:

```powershell
$env:UV_PROJECT_ENVIRONMENT="$env:LOCALAPPDATA\Vibecleaning\venv"
$env:VIBECLEANING_DATA_ROOT="\\university-server\research\movement-data"
$env:VIBECLEANING_CACHE_ROOT="$env:LOCALAPPDATA\Vibecleaning\cache"
```

### 7. Automated tests

- [ ] Run the existing suite on Windows with Python 3.11.
- [ ] Retain Linux and macOS coverage.
- [ ] Add a two-process test proving only one simultaneous project mutation can
      commit.
- [ ] Add a stale-head test where one process advances the dataset and the other
      receives a conflict.
- [ ] Add reviewer/editor-control tests across separate processes.
- [ ] Add account and query-library concurrency tests.
- [ ] Add atomic-write failure and retry tests.
- [ ] Add local RDS cache construction and interrupted-build tests.
- [ ] Add UNC-path and spaces-in-path tests.
- [ ] Add cross-instance event/polling tests.
- [ ] Confirm that a local cache deletion causes a rebuild without changing the
      dataset graph.

### 8. University file-share pilot

- [ ] Test on the actual university share, not only a local temporary directory.
- [ ] Include at least one Windows computer and one macOS or Linux computer.
- [ ] Verify mutual exclusion in both directions between operating systems.
- [ ] Test editor takeover while the reviewer screen is open elsewhere.
- [ ] Test simultaneous flagging attempts and confirm one is safely rejected.
- [ ] Test application or network interruption while a lock is held.
- [ ] Test large RDS load, export, and local cache rebuild.
- [ ] Test antivirus or endpoint-protection interaction with temporary-file
      replacement.
- [ ] Confirm acceptable load time and file-share traffic.
- [ ] Record the supported share protocol and server configuration.

## Acceptance criteria

The update is ready when:

- [ ] Native Windows startup requires no WSL or Docker.
- [ ] No application module imports `fcntl` directly.
- [ ] Windows, macOS, and Linux can use the configured central data root.
- [ ] Every computer uses its own environment and local derived cache.
- [ ] Only the assigned reviewer or controlling editor can mutate a project.
- [ ] Two concurrent writes cannot both advance the same dataset head.
- [ ] Other local instances display a shared-state change within five seconds.
- [ ] RDS indexes are never read from or written to the central share.
- [ ] A failed write leaves the previous authoritative state readable.
- [ ] The full automated test suite passes on Windows and Linux.
- [ ] The mixed-platform university-share pilot passes.

## Rollout

- [ ] Back up the complete central data directory.
- [ ] Deploy a versioned application release to pilot users.
- [ ] Create local environments and caches per computer.
- [ ] Configure the shared data path.
- [ ] Run the mixed-platform acceptance test.
- [ ] Begin with one study and a small reviewer group.
- [ ] Monitor lock conflicts, refresh failures, cache rebuilds, and share latency.
- [ ] Expand access only after the pilot is stable.

Rollback consists of stopping the new local instances and returning to the prior
application release. The compatibility work must not change the lineage schema
or raw input files.

## Open decisions

- [ ] Filesystem lock on the university share or central Redis lock
- [ ] Local application installation or shared read-only application folder
- [ ] Supported Windows editions and CPU architectures
- [ ] Required macOS and Linux versions
- [ ] Polling interval for cross-instance changes
- [ ] Whether R-native RDS export is required
- [ ] Pilot users and representative large study

