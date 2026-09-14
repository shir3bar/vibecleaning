# Vibecleaning Movement Review: Deployment Requirements

## Deployment model

Each reviewer runs the same versioned Vibecleaning release locally on a
university-managed Windows, macOS, or Linux computer. Every instance connects to
one university file share containing authoritative inputs, lineage, review state,
accounts, and exports. The server listens only on `127.0.0.1`; no central web
server or public port is required.

RDS SQLite indexes, Python environments, and other rebuildable caches stay on
each user's computer. Raw inputs remain immutable, and the existing lineage
schema is unchanged.

## Supported clients

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

University share ACLs protect `<data-root>/.vibecleaning/users.json`. A POSIX
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

Start an RDS client from a local release checkout:

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

```powershell
uv run python -m app.cache_cli clear-rds --all
```

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
