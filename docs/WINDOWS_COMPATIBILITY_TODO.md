# Windows Compatibility Implementation and Pilot Checklist

## Implemented platform support

- [x] Configurable authoritative data and local cache roots with explicit,
  environment, then platform-default precedence
- [x] Windows `%LOCALAPPDATA%\Vibecleaning` environment/cache convention
- [x] Cross-platform re-entrant locking adapter with a 15-second Portalocker
  timeout and `required|disabled` modes
- [x] Cooperative mode that retains local serialization, atomic writes,
  authorization, and stale head/revision checks
- [x] Atomic same-directory writes with flush, `fsync`, bounded retries for
  Windows permission/sharing violations, and prior-destination preservation
- [x] Atomic archive promotion and Windows-safe cache/output promotion
- [x] Local signature-keyed RDS indexes, integrity validation, per-signature
  build locks, no request-time deletion of alternate signatures, and explicit
  cache cleanup
- [x] Three-second shared-state fingerprint polling through SSE, with lightweight
  refresh for workflow changes and movement reload only for a changed head
- [x] Stale-write rejection followed by browser refresh without mutation retry
- [x] Cooperative-mode startup and browser warnings
- [x] PowerShell launcher for RDS/CSV profiles, local environment/cache,
  loopback binding, port checks, and actionable path errors
- [x] Existing optional CI definition; local machine acceptance is the rollout
  evidence and does not require GitHub CI
- [x] Automated coverage for path precedence/non-ASCII paths, Windows defaults,
  lock re-entry/timeouts/two-process exclusion, cooperative mode, atomic retries,
  global-file concurrency, fingerprints, interrupted/corrupt RDS caches, and
  cache rebuilding/cleanup

## P0 candidate fixes and handoff

- [x] Explicit UTF-8 generated scripts, subprocess text and report inputs/outputs
- [x] Active issue counts/labels exclude dismissed allegations, retaining history
- [x] Individual report source/species and saved decision/notes
- [x] LF/CRLF-independent JavaScript tests and closed SQLite test handles
- [x] Browser readiness waits with separately recorded preview latency
- [x] Mac/Linux startup counterpart and per-release Windows environments
- [x] Checksummed application and synthetic CSV/RDS data bundles
- [x] Production-dependency-only automatic save/report/export/restore check
- [x] Account, shutdown, troubleshooting, backup and rollback instructions
- [ ] Real Windows VM acceptance: complete `docs/P0_ACCEPTANCE.md`
- [ ] Real Mac acceptance: complete `docs/P0_ACCEPTANCE.md`
- [ ] Optional remote Linux acceptance: complete `docs/P0_ACCEPTANCE.md`

These checkboxes describe implemented code, not completed real-machine testing.
Keep the completed acceptance records and machine errors with each release ID.

## Required university-share pilot

- [ ] Confirm the exact SMB/NFS protocol and server configuration.
- [ ] Test required-mode mutual exclusion in both directions between Windows 11
  and macOS/Linux clients.
- [ ] Test two simultaneous mutations and verify only one advances the head.
- [ ] Interrupt a process and the network while a lock is held; verify recovery.
- [ ] Test assignment changes, editor takeover/release, and stale browser writes.
- [ ] Test large RDS load/export and local cache rebuild on every client type.
- [ ] Test endpoint protection interaction with temporary-file replacement.
- [ ] Confirm cross-instance updates appear within five seconds.
- [ ] Record load time, cache size, and file-share traffic.

## Deployment decision

- [ ] If the pilot passes, deploy with `VIBECLEANING_SHARED_LOCKING=required`.
- [ ] If locks are unreliable, deploy with `disabled`, nominate one active writer,
  require write-access announcements/releases, and verify the warning is visible
  to every user.
- [ ] Back up the authoritative directory and pilot one study with a small group.
- [ ] Keep local installations on the same versioned release.

Raw inputs remain immutable, lineage state stays under each project's
`.vibecleaning` directory, and rollback is returning clients to the prior release.
