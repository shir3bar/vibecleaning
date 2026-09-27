# P0 acceptance record

Use one copy of this record per candidate. The release identity is in
`release-manifest.json`. Automated checks on Linux do not certify Windows or
Mac, and local locking checks do not certify a network share. CI is optional.

## Candidate and evidence

- Release ID:
- Archive SHA-256:
- Test date and operator:
- Machine, OS/version and architecture:
- Python version and browser/version:
- Profile (CSV or RDS), study name, source hash, rows and individuals:
- Data location and local cache location:
- Share protocol/server/configuration (if applicable):
- Automatic check JSON, terminal log, screenshots and error files:

Record `PASS`, `FAIL`, or `NOT RUN` for every check. Include elapsed load time,
exact error and enough steps to reproduce a failure. Keep personal data and
passwords out of logs sent to others. Save this completed record outside the
release directory so the candidate's packaged files remain unchanged.

## Per-machine browser acceptance

Run the server locally, initially using a disposable copy of the supplied data.
Run `scripts/check_deployment.py` as described in the deployment guide, then
complete this table **for both CSV and RDS**. Repeat the loading, thresholds,
reports and exports checks on an authorized representative large study. The
24-fix bundle checks correctness, not large-study performance.

| Check | CSV result/time/error | RDS result/time/error |
| --- | --- | --- |
| Verify archive/extracted checksums and install with `uv sync --locked --no-dev` in a fresh environment | NOT RUN | NOT RUN |
| Start, log in, load the small study; all 24 fixes and two individuals present | NOT RUN | NOT RUN |
| Load a representative large study; record cold and warm load times and expected row/individual counts | NOT RUN | NOT RUN |
| Switch individuals repeatedly; inspect map and table, pan/zoom; no missing fixes or persistent blank map | NOT RUN | NOT RUN |
| Adjust thresholds; wait for readiness and compare flagged fixes with the table | NOT RUN | NOT RUN |
| Save manual allegations, dismiss and confirm them; verify saved state after restart | NOT RUN | NOT RUN |
| Save an individual decision with notes and needs-check; restart and verify | NOT RUN | NOT RUN |
| Generate issue reports using the overlapping-allegation scenario below | NOT RUN | NOT RUN |
| Generate individual reports for **each** animal; correct source, species, decision and notes | NOT RUN | NOT RUN |
| Export, reopen as a separate study and compare original fields and saved review decisions | NOT RUN | NOT RUN |
| Interrupt the local server after an acknowledged save; restart and verify saved history | NOT RUN | NOT RUN |
| Stop the server, clear only local RDS caches, restart and recover the same head/decisions | NOT RUN | NOT RUN |
| Back up with no writers, restore to another root, reopen, report and export successfully | NOT RUN | NOT RUN |

For the overlap check, mark the first two fixes of `animal-A` as suspected
`speed`, and the first fix additionally as suspected `GPS spike`. Dismiss only
the speed allegation on the first fix, then confirm its GPS allegation. Expected:

- The first fix has only active `GPS spike`, confirmed. Its dismissed speed
  allegation remains visible in history.
- The second fix retains suspected `speed`. A report selected by the speed
  allegation includes one fix, not two. A combined report has one active fix
  for each issue type. The export has those same statuses and labels.
- Save `Fix & Keep` with `José: vérifier 150° — 鹿` in the notes. The individual
  report must retain the decision and text. Use a data directory containing an
  accented or non-English name as well.
- In the RDS bundle, animal-A is from `9001_1.rds` / `Synthetic species A`;
  animal-B is from `9001_2.rds` / `Synthetic species B`. CSV uses `movement.csv`.

For export verification, compare row counts, source order, identifiers,
coordinates, timestamps and every original column. RDS additionally preserves
the move2/sf class, CRS, geometry, track/time attributes and track metadata.
Reopen exported files in a fresh study directory, without copying the original
study's review history into it. Do not replace the original source files.

Loading is ready when the intended individuals and their data have appeared and
loading has finished. Record slowness separately from an error or a map that
never becomes ready. Browser regression tests record `preview_activation_ms`;
an optional `VIBECLEANING_PREVIEW_BUDGET_MS` enables a separate performance limit.

## Two-machine shared-storage pilot

Use the same candidate and a separate disposable study on storage both machines
can reach. Keep each server, Python environment and cache local. Start in
`required` locking mode. Use distinct reviewer accounts plus an editor account.

| Check | Result, machine direction, evidence/error |
| --- | --- |
| A saves; B sees current head and updated review within five seconds; repeat B to A | NOT RUN |
| Arrange competing saves from stale pages; verify rejection/refresh without lost acknowledged work | NOT RUN |
| Perform simultaneous mutations repeatedly; verify mutual exclusion and consistent lineage | NOT RUN |
| Editor assigns/reassigns reviewers; old assignee cannot keep editing | NOT RUN |
| Editor takes control and releases it; other client reflects control and resumes correctly | NOT RUN |
| Disconnect one client/network during work; reconnect, inspect last acknowledged save, resume editing | NOT RUN |
| Interrupt a writer process; locks release and saved state remains readable from both machines | NOT RUN |
| Quiescent full backup, hash verification, restoration elsewhere and reopen from both clients | NOT RUN |

Do not infer university-share reliability from a different share. Repeat the
pilot on the actual university server, including its endpoint protection and
network configuration. If mutual exclusion fails, record the failure and use
the documented small pilot with `disabled` locking and **one named writer at
a time**. Confirm that every client shows the cooperative-mode warning.

## Release gate

- [ ] Confirmed app defects fixed and regression evidence attached.
- [ ] Windows acceptance completed for CSV and RDS, including large studies.
- [ ] Mac acceptance completed for CSV and RDS, including large studies.
- [ ] Reports and exports agree with saved decisions, including overlap/dismissal.
- [ ] Backup/restore demonstrated and recorded.
- [ ] Shared-storage pilot recorded; actual university-share pilot or explicit
  small single-writer rollout procedure agreed and assigned.
- [ ] Everyone has the same checksummed candidate and a retained rollback release.

Linux acceptance is useful but optional. Protocol development and comparison to
Kami's app are separate work and do not block these P0 fixes.
