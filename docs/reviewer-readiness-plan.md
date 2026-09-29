# Reviewer readiness: plan and restart checkpoint

Updated: 2026-09-29. Repository: `/workspaces/vibecleaning`. Branch: `rds_input`.

## Where we are

All six requested improvements are implemented and tested locally. **They have not been pushed to GitHub or tested on the collaborators’ machines.** Protocol sensitivity testing is the next scientific task.

| Request | Local result |
| --- | --- |
| Record filters with 0 flags | Done for CSV and RDS. The saved step records settings, scope, user, source version and zero matches. Boolean choices remain available when their count is zero. Empty manual selections remain invalid. |
| Order the queue by flagged fixes | Done: “Flagged fixes — most first,” using unique active flagged fixes. Confirmed/dismissed fixes are not active flags. Saves preserve the active individual. |
| Loading/export progress | Done. Study/cache preparation and map preparation show an indeterminate bar; transfer shows a percentage when its size is known. The existing RDS export bar shows stages/files and completion or failure. |
| Stationarity and slow queue entry | Done locally. Obsolete whole-study downloads are cancelled on queue entry; stationarity waits for the queue’s individual/group scope. Repeated unchanged stationarity previews reuse bounded, compact results. |
| GPS 95th/99th shortcuts | Done. Default: 95th percentile. Buttons: “Flag 95th,” “Flag 99th,” “Flag both.” Both saves two labelled steps; overlapping flags count once. |
| Editor assignment on first action | Confirmed working for saved CSV and RDS actions. Fixed the dropdown that could keep saying “unassigned” afterward. Previews remain read-only; existing assignments are respected. |

The GPS cutoff is **one shared cutoff across selected individuals**, as requested. It uses all finite, nonnegative outbound step lengths at unconfirmed fixes, across all track sets, before applying the turn-angle condition. Exact linear percentiles, population size and numeric cutoffs are saved. Histogram zoom does not change the cutoff. The two percentile runs retain the same GPS issue type for review.

Also fixed during validation: CSV GPS previews were reading the wrong binary column names, and RDS filter counts could include already-confirmed fixes.

## What to do next

1. [ ] Make the local commits available on GitHub. No release tag is needed.
2. [ ] Have collaborators update the same branch, restart the app and reload the browser. Their earlier clone lacks the previous stationarity memory fix and export progress too.
3. [ ] Run this short check on the Mac and collaborators’ Windows installation:
   - Save a filter with zero matches; restart and find its “0 flags” step.
   - In a disposable unassigned study, save a first action and check the reviewer name.
   - Select individuals, check the automatic GPS cutoff, then save 95th/99th/both.
   - Enter the review queue while Browse all is loading; sort by flagged fixes.
   - Run Bildstein stationarity at 50 m / 48 h / 72 h; repeat unchanged; export RDS and watch progress.
4. [ ] Return to protocol validation and threshold sensitivity. Keep owner-marked outlier handling as an explicit first protocol step and run GPS before stationarity.

Do not compare the 9,260 benchmark count below with a study that has different saved exclusions or GPS flags.

## Evidence and limits

- **173 targeted non-browser tests passed**, covering filter history, assignment, stationarity/reruns, RDS, exports and review groups.
- **13 browser scenarios passed**, covering CSV/RDS zero-filter and percentile actions, queue behaviour, stationarity controls and RDS export success/failure. Subsequent focused reruns passed after final fixes (5 browser checks; then 3 browser + 4 frontend checks).
- Bildstein benchmark: 71 individuals, 1,239,130 fixes, existing local index, no saved annotations, 50 m / 48 h / 72 h. Identical source-row ranges and input history: **9,260 candidates**. Local uncached scans were approximately 6–8 seconds before and 5.8 seconds after; an unchanged repeated preview took approximately 0.001 seconds. These are container measurements, not a Windows/Mac guarantee or a cold RDS-cache-build measurement.
- Cache tests verify invalidation for settings, source changes and annotations, plus bounded memory and independent returned results.
- The stationarity algorithm and exclusion policy are unchanged. Raw tracking data and user review histories were not modified. No dependency, installer or CI changes.

## Commit and recovery checkpoint

- Previous local base: `b7f389c`.
- `b4d47db`: backend zero-result audit, percentile metadata, eligible RDS counts and stationarity result reuse.
- `1b720a1`: reviewer controls, percentile shortcuts, progress, queue changes and browser tests.
- Remote `origin/rds_input` was verified via HTTPS on 2026-09-29 at `38470ad4f93b51e18fadf6436651d649b7b11a89`. No push was performed. SSH fetch failed because the container had no accepted key. Git commit timestamps do not establish a push time.
- The earlier unpublished stationarity memory fix is `3026252`; RDS export progress is `557f349`.

After a connection loss: read this file, inspect `git status` and recent commits in this repository, and continue with the unchecked next steps. Do not redo completed implementation, reset local work, modify raw data, or stop the user’s app. Automated tests use temporary studies and separate server ports.

Primary regression commands:

```sh
.venv/bin/python -m pytest tests/test_filter_audit.py tests/test_movement_threshold_state.py tests/test_stationarity.py tests/test_stationarity_memory.py tests/test_stationarity_rerun.py tests/test_movement_fixes.py tests/test_multi_user_review.py tests/test_rds_movement.py tests/test_rds_export.py tests/test_rds_owner_colors.py tests/test_movement_issue_groups.py -m 'not browser' -q
.venv/bin/python -m pytest tests/test_review_readiness_browser.py tests/test_movement_browser.py::test_rds_export_shows_live_progress_and_completion -q
```
