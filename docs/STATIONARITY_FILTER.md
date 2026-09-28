# Find long stationary periods

1. Load a study and select an individual (or several individuals).
2. Choose **Stationarity** in **Color by**.
3. Set the **radius**, **minimum duration** and **maximum gap between fixes**.
4. Choose **Start/end of track** or **Anywhere**.
5. Colours update automatically after you change a setting. Stationary
   candidates are highlighted on the map. This does not save a review decision.
6. Inspect the candidates using **Select fixes**, then use the normal flag
   button, add your explanation and save. Filters apply to the individuals
   selected in the list. Use **All individuals** to include the whole study.
   Use the usual confirm/dismiss actions after reviewing them.

The initial values (50 metres, 6 hours, maximum 1-hour gaps) are starting values
for exploration, not a validated cleaning protocol. For example, an hourly
deployment with small timing differences may need a gap limit slightly above
one hour. Compare plausible settings before adopting a protocol.

A resting or roosting animal can match. This filter does not decide whether a
tag was dropped, whether deployment timing was wrong, or whether an individual
should be removed.

## What counts as stationary?

The app scans each individual/source file/track set in time order. A stay starts
at a fix and includes consecutive fixes within the chosen radius **of that
first fix**. The centre stays fixed. A fix outside the radius starts a new stay.
A candidate must contain at least three fixes and span the minimum elapsed time.

This is a simple, conservative scan (`anchor-radius-v3`), not a search for every
possible cluster. The choice of the first fix can affect boundaries, and an
excursion that has not been flagged as a GPS spike can split a stay. It can miss or split stationary
periods; radius and duration sensitivity checks matter.

**Saved GPS-spike flags and confirmed exclusions are skipped for this calculation.**
This includes unresolved GPS-spike flags. Skipping a flag here does not confirm
it or exclude it from exports. Other unresolved issue types are still included.

**Maximum gap is measured between the remaining fixes.** For example, if a
GPS spike at noon is skipped, the gap from the retained 11:00 fix to the retained
13:00 fix is two hours. Those observations can join only if your gap limit allows
two hours. A longer limit tolerates missing observations; it does not establish
that the animal stayed there throughout the gap.

The filter breaks stays at gaps larger than your maximum, non-increasing
timestamps and changes in tag identifier (when supplied).
Existing `burst_` boundaries do not force a split: those segments can be shorter
than the stationary period being sought. Your **Maximum gap** controls whether
the observations on either side can be joined.
CSV rows with invalid coordinates also break stays; an unparseable timestamp
breaks the source sequence. Tracks from different individuals or files are
never joined. **Start/end** only includes stays touching a track's first or last
retained record, not the boundaries of an internal gap or burst. The app's display-only
burst-gap setting does not change this filter.

Earlier v1/v2 flags retain their saved rows and settings. Explicit v1/v2 filters
still split at confirmed exclusions; v1 also splits at source burst boundaries.
An explicit rerun uses v3 and records the change. Earlier history is unchanged.

CSV and RDS use the same distance calculation (WGS84 geodesic metres) and rule.
Both evaluate the full selected individuals across track sets, regardless of
map zoom. The colour column shows candidates on loaded tracks. When flagging,
the app counts exact matches for the selected individuals. Hidden individuals
are not included; choose **All individuals** to include everyone.

## What is recorded?

Saving a flag records the radius, elapsed duration, gap limit, minimum fix count,
position choice, selected scope, algorithm version and implementation digest.
The existing review step also retains the reviewer, time, notes, parent dataset
and exact original source rows. Its saved script bundles the filtering code.
It also records the source rows examined and skipped, including individuals
with no candidates in a saved study-wide run.
Changing settings later does not change an earlier flag. Raw data and
coordinates remain unchanged, and existing dismissal/history/export workflows
apply.

For a sensitivity comparison, start each alternative from the same dataset
version: saved GPS flags and confirmed exclusions affect the calculation.
An unsaved preview is exploratory and is not part of the recorded protocol.

## When GPS decisions change

In **Individual queue**, an affected saved stationarity run offers **Rerun filter**.
The check covers the individual, source files and rows examined by that run,
not just the periods it detected. Removing a spike can reveal a previously
undetected stationary period.

- Confirming a GPS flag already skipped by v3 does not need a rerun.
- Unflagging prompts a rerun only when it restores a fix to the calculation.
  Another GPS flag or confirmed exclusion on that fix can still keep it skipped.
- Adding GPS flags or other confirmed exclusions can also require a rerun.
- Resolving that run's own stationarity flags does not invalidate the run.

Click **Rerun filter** to calculate that individual's examined track with the saved
thresholds. You will see counts of new and obsolete flags. **Cancel** saves nothing.
**Update flags** records a new step, adds new candidates and dismisses obsolete
unresolved candidates. Earlier confirmations and human dismissals remain intact;
the preview reports when its results differ from those decisions. Previous
versions remain available through history and Undo.

Opening an individual's card checks changed inputs without running the distance
scan. Reruns reuse cached source records and results for unchanged inputs; they
do not rerun the whole study. The **Color by** preview remains exploratory and
updates to reflect the currently selected version and settings.
