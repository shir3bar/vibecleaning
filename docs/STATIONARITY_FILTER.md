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

This is a simple, conservative scan (`anchor-radius-v2`), not a search for every
possible cluster. The choice of the first fix can affect boundaries, and an
isolated excursion splits a stay. It can therefore miss or split stationary
periods; radius and duration sensitivity checks matter.

The filter breaks stays at gaps larger than your maximum, non-increasing
timestamps, changes in tag identifier (when supplied), and confirmed exclusions.
Existing `burst_` boundaries do not force a split: those segments can be shorter
than the stationary period being sought. Your **Maximum gap** controls whether
the observations on either side can be joined.
CSV rows with invalid coordinates also break stays; an unparseable timestamp
breaks the source sequence. Tracks from different individuals or files are
never joined. **Start/end** only includes stays touching a track's first or last
record, not the boundaries of an internal gap or burst. The app's display-only
burst-gap setting does not change this filter.

Earlier `anchor-radius-v1` flags retain their saved rows and settings. Explicit
v1 filters still use their original rule that source burst boundaries split
periods. New filters use v2; changing the algorithm does not rewrite review
history.

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
Changing settings later does not change an earlier flag. Raw data and
coordinates remain unchanged, and existing dismissal/history/export workflows
apply.

For a sensitivity comparison, start each alternative from the same dataset
version: already-confirmed exclusions affect where subsequent stays break.
An unsaved preview is exploratory and is not part of the recorded protocol.
