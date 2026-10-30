# Movement review manual

This guide describes the practical review workflow for the CSV and RDS movement
applications. The two versions share the same map, review queue, annotations,
rankings, reports, and lineage behavior. They differ only in how movement data
is loaded, how bursts are defined, and how reviewed data is exported.

## 1. Open the study and choose what to view

1. Select the study and dataset version.
2. Select one or more individuals. Large studies initially show no individuals;
   selecting one first shows its overview track and then replaces it with exact
   points and tracks when they are ready.
3. Use **All individuals** only when the complete study is needed on the map.
4. Open **Layers** for **Show track lines**, **Points**, **Bursts**, and the basemap.
   Use these controls and the individual checkboxes
   to change the map view. Turning off track lines also hides their line highlights
   and preview paths; points, point highlights, the player marker, and other
   overlays stay as they were. These preferences do not create dataset versions.

RDS studies use the source `burst_` values as authoritative bursts. CSV studies
use the configured movement burst definition.

When a study includes metadata, species names appear in navigation and individual
cards. Expand **Individual metadata** for biological details and the separate
deployment records. Species links open external name searches. Source deployment
dates and sampling statistics can differ from the supplied review track.

An individual's **invalid positions** button opens its table filtered to those
rows, including original longitude and latitude. Invalid positions remain
reviewable and exportable, with no map or player location. Paths break at these
positions. See [Study metadata and native AMT review](STUDY_METADATA.md) for the
file format and export behavior.

## 2. Choose a color variable

The **Color by** menu changes point and track-step colors. Hover or keyboard-focus
the help marker beside the active variable for its definition.

- **Individual ID** assigns each individual a categorical track color.
- **Step length** is the WGS84 geodesic distance from a fix to the following fix.
- **Speed** is that step length divided by the positive time interval to the
  following fix. Applying a Speed threshold requires both incoming and outgoing
  speeds to exceed it. Turning angle does not affect this filter. Track endpoints
  and other fixes with a missing adjacent speed cannot match. **Reverse** requires
  both speeds below the cutoff. Preset quantiles count each eligible step once.
- **Time delta** is the number of seconds from a fix to the following fix.
- **Turn angle** is the signed change in WGS84 geodesic bearing at the middle of
  the preceding, current, and following fixes.
- **GPS spike (step + turn)** requires both adjacent step lengths to exceed the
  selected distance and the absolute turn angle to exceed its configured minimum.
  Its color scale displays the outbound step length.
- **is_outlier** is the raw boolean result supplied by move2utils in an RDS file.
  It is source data, not Scrub Data review state.

## 3. Filter and inspect possible outliers

For the ordered study-wide Stationarity → Speed → GPS Spike protocol, open
**Filters** in the toolbar. Recalculation is optional and user initiated,
per filter, and creates its own graph step only when applied. See the
[study-filter walkthrough and pre-filtering script](STUDY_FILTERS.md).

The version selector shows a short action label. Open the **Version details**
information button for the saved rule, author, and date. The species badge opens
reference links and, when available online, a credited PhyloPic silhouette. Missing
images leave the text and links available. **Export** contains reviewed tracks and individual decisions;
**More** contains reports, ranking, and additional study actions.

For a numeric variable, click its histogram or enter a threshold. For a boolean
or categorical variable, select one or more levels. Matching fixes retain the
variable's colors and nonmatching context becomes gray.

The threshold is a temporary visual filter. It does not flag anything and does
not create a dataset version. **Select fixes** adds a bounded set of matches to
the checked-fix list for inspection; **Flag thresholded fixes** applies the saved
filter to the selected scope, including matches not materialized in that preview.

For **GPS spike (two-stage speed)** and **GPS spike (physiology)**, expand
**Edit thresholds** to change the two speed cutoffs (m/s) and the two minimum
absolute turn angles. Click **Preview changes**, inspect the updated map and
histogram, then flag when ready. **Reset defaults** restores the calculated
cutoffs and default angles. Custom values are recorded in the saved filter step.
The physiology default uses the speed cap's **lower confidence bound**
(`vmax_ci_lower`), with study Q3 still required as the lower speed cutoff.
Previously saved filters retain their recorded thresholds.

The timeline similarly keeps the nearest fix and its immediate neighbors colored
while temporarily graying other context.

## 4. Understand map review states

- **Temporary threshold match:** colored against gray context; not yet reviewed.
- **Checked fix:** selected locally for inspection or an action.
- **Suspected fix:** saved in a Scrub Data annotation step and shown with an
  amber halo unless suspicious fixes are hidden.
- **Confirmed exclusion:** confirmed against an originating suspicion and excluded
  analytically; it can be shown with **Confirmed exclusions**.
- **Raw is_outlier:** remains a source observation even if it has never been
  flagged in Scrub Data.

**Hide suspicious fixes** is only a map aid. It does not dismiss annotations,
change counts, alter reports, or change analytical eligibility.

## 5. Flag, confirm, or unflag fixes

1. Choose a threshold, checked fixes, a segment, bursts, or an entire individual.
2. Use the corresponding **Flag…** action and record the issue details.
3. To accept a suspicion as an analytical exclusion, check the suspected fixes
   and choose **Mark confirmed**.
4. To withdraw a suspicion, check the suspected fixes and choose
   **Unflag suspicious**. If several originating suspicions overlap, choose which
   groups to resolve in the dialog.

Flagging, confirmation, and unflagging are persistent DAG steps. Unflagging does
not delete history: it writes a dismissal annotation that resolves the selected
originating suspicion.

## 6. Review individuals

Open **Review queue** to work through individuals. Record **OK**, **Fix & Keep**,
or **Remove**, and use **Needs check** when another decision is required. Moving
to another individual saves a staged decision using the existing queue workflow.
A new or changed Remove decision requires **Redeployment**, **Short track**,
**Confined**, **Other**, or **Nest**. Other also requires a comment. Save and
navigation wait until these fields are valid. Older saved Remove decisions stay
valid even when no reason was recorded.

**Remove entire study** shows the affected count and asks for a shared reason and
comment. It supersedes decisions for every individual, including hidden or
filtered individuals, in one undoable step. Existing Needs check values, fix-level
flags, previous decisions, and notes remain available. Completion is a separate
action.

Use **Flagged fixes — most first** or **Flagged fixes — fewest first** in the queue
order menu. Counts use the existing flagged-fix metric; equal counts keep dataset
order. The direction is remembered and changing it keeps the active individual.

Under the active individual's saved flags, open **Flagged periods / fixes**.
The scrollable list groups clickable rows by issue type, with dates and counts.
**Periods / segments** uses the saved stationarity filter's actual periods:
source bursts and skipped GPS fixes do not split them. Other flags are grouped
by consecutive fixes in their source track. **Each fix** lists every unresolved
fix separately, with Previous/Next pages for long lists. Click a row to
highlight it, show flagged fixes, zoom to it with nearby track context,
and pause the player at its first fix. The black player marker shows that fix.
Times are UTC. This is navigation only: **Confirm** and **Unflag** beside an issue
type still act on that individual's whole group of flags for that issue.

Stationarity measures radius from the first fix of each candidate period. A fix
outside that radius or a gap above the maximum starts a new period. Each period
must independently meet the minimum duration. Start/end then selects periods
containing any of the individual's first/last three retained fixes. A noisy tail
can therefore contain long qualifying periods followed by a short final period
that fails the duration setting; Start/end will not join them automatically.

Keyboard shortcuts in the queue:

- Left/Right Arrow: previous or next individual.
- 1: OK.
- 2: Fix & Keep.
- 3: Remove.
- 4: toggle Needs check.

Later review rounds can carry prior OK decisions forward. The current card always
shows the effective decision for the active round.

## 7. Rankings, reports, and export

**Complete review** saves pending individual decisions, checks that all required
individuals are reviewed, and saves annotated source files plus
`individual_decisions.csv` to `<study>/scrubdata/cleaned_files/` on the app server.
Watch the progress bar. The review becomes Completed only after saving succeeds.
On failure, the review stays active and the last successful export is preserved;
use **Retry completion** after resolving the error or refreshing a changed study.
Completed studies are labelled and placed last in the study dropdown. A new
assignment or a later dataset change removes that label.

**Export individual decisions** downloads the selected dataset's current review
context during review or after completion. Save pending decisions first. It has
one row per individual, including undecided individuals, and includes the reason,
Needs check, comment, review round, reviewer identity, timestamp, and originating
step. Completion uses the same CSV generator and includes its checksum in
`writer_manifest.json`.

- **Rank bursts** refreshes available ranking analyses. The ranking tab and queue
  ordering can display saved rankings without rerunning them.
- Reports are analyses: they capture the selected review context but do not change
  the dataset.
- CSV export writes the reviewed CSV representation. **Export RDS** saves one
  `<source>_cleaned.rds` per source individual in the study's
  `scrubdata/cleaned_files/` folder, together with `individual_decisions.csv` and `writer_manifest.json` recording
  the exported dataset version and file checksums. The progress bar shows when
  the folder is ready; no browser download is needed. The folder is on the
  machine running the app.
- A successful RDS export refreshes `cleaned_files` after all files are validated.
  Previous exports remain recorded, with their files, under `analyses/`.
  Exported files retain the original rows and metadata plus the review columns;
  the `_cleaned` suffix does not mean confirmed rows are physically removed.

## 8. Versions, undo, and provenance

Every persistent review action creates a new dataset node. **Undo** moves the head
back one step and retains the undone steps. The earlier version is read-only,
just as when you select it from the version dropdown. You can continue undoing
or return to the latest version without discarding anything.

To make new edits from the earlier version, choose **Resume** and confirm the
warning. Resume archives the discarded steps' metadata, scripts, and summaries,
records them in an archive manifest, and deletes their generated outputs. Editing
is enabled after Resume completes. Raw source files remain unchanged.
