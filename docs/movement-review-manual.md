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
4. Use **Points**, **Bursts**, and the individual checkboxes to change the map
   view. These controls do not create dataset versions.

RDS studies use the source `burst_` values as authoritative bursts. CSV studies
use the configured movement burst definition.

## 2. Choose a color variable

The **Color by** menu changes point and track-step colors. Hover or keyboard-focus
the help marker beside the active variable for its definition.

- **Individual ID** assigns each individual a categorical track color.
- **Step length** is the WGS84 geodesic distance from a fix to the following fix.
- **Speed** is that step length divided by the positive time interval to the
  following fix.
- **Time delta** is the number of seconds from a fix to the following fix.
- **Turn angle** is the signed change in WGS84 geodesic bearing at the middle of
  the preceding, current, and following fixes.
- **GPS spike (step + turn)** requires both adjacent step lengths to exceed the
  selected distance and the absolute turn angle to exceed its configured minimum.
  Its color scale displays the outbound step length.
- **is_outlier** is the raw boolean result supplied by move2utils in an RDS file.
  It is source data, not Vibecleaning review state.

## 3. Filter and inspect possible outliers

For a numeric variable, click its histogram or enter a threshold. For a boolean
or categorical variable, select one or more levels. Matching fixes retain the
variable's colors and nonmatching context becomes gray.

The threshold is a temporary visual filter. It does not flag anything and does
not create a dataset version. **Select fixes** adds a bounded set of matches to
the checked-fix list for inspection; **Flag thresholded fixes** applies the saved
filter to the selected scope, including matches not materialized in that preview.

The timeline similarly keeps the nearest fix and its immediate neighbors colored
while temporarily graying other context.

## 4. Understand map review states

- **Temporary threshold match:** colored against gray context; not yet reviewed.
- **Checked fix:** selected locally for inspection or an action.
- **Suspected fix:** saved in a Vibecleaning annotation step and shown with an
  amber halo unless suspicious fixes are hidden.
- **Confirmed exclusion:** confirmed against an originating suspicion and excluded
  analytically; it can be shown with **Confirmed exclusions**.
- **Raw is_outlier:** remains a source observation even if it has never been
  flagged in Vibecleaning.

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

Keyboard shortcuts in the queue:

- Left/Right Arrow: previous or next individual.
- 1: OK.
- 2: Fix & Keep.
- 3: Remove.
- 4: toggle Needs check.

Later review rounds can carry prior OK decisions forward. The current card always
shows the effective decision for the active round.

## 7. Rankings, reports, and export

- **Rank bursts** refreshes available ranking analyses. The ranking tab and queue
  ordering can display saved rankings without rerunning them.
- Reports are analyses: they capture the selected review context but do not change
  the dataset.
- CSV export writes the reviewed CSV representation. RDS export writes a ZIP with
  one reviewed RDS per source individual and the writer manifest.

## 8. Versions, undo, and provenance

Every persistent review action creates a new dataset node. **Undo** moves the head
back one step; it does not erase the recorded lineage. Opening an older dataset
may be read-only when it is not the active graph head. Use the version controls
and the existing resume workflow rather than editing raw source files.

