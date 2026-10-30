# Optional study-wide filters

Study filters are calculated only when requested. Review decisions can accumulate
for as long as needed. An outdated-filter notice never blocks review, completion,
or export. A preview does not change the dataset head. Applying a preview creates
one new data graph step for that filter alone.

Open **Filters** in the toolbar. Each filter has its own
Preview/Recalculate, Apply, and Cancel controls. Preview shows old/new thresholds,
eligible sample sizes, new and obsolete flags, and affected individuals. All
calculations cover the whole study, regardless of selected individuals or map
visibility. If earlier filters are outdated, the preview uses their current saved
flags; it does not rerun them.

**Rule & last run** expands the settings and saved statistics. Previews show the
proposed changes before **Apply changes** saves a new step. Closing the drawer
keeps the preview available until you cancel it or change the dataset version.

The default protocol is:

1. **Stationarity:** fixes within 100 m of the period's first fix, spanning
   strictly more than 12 hours, with at least three fixes and including any of the
   individual's first/last three eligible fixes. Gaps over 72 hours, invalid
   positions, and tag changes break periods.
2. **Speed:** both incoming and outgoing speeds strictly greater than study
   Q3 + 100 × IQR. The central fix is flagged, with no turning-angle condition.
3. **GPS Spike:** both adjacent speeds strictly greater than study P99 and an
   absolute turn angle strictly greater than 166°. The central fix is flagged.

Quantiles use linear interpolation (R type 7) over every finite, nonnegative
eligible step speed, including zero. Distances use WGS84 geodesics and speeds are
in m/s. Empty populations show an unavailable threshold rather than a fabricated
zero. Invalid positions remain in tables and exports and never join spatial steps.

Automatic protocol flags feed forward: Stationarity affects Speed and GPS Spike;
Speed affects GPS Spike. A filter's previous automatic candidates do not remove
themselves from its own statistics. Manual and confirmed exclusions apply to all
stages. Legacy/unmanaged suspicions remain independent exclusions and are not
silently converted into protocol results. Source owner bookkeeping fields such
as `is_outlier` do not independently exclude positions.

Human unflags are specific to an allegation/filter. That filter respects them on
later runs; another filter can independently flag the same position. Applying a
recalculation only retires obsolete unresolved automatic allegations from that
filter. Manual flags, confirmations, comments, human unflags and previous graph
versions remain. New issues after an individual's last review mark it as needing
review while retaining its recorded decision.

Each applied filter saves its settings, source checksums, excluded source rows,
quantiles, threshold, candidate rows, and implementation version with the graph
step, even when it finds no candidates. Rebuilding a disposable index or moving
the collection does not erase these records or invalidate unchanged inputs.

## Pre-filter an annotator's folder

From the application folder, using its existing environment:

```sh
uv run --locked python scripts/prefilter_movement_rds.py "/path/to/annotator/movement_rds" --user annotator
```

The same command works in PowerShell with a quoted Windows folder path. It runs
the three filters in order and creates three separate steps per study, leaving
RDS files and metadata unchanged. No running app or R installation is required.
Process studies sequentially; close active editing sessions before running it.

- `--dry-run` reports planned work without initializing review histories.
- `--resume` resumes missing stages; skipping existing runs is also the default.
- `--refresh` explicitly updates outdated stages while preserving human decisions.
- `--filter speed` limits work to one filter; repeat this option to select several.
- `--report report.json` saves results and per-study failures.

Successful filter steps survive an interruption. Rerun to complete missing stages.
Failures are reported per study; other studies continue and the command exits
with a failure status if any study failed. Existing outdated results require the
explicit `--refresh` option. Do not copy disposable SQLite caches between machines.

## Interactive synthetic walkthrough

The generated collection contains **raw/** and **prefiltered/** copies. Load
`prefiltered` as the app's data root: its `movement_rds` folder contains three
small studies. The species metadata is display-only; all positions and extreme
speeds are constructed examples, not observations of animals.

From the app folder, launch that copy with:

```sh
bash scripts/start-vibecleaning.sh --profile Rds --data-root "/path/to/demo-folder/prefiltered" --port 8423
```

On Windows, use the existing PowerShell launcher with `-Profile Rds`,
`-DataRoot "C:\path\to\demo-folder\prefiltered"`, and `-Port 8423`.

Generate a fresh copy from the app folder:

```sh
uv run --locked python scripts/generate_filter_pipeline_demo.py "/path/to/new/demo-folder"
```

The generator refuses to overwrite an existing folder, preserving manual review
history. `expected.json` contains source checksums and independently specified
expected thresholds. To repeat a walkthrough, generate another copy or copy
**raw/** into a new location and run the pre-filtering command there.

### 910001: stationarity changes Speed

1. Open **Stationary_head** in Individual queue. Unflag its entire 80-fix
   Stationarity period. This restores the synthetic 13-hour stationary head.
2. Speed and GPS Spike show optional recalculation notices. Continue reviewing
   other individuals; no calculations or additional filter steps happen.
3. Preview **Speed**. Its cutoff should change from approximately **152.75 to
   202 m/s**, removing the unresolved Speed flag on **Speed_candidate**.
4. Cancel. The dataset version and saved flags must be unchanged.
5. Preview again and apply Speed. There is one new Speed graph step. GPS Spike
   still has its old saved result and needs an optional recalculation.
6. Preview/apply GPS Spike separately. The 180 m/s reversal on **Speed_candidate**
   now receives a GPS allegation. Your Stationarity unflags remain intact.

### 910002: Speed changes GPS Spike

1. Inspect the initial GPS allegation on **Borderline_GPS**. Study GPS P99 is
   approximately **78.46 m/s**.
2. Unflag the Speed allegation on **Extreme_speed**. Optionally mark that
   individual reviewed. Speed's threshold calculation itself is unchanged;
   GPS Spike needs an optional update.
3. Preview GPS Spike. P99 becomes approximately **972.4 m/s**. One old GPS
   allegation disappears on **Borderline_GPS** and a new one appears on
   **Extreme_speed**.
4. Apply. Only GPS Spike creates a new graph step. A previously reviewed
   **Extreme_speed** needs review again, with its earlier decision retained.
   Its Speed unflags remain respected: a GPS allegation is a different issue.

### 910003: boundaries and map safety

- **Exactly_12_hours** is not stationary; **Over_12_hours** qualifies.
- **Radius_100m** qualifies; **Outside_radius** does not.
- **Invalid_gap** has an invalid position in its table. Its path breaks there;
  stationarity does not join the two sides.
- **Unplottable** remains reviewable in the table with no map location.
- **Single_fix** has no step speed.

For another batching check, manually flag a position on a control individual.
Affected filters become outdated. Add another allegation on that same position,
then unflag only one: the effective excluded input is unchanged. Unflag both
before recalculating and the notices clear if the input returns to its saved
state. Notes and individual-review decisions alone do not invalidate statistics.

Legacy Color by filters retain their saved meanings. Use the study-filter panel
for this versioned, ordered study-wide protocol.

New Color by Speed thresholds also require both adjacent speeds to pass the
chosen cutoff (both below it when **Reverse** is selected). Colors and histograms
still show outgoing step speeds; quantiles count each eligible step once and do
not use turning angles. A missing adjacent speed cannot match. Older saved
outbound-only study Speed runs show **Out of date** until you explicitly preview
and apply Speed as a new graph step. This rule update alone does not invalidate
Stationarity or GPS Spike; applying a changed Speed mask can invalidate GPS Spike.
