# Speed spike filters

Requested 2026-10-01. Keep this checklist current so work can resume after a connection loss.

## Agreed scope

- Switch the editable GPS spike filter from distance to speed in m/s. Keep old saved distance filters reproducible under their original settings.
- Add two separate Color by choices: **GPS spike (two-stage speed)** and **GPS spike (physiology)**. Previewing does not create a review step; the existing flag action saves it, including zero-result runs.
- Study-quantile rule: use Q3 and P99.9 of the entire study's eligible step-speed distribution, independent of selected individuals, turn angle and map viewport. Count each step once. Both adjacent speeds must exceed the applicable cutoff. Above Q3 requires |turn| >= 166 degrees; above P99.9 requires |turn| >= 157 degrees. No matches below Q3.
- Physiology rule: default to the supplied lower confidence bound, `vmax_ci_lower`, from species.csv. Both adjacent speeds must exceed study Q3. Above the lower-CI cutoff requires |turn| >= 157 degrees; otherwise |turn| >= 166 degrees. The user explicitly retained Q3 as the floor, including when the supplied lower CI is zero or below Q3. Keep the original `v_max_m_s` point estimate in the saved metadata.
- Preserve the existing move2-compatible signed bearing-change calculation. The quoted 166–194 and 157–203 degree ranges wrap around 180, giving the absolute-angle thresholds above.
- Add a **Q3 + 100 × IQR** preview preset for speed. Keep P95/P99 previews. Display the resulting cutoff and record how it was obtained when a filter is saved.
- Show the actual study cutoffs / physiology cap and units, and distinguish the two candidate groups on the map. Descriptive guidance belongs in tooltips. Preserve review, exports, reports and stationarity's recognition of GPS-spike flags.

## Implementation

- [x] Inspect and define one shared speed-spike calculation for CSV/RDS, including exclusions, invalid times, triple endpoints and study-wide percentile scope.
- [x] Implement versioned speed filter definitions and resolved preview/save scopes. Record numerical cutoffs, percentile population, angle limits, cap source and implementation identity.
- [x] Package the supplied species cap table for other computers, without workspace-specific paths. Match study / species unambiguously and explain missing caps rather than guessing them.
- [x] Update manual spike speed previews, including binary workers and labels; preserve legacy saved distance rules.
- [x] Add both derived Color by choices and source-row-based candidate groups, keeping memory bounded for large RDS studies.
- [x] Add the speed IQR preset and its saved provenance.
- [x] Check preview/save/export agreement, stationarity interactions, and historical distance behavior with targeted backend and browser tests.
- [x] Document the final rules and how to explore them.
- [x] Replace ambiguous group names with speed ranges and angle limits; add a compact whole-study speed histogram with Q3 and the upper cutoff marked.
- [x] Allow explicit speed/angle overrides, with preview and reset, truthful labels and histogram markers, and recorded settings for reproducible replay.
- [x] Default new physiology filters to the cap's lower CI; retain the original point-estimate meaning of previously saved filters.

## Scientific interpretation

This adapts the user's supplied Roland excerpt; it does not reproduce its fixed distance thresholds averaged across species. At exactly four minutes, 28 m / 240 s = 0.1167 m/s and 173 m / 240 s = 0.7208 m/s. Study-specific speed quantiles and physiology caps are separate comparisons. Use supplied caps as supplied, without silently adjusting them.

The source table is /workspaces/movebench-mini_test/datasets/movebench-mini/species.csv. Preserve its supplied fields and all raw tracks. The added `speed_p999_m_s` column contains full MoveBench Mini reference percentiles before app or owner-outlier exclusions; `speed_percentiles_provenance.json` records source hashes, populations and calculation details. These references do not replace the app's current-dataset percentiles, which reflect confirmed exclusions. Tests use disposable data; do not create flags in the user's pilot or restart their server.


## Using the filters

Restart the server and refresh the browser. In Browse all, select **GPS spike (two-stage speed)** or **GPS spike (physiology)** under Color by. The pane shows study Q3 and P99.9 / cap lower CI in m/s. Orange is **Q3–P99.9 · |turn| ≥166°**; red is **Above P99.9 · |turn| ≥157°**. The physiology filter labels use **cap lower CI** instead of P99.9; its tooltip also shows the original point estimate. Each fix's group uses the slower of its incoming and outgoing speeds. The two checkboxes let you explore or flag either group. Select fixes only previews; the separate flag action creates a step. A selected group with no matches can still be saved as a zero-flag step.

The small histogram shows all eligible study step speeds before angle filtering, with Q3 and P99.9 / cap lower CI marked. Hover for bin counts and exact cutoff values. Log / Linear changes only the horizontal axis: bar height always represents step count. Zero speeds have their own bar on the log axis. Both cutoffs remain visible even when a supplied cutoff is zero or outside the observed range. Selecting individuals changes the candidate scope, not this study-wide distribution. Histogram counts are computed during the existing distribution pass and cached with its cutoffs; no extra track scan is needed.

Open **Edit thresholds** to change either speed cutoff (m/s) or either minimum absolute turn angle (0–180°). **Preview changes** updates the candidates and histogram without saving flags. Flagging is disabled while edits are waiting to be previewed. **Reset defaults** restores Q3, P99.9 / cap lower CI and the 166° / 157° angles. Edited speeds are labelled **Lower** / **Upper**, rather than claiming they remain calculated quartiles or caps. Only edited fields become fixed overrides; untouched fields continue using their calculated defaults. Switching studies clears the overrides. Saving flags records the overrides, effective numerical limits and original calculated defaults; replay uses those recorded values. The supplied species-cap values are unchanged.

The editable **GPS spike (speed + turn)** has speed in m/s, the existing angle control, P95/P99 and **Q3 + 100 × IQR** preview buttons. The ordinary Speed column has the IQR preset too. Those editable presets use the selected individuals, as before; the two automatic filters always use the whole study.

## Exact calculations and code

For A → B → C, compute v_in = distance(A,B) / elapsed_seconds(A,B), and v_out = distance(B,C) / elapsed_seconds(B,C). Distances use the existing WGS84 geodesic calculation. With default settings, let s = min(v_in, v_out), q = study Q3 and u = study P99.9 or the supplied cap's lower confidence bound (`vmax_ci_lower`):

| Condition | Rule for B |
| --- | --- |
| s ≤ q | No spike candidate |
| q < s ≤ u | Candidate if absolute turn ≥ 166° |
| s > q and s > u | Candidate if absolute turn ≥ 157° |

If the cap is below Q3, the Q3 floor still applies. Threshold speeds are strictly greater; angle limits are inclusive. Undefined speeds or angles cannot match. Duplicate timestamps give undefined speed. Each individual / train-test track is ordered chronologically; endpoints lack the two adjacent steps needed for a spike. No extra time-gap or burst boundary rule has been added to the existing movement-feature convention.

With edits, substitute the chosen lower/upper speeds and angle limits in the same rule. The lower speed floor always applies, even if the upper cutoff is below it. `validate_speed_spike` checks overrides; `apply_threshold_overrides` applies them without replacing the original distribution or species-cap record; `candidate_stage` uses the effective cutoffs.

Confirmed exclusions are removed before rebuilding steps. When confirmations change while the map stays open, the speed controls refresh numerical values for the affected individuals without replacing their retained map geometry or resetting the view. Unresolved flags remain in the speed population. Stationarity still skips saved GPS-spike flags through its existing dependency logic. Quantiles include every finite nonnegative retained outbound speed exactly once, before angle filtering. They are linear quantiles, equivalent to R `quantile(..., type = 7)`. The IQR button computes Q3 + 100 × (Q3 − Q1).

Review these functions:

- `examples/movement/movement_features.py`: `step_movement_metrics`, `centered_turn_angle_degrees` (existing distance/time and bearing-change formulas).
- `examples/movement/speed_spikes.py`: `track_metrics`, `study_distribution`, `candidate_stage`, `species_cap`, `resolve_speed_spikes` (shared CSV/RDS calculation).
- `examples/movement/speed_spikes.py`: `speed_histogram` (exact linear/log bin counts); `examples/movement/static/app.js`: `speedHistogramMarkup` (display and cutoff markers only).
- `examples/movement/static/app.js`: `thresholdQuantiles`, `setSpeedIqrPreset` (editable preview presets).
- `examples/movement/species_speed_caps.csv`: all supplied fields from species.csv, unchanged. One additional `movebank_study_id` column explicitly links 481458 to Bildstein, verified against the pilot-v3 Car Talk source manifest; that RDS lacks species metadata. Exact study key is preferred, then this known ID, then unique species. Unknown/ambiguous lookups show an error.

Saved filter definitions include actual cutoffs, sample count, angle limits, selected groups, implementation identity, and the supplied species-cap row / file hash / lookup method when relevant. New physiology steps also record `species_cap_field: vmax_ci_lower`. Replaying a saved step uses its frozen cutoffs. Older physiology steps with a recorded distribution but no `species_cap_field` continue to use their recorded `v_max_m_s`; they are not silently converted to the new default. Historical distance-based filter definitions retain their original semantics.

## Validation

Targeted tests cover exact boundary values, unequal time intervals, exclusions, duplicate times, study-wide scope, CSV/RDS agreement, bundled CSV replay, RDS export rows, missing caps, Car Talk's missing species metadata, browser previews, group selection, zero-result saves, post-confirmation percentile refresh without a map reset, IQR presets, and retained old distance filters. Existing stationarity, review and RDS export regression tests also pass.

Editable-threshold tests cover invalid values, exact custom speed and angle boundaries, frozen replay, original cap/distribution provenance, CSV/RDS preview/save parity, reset, selected-group preservation, histogram marker updates, and clearing edits on study changes.

A read-only benchmark against the existing 1,239,130-fix Bildstein index, without review exclusions: first whole-study quantile filter 12.99 s, maximum process RSS 221 MB; subsequent Car Talk calculation with cached study cutoffs 0.14 s. These are server calculation times on this container, excluding index construction and browser loading. No user review state or raw data was changed; the running user server was left alone.
