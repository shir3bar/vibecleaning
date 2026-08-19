# Movement Burst Map Rendering Plan

## Scope

This work changes how automatic bursts are drawn on the live map in
`examples/movement/static/app.js`. Two problems drive it:

- The burst overlay colored paths by `burstIdx`, which destroyed individual
  identity. `burstIdx` restarts at zero for every track, so every individual's
  first burst shared one color, every second burst shared another, and so on.
- A focused ranking burst was drawn by four extra layers stacked on top of the
  muted burst layer, so the focused burst rendered twice, from two different
  geometry sources.

`examples/slim_movement/server.py` serves this same `app.js`, so every change
here reaches the user-facing review app as well as the development playground.

This pass is frontend only. No backend routes, no lineage behavior, and no
changes under `app/`.

## Design

### Burst identity

The map does not need to say *which* burst a path is. It needs to show where
burst boundaries fall while keeping individual identity readable. Identity of a
specific burst is answered by hover, click, and the table.

- Burst path fill color comes from `individualPalette`, matching base tracks,
  fix points, the legend, and the individuals list.
- A casing layer draws beneath the fill layer at a greater width in a dark
  constant color. Because consecutive bursts are separate polylines, adjacent
  bursts render as two dark-edged lines rather than one continuous stroke. The
  seam is the boundary.
- Existing start and end endpoint markers reinforce the boundary.

Width, alpha, and dash patterns are deliberately not used to enumerate bursts.
A long track can hold more bursts than any of those channels can encode, and
those channels are needed for source-flag state and focus.

### Focus

Focus is a value inside the accessors of the existing burst layers, not a
parallel layer set. The casing layer carries it: a neutral casing means "a
burst", an accent casing means "the focused burst".

Fix-level emphasis for the focused burst is a stroked-only ring layer. It must
not recolor fixes, because fix color already encodes individual or the selected
color-by variable.

### Performance

Adding a casing layer doubles path tesselation for bursts. That cost is paid
only when burst data changes, not per render, because:

- `autoBurstRenderCache` holds identity-stable `pathItem` objects.
- `visibleAutoBurstPaths` is rebuilt each render but contains those same object
  references.
- `dataComparator: sameArrayItems` compares element identity, which is
  `O(bursts)` rather than `O(points)`, so deck.gl skips re-tesselation.

The casing layer must receive the same array reference and the same comparator
as the fill layer to inherit this. That is the correctness requirement to check
in review.

No point-count threshold guards the casing. A threshold would remove boundary
information exactly when the map is most crowded and boundaries matter most,
and it would introduce an invisible mode change.

## Frontend Changes

### Burst render cache

- `autoBurstRenderCache` colors `pathItem` from `data.individualPalette` via
  `splitColor`, with the established `[124, 210, 255]` fallback for individuals
  that arrive later through candidate fixes.
- `autoBurstColor` is retired from map rendering. The burst preview card uses
  the same individual-palette helper so the card and the map agree.

### Layers

Before this pass the map used six burst-related layers:

- `movement-auto-bursts`
- `movement-auto-burst-endpoints`
- `movement-focused-ranking-burst-path-outline`
- `movement-focused-ranking-burst-path`
- `movement-focused-ranking-burst-points`
- `movement-focused-ranking-burst-markers`

After this pass it uses four:

- `movement-burst-casing`
- `movement-bursts`
- `movement-auto-burst-endpoints`
- `movement-burst-focus-ring`

The four focused-ranking-burst layers are removed. Focus is expressed through
`updateTriggers` keyed on the focused burst id, so color attributes rebuild only
when focus actually changes.

### Source-flag state

The focus branch previously short-circuited before the `sourceFlagged` branch,
so focusing any burst erased the source-flag color distinction on every other
burst. Focus and source-flag state now compose instead of overriding.

### Base-track suppression

`suppressedBaseTrackKeys` derives from the filtered burst list that requires at
least two positions. Previously it derived from all visible bursts, so a track
whose bursts were single fixes lost its base track and drew no burst path,
leaving disconnected dots and no track.

### Picking

- Burst paths are pickable unconditionally. Previously pickability depended on
  whether a burst feature-space analysis had been run, which made bursts inert
  in slim mode, where feature space is hidden entirely.
- Endpoint markers are pickable.
- `handleMapClick` gains an explicit burst branch, ordered after the
  feature-space branch and before the fix branch, taken only when the pick
  carries no `fixKey`. Clicking a burst path or endpoint marker focuses that
  burst.

Feature-space selection is unaffected. `getMapPickedFeatureSpaceBurst` guards
independently on the active sheet, on loaded feature-space points, and on each
picked burst resolving to a feature-space point. None of those guards depend on
layer pickability.

`handleMapContextMenu` is unchanged. A burst pick carries no `fixKey`, so it
closes the popup exactly as it did when burst layers were not pickable.

### Burst counter

`renderBurstCountIndicator` counts with `requireOverlay: true` so it stops
reporting bursts as visible while the overlay checkbox is off.

## A Latent Inconsistency, Not A Current Bug

In `build_movement_fixes`, `auto_burst_records` is populated before both the
review-status filter and the returned-fix cap, so burst paths cover every record
in scope while the returned `fixes` list does not. Because visible bursts
suppress the plain track rather than layering over it, a divergence between the
two would silently change which path a reviewer sees.

That divergence is not reachable today:

- The cap defaults to `DEFAULT_FIX_LIMIT`, one million fixes, and the main
  detail load sends no explicit limit. It is a runaway guard, not a routine
  truncation, so the fix-cap message in the UI is effectively unreachable.
- The review-status filter belongs to the separate suspicious and confirmed fix
  loads, not the detail load. That path blanks `auto_bursts` and `segments`
  outright, and the client reads only `fixes` from it.

The ordering is worth knowing about because it would become a real bug the
moment the cap were lowered or `review_status` were wired into the detail load.
It needs no change now.

The downsampling of `series_by_individual` to `MAX_SERIES_POINTS` is likewise
not a live problem. That fallback applies only when the overview is truncated,
and the server returns no overview bursts in that case, so there is nothing to
swap between.

## Verification

Automated checks follow the existing convention of asserting against `app.js`
source text in `tests/test_movement_fixes.py`:

- the four focused-ranking-burst layer ids are absent
- the casing, fill, and focus-ring layer ids are present
- burst fill resolves through `individualPalette`
- the casing layer declares `dataComparator` and `updateTriggers`

Manual verification checklist:

- With five or more individuals visible and bursts on, each track reads in its
  own palette color and burst boundaries are visible as casing seams.
- Focus a burst from the ranking sheet. Exactly one accent-cased path appears,
  with no second overdrawn copy, and source-flagged bursts elsewhere remain
  visually distinct.
- The focus ring marks the focused burst's fixes without changing their color
  under any color-by selection.
- A track whose bursts are single fixes still draws its base track.
- Click a burst path with no feature-space analysis loaded and confirm it
  focuses the burst.
- Open the feature-space sheet in the full app, click a burst on the map, and
  confirm feature-space selection still works as before.
- Turn the burst overlay off and confirm the burst counter reports zero visible.
- Repeat the core checks in the slim app, which serves the same `app.js`.

## Assumptions

- Casing is the only outline technique available, since deck.gl `PathLayer` has
  no stroke. The removed focus layers already used this technique.
- Burst identity on the map is carried by boundaries plus individual color, not
  by per-burst color.
- Focus applies to at most one burst at a time.
