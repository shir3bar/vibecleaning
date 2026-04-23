# Movement Review Upgrades Plan

## Scope

This work extends the movement example app in `examples/movement/` with four linked review improvements:

- richer basemap presets for live map use and report snapshots
- spatial scale bars burned into generated map snapshots
- a right-panel table workspace with `Individuals` and `Table` sheets
- persistent segment flagging for contiguous track ranges

The implementation intentionally avoids adding or expanding automated tests for this pass. Verification is manual.

## Backend Changes

### Segment persistence

- Add `POST /api/apps/movement/family/{family}/study/{study}/actions/annotate-segment`.
- Validate the table-driven payload:
  - `dataset_id`
  - `logical_name`
  - `start_fix_key`
  - `end_fix_key`
  - `selected_fix_keys`
  - `status`
  - `issue_type`
  - `issue_note`
  - `owner_question`
  - `user`
- Create a lineage step that rewrites the selected CSV artifact with segment review metadata populated for every row in the contiguous selected range.

### Segment review columns

The movement CSV review model now includes:

- `vc_segment_status`
- `vc_segment_id`
- `vc_segment_type`
- `vc_segment_note`
- `vc_segment_owner_question`
- `vc_segment_review_user`
- `vc_segment_reviewed_at`
- `vc_segment_refs`

`vc_segment_refs` is the canonical serialized payload for segment membership on a row.

### Summary payloads

- Movement summaries and fix-detail payloads now expose a top-level `segments` array.
- Individual fix records now include resolved `segments` memberships so the UI can style flagged rows directly.
- Segment objects aggregate:
  - identifiers
  - individual and track
  - start and end fix keys
  - start and end timestamps
  - fix count
  - status and issue type
  - note, owner question, review user, reviewed-at
  - ordered fix keys and path geometry

### Overlap and range rules

- Segment creation is restricted to one contiguous range on a single `individual + set_name` track.
- Overlapping active segment flags on the same range are rejected in v1.
- Correction is expected to happen through dataset undo rather than in-place editing.

## Frontend Changes

### Basemaps and snapshots

- Replace the old flat basemap mapping with named presets that carry:
  - live style
  - snapshot style
  - HTML attribution
  - text attribution for rendered images
- Keep `Blank` and `OSM Streets`.
- Add:
  - `Satellite`
  - `Satellite + labels`
  - `Topographic`
- Add a live metric scale control to the interactive map.
- Replace the old snapshot grid helper with a shared snapshot overlay renderer that can draw:
  - coordinate grid
  - metric scale bar
  - attribution footer

### Right-side workspace

- Split the right panel into:
  - `Individuals`
  - `Table`
- Keep the existing selection and checked-fix workflow under `Individuals`.
- Add a dense `Table` sheet with:
  - sticky headers
  - filter input
  - sort controls
  - sort direction toggle
  - `Fix rows` mode
  - `Flagged segments` mode

### Table behavior

`Fix rows` mode:

- uses the current visible-scope detail load
- preserves truncation messaging when the visible detail query hits the fix cap
- allows row click to set an anchor and Shift-click to select a contiguous range

`Flagged segments` mode:

- shows one row per persisted segment in the visible scope
- supports filtering and sorting through the shared table controls
- allows row click or button click to zoom the map to the flagged segment extent

### Segment actions

- Segment creation happens only from the `Table` sheet.
- Valid contiguous selections enable:
  - `Mark segment suspected`
  - `Mark segment confirmed`
- The existing issue modal is reused with segment-specific wording and metadata.
- Persisted segments render back onto the map as emphasized path overlays distinct from fix-level point highlights.

## Reporting Integration

- Report snapshot basemap selection uses the new preset model.
- Auto-rendered snapshots include the scale bar and attribution footer.
- Segment metadata now flows into the issue-first appendix CSV alongside fix-level issue metadata.
- This pass does not add a separate segment-specific report template.

## Manual Verification Checklist

- Switch between `OSM Streets`, `Satellite`, `Satellite + labels`, and `Topographic` on the live map.
- Generate report snapshots and confirm the coordinate grid and scale bar appear together.
- Open `Table`, select a contiguous row range, mark a suspected segment, reload the study, and confirm the segment still appears in both the map and the segment table.
- Click a flagged-segment row and confirm the map zooms to the segment extent.
- Use a wide visible scope and confirm the table still surfaces truncation messaging instead of silently dropping rows.

## Assumptions

- The table lives in the existing right-side workspace.
- Segment creation is table-only, not map-driven.
- Segment persistence uses the main CSV artifact plus a normal lineage step.
- Scale bars are metric.
- Basemap presets use public endpoints that do not require credentials.
