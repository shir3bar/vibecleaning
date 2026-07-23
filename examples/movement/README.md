# Movement Outlier Review Example

This example app is the full movement-ecology development playground layered on
top of Vibecleaning's generic DAG engine. The user-facing restricted profile is
[`examples/slim_movement/`](../slim_movement/README.md); both applications reuse
the routes and viewer implemented here.

It adds:

- a movement-owned family/study catalog on top of nested study lineage roots
- a movement-specific summary route that exposes fix-level data for map review
- point coloring by GPS/data-quality fields and derived movement metrics
- sidecar-backed fix, segment, burst, and individual suspicion annotations
- issue-linked confirmation as a persistent dataset step
- restoration of compatible saved burst analyses
- reviewed CSV export without mutating the source CSV
- report generation as an analysis
- analytical exclusion of confirmed fixes with an optional map audit layer

## Run

```bash
python examples/movement/server.py
```

## Routes

- `GET /api/apps/movement/families`
- `GET /api/apps/movement/family/{family}/studies`
- `GET /api/apps/movement/family/{family}/study/{study}/state`
- `GET /api/apps/movement/family/{family}/study/{study}/graph`
- `GET /api/apps/movement/family/{family}/study/{study}/dataset/{dataset_id}`
- `GET /api/apps/movement/family/{family}/study/{study}/dataset/{dataset_id}/summary`
- `GET /api/apps/movement/family/{family}/study/{study}/analyses`
- `POST /api/apps/movement/family/{family}/study/{study}/actions/annotate-scope`
- `POST /api/apps/movement/family/{family}/study/{study}/actions/confirm-issues`
- `POST /api/apps/movement/family/{family}/study/{study}/actions/export-reviewed-csv`
- `POST /api/apps/movement/family/{family}/study/{study}/actions/annotate-fixes`
- `POST /api/apps/movement/family/{family}/study/{study}/actions/generate-report`
- `POST /api/apps/movement/family/{family}/study/{study}/actions/remove-confirmed-fixes`
- `POST /api/apps/movement/family/{family}/study/{study}/undo`

## Data layout

Movement sample data is organized into three top-level family folders:

- `data/movement_raw/`
- `data/movement_clean/`
- `data/movement_hightemporalres/`

Each direct child study folder is its own lineage root and owns its own `.vibecleaning/`.
If a study has multiple CSVs, keep them together in the same study folder so they intentionally share lineage.

The starter app and trajectory example do not use this nested study catalog. They stay on the generic top-level `data/<project>/` contract.

## Workflow

1. Choose a family, then choose a study.
2. Load a version and trajectory CSV artifact.
3. Color fixes by a GPS/data-quality or movement-derived field.
4. Click fixes to build a review selection.
5. Mark the selection as `suspected`, recording its issue type and provenance.
6. Confirm selected fixes against one or more of their originating suspected issues.
7. Continue analysis on the remaining fixes. Confirmed rows stay in the derived CSV
   with `visible=false`, retain their review lineage, and remain available in the
   confirmed-exclusions map layer.
8. Generate an owner-facing report or export the reviewed CSV.

Confirmation does not start a new burst by itself. Movement features are
recomputed across the remaining fixes, and statistical burst assignment is
recalculated from those transitions. Source/fixed-time burst columns remain
unchanged for audit purposes.
