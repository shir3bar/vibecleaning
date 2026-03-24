# Movement Outlier Review Example

This example app is a movement-ecology workflow layered on top of Vibecleaning's generic DAG engine.

It adds:

- a movement-owned family/study catalog on top of nested study lineage roots
- a movement-specific summary route that exposes fix-level data for map review
- point coloring by GPS/data-quality fields and derived movement metrics
- persistent fix annotation as `suspected` or `confirmed` outliers
- report generation as an analysis
- confirmed-fix removal as a step

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
5. Mark the selection as `suspected` or `confirmed`.
6. Generate an owner-facing report for selected reviewed fixes.
7. Remove confirmed fixes in a later step when appropriate.
