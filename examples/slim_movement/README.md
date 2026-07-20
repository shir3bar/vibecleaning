# Slim Movement

`slim_movement` is the user-facing movement review application. It reuses the
movement example's backend and frontend components with a restricted profile:

- only studies under `data/movement_raw/`
- one raw movement CSV per dataset, selected automatically (OSM-context and
  reviewed exports are ignored)
- all non-OSM color-by fields and all basemap choices
- fix, segment, burst, and individual review annotations
- reviewed CSV export and owner report generation
- automatic restoration of the latest compatible burst ranking
- no artifact, train/test, candidate-query, ranking-feature, OSM-derived
  feature, or burst feature-space controls
- a draggable divider for resizing the Individuals and Checked fixes lists

The full `examples/movement/` application remains the development playground.

## Run

```bash
python examples/slim_movement/server.py
```

The default address is `http://127.0.0.1:8421`.
