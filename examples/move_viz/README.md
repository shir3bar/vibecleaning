# Move Viz

`move_viz` is a lightweight, user-facing SQLite movement viewer. It opens a
local database through the browser instead of presenting Vibecleaning projects
or datasets.

It provides:

- automatic discovery of movement tables and common coordinate/time/individual
  column names
- all compatible SQLite columns as numeric or categorical color-by options
- OSM, satellite, topographic, light, and dark basemaps
- individual filtering and track/point visibility controls
- explicit manual fix flagging, persisted in browser storage by database
  fingerprint
- export of manually flagged fixes as CSV

The uploaded database is copied into a temporary server session and opened
read-only. `move_viz` never writes to the selected SQLite source. It has no
anomaly detection, OSM feature queries, reports, or project selector.

## Run

```bash
python examples/move_viz/server.py
```

The default address is `http://127.0.0.1:8422`.

Click **Browse SQLite** and choose a `.sqlite`, `.sqlite3`, or `.db` file. A
ready-to-use example is included at:

```text
examples/move_viz/sample_data/synthetic_demo_cp2.sqlite
```

The example contains the 4,800 rows from
`data/movement_raw/synthetic_demo_cp2/synthetic_demo_cp2.csv`. Regenerate it
with:

```bash
python examples/move_viz/scripts/create_sample_sqlite.py
```

By default the viewer loads at most 100,000 rows and accepts databases up to
512 MB. Override these with `MOVE_VIZ_MAX_ROWS` and
`MOVE_VIZ_MAX_UPLOAD_BYTES`.
