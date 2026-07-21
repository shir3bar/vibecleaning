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
- overview-first loading: opening a table loads individual counts but no map
  fixes until individuals are explicitly selected
- lightweight review overlays, so selecting or flagging one individual does
  not rebuild every loaded point and track
- explicit manual flagging of fixes, two-click track segments, or entire
  individuals as reproducible Vibecleaning graph steps
- a compact history control for loading earlier datasets or undoing the current
  step
- export of manually flagged fixes as CSV, recorded as a graph analysis

The uploaded database is fingerprinted and imported as the immutable
`source.sqlite` artifact of an internal `data/move_viz_<fingerprint>/` project.
Flag and unflag operations create new datasets whose versioned
`move_viz_review_annotations.json` sidecar records the affected rows, scope,
reviewer, comment, timestamp, and step ID. The SQLite artifact is reused by
reference and is never modified. Reopening the same database resumes its graph
without presenting a project selector.

CSV export runs against the selected dataset and is saved as a graph analysis,
including its user, script, specification, summary, and output artifact. Export
does not create a new dataset because it does not change the reviewed data.

`move_viz` has no anomaly detection, OSM feature queries, or report workflow.

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

You can also click **Load bundled example** to open the same database directly
from the running server without a browser file upload. The header displays
`client protocol 6`; if it does not, restart the server and hard-refresh the
page.

The example contains the 4,800 rows from
`data/movement_raw/synthetic_demo_cp2/synthetic_demo_cp2.csv`. Regenerate it
with:

```bash
python examples/move_viz/scripts/create_sample_sqlite.py
```

The browser reads only the SQLite header before uploading the original file,
instead of buffering a second full copy in JavaScript. After upload, table
opening returns a small overview and leaves the map empty. Selecting one or
more individuals requests only their fixes; changing the selection cancels any
obsolete request.

By default each map page loads at most 100,000 rows and the server accepts
databases up to 512 MB. When more matching fixes exist, **Load more fixes**
appends the next page without restarting the server or clearing selected review
fixes. Entire-individual review remains disabled until every page for the
current selection is loaded, so a partial track cannot be mislabeled as the
whole individual. Override the page and upload limits with `MOVE_VIZ_MAX_ROWS`
and `MOVE_VIZ_MAX_UPLOAD_BYTES` when appropriate.
