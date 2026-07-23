# move_viz Agent Handoff

This document is for an agent adapting `move_viz` to another SQLite movement
database. The goal is to extend schema compatibility without turning the app
into the full `movement` playground or breaking reproducibility and large-file
behavior.

## Read first

Read these files before changing code:

1. [`AGENTS.md`](../../AGENTS.md) for repository rules.
2. [`docs/ENVIRONMENT.md`](../../docs/ENVIRONMENT.md) for installation and
   verification.
3. [`README.md`](README.md) for the current product behavior.
4. [`docs/STATE_MODEL.md`](../../docs/STATE_MODEL.md) and
   [`docs/EXECUTION_CONTRACT.md`](../../docs/EXECUTION_CONTRACT.md) for graph
   persistence rules.
5. [`routes.py`](routes.py), [`static/app.js`](static/app.js), and
   [`tests/test_move_viz.py`](../../tests/test_move_viz.py) for the actual
   contract.

Use `uv sync --locked` for the canonical agent environment. If dependencies
change, commit `pyproject.toml` and `uv.lock` together and keep the legacy
`requirements.txt` list aligned.

The other movement applications have different roles:

- `examples/movement/` is the development playground.
- `examples/slim_movement/` is the user-facing CSV review application.
- `examples/move_viz/` is the lightweight direct-SQLite viewer.

Do not add project browsing, anomaly detection, OSM feature queries, or report
generation to `move_viz` unless the user explicitly changes its product scope.
Basemap tiles are part of the viewer; OSM feature-query tooling is not.

## Current product contract

`move_viz` must continue to provide:

- direct browser selection of one SQLite artifact, without a project picker;
- automatic movement-table and column detection;
- all compatible columns as color-by choices;
- individual filtering, paged loading, tracks, points, and basemaps;
- fix, segment, and individual manual review;
- a compact but real Vibecleaning data graph with history and undo;
- graph-recorded CSV export; and
- no mutation of the selected SQLite source.

The current client/server protocol is defined by `MOVE_VIZ_PROTOCOL` in both
`routes.py` and `static/app.js`. Bump both values, update the README and test,
and require a hard refresh whenever an API or payload change would make an old
client unsafe with a new server.

## SQLite input contract

A compatible table needs detectable longitude and latitude columns. The other
roles are optional but strongly recommended:

| Role | Current aliases | Effect when absent |
| --- | --- | --- |
| Longitude | `location-long`, `longitude`, `lon`, `lng`, `long`, `x` | Table is incompatible |
| Latitude | `location-lat`, `latitude`, `lat`, `y` | Table is incompatible |
| Timestamp | `timestamp`, `time`, `datetime`, `date_time`, `recorded_at` | Source/row order is used |
| Individual | Movebank-style identifiers, `individual`, `track_id`, `animal_id` | Rows appear as `All fixes` |
| Event ID | `event-id`, `eventid`, `event_id`, `id`, `fix_id` | Stable row number remains in the key |

Alias matching normalizes punctuation and case. Extend `COLUMN_ALIASES` for a
new general synonym; do not hard-code a dataset filename, table name, or one-off
column mapping. Add a test fixture that uses the new names.

All non-coordinate columns remain eligible for color-by. `_column_kind()` uses
the SQLite declaration first and samples values otherwise. Preserve this broad
behavior when adapting a schema.

Rows with non-numeric coordinates or coordinates outside longitude
`[-180, 180]` and latitude `[-90, 90]` are skipped from visualization. Do not
silently swap coordinates or infer a projection. Surface an explicit mapping
or conversion decision if a new database is not WGS84 longitude/latitude.

For good large-table performance, a source database should preferably have an
index beginning with the individual column and then timestamp, for example:

```sql
CREATE INDEX movement_individual_time
ON movement ("individual-local-identifier", "timestamp");
```

Never add that index to the uploaded raw artifact from the app. If an index is
needed, it belongs in the upstream database-building process or in an explicit,
user-approved derived artifact.

## Row identity and pagination

Review annotations depend on stable row keys. For ordinary SQLite tables,
`move_viz` builds keys from the immutable `rowid`, optionally prefixed by the
event ID:

```text
event:<event-id>#row:<rowid>
row:<rowid>
```

`event-id` alone is not assumed unique. Do not change key construction in only
one location. Loading, validation, review sidecars, and export must reconstruct
the same key.

Tables declared `WITHOUT ROWID` use their stable ordered position as a
fallback. If a new target database uses `WITHOUT ROWID`, add explicit tests for
its primary key, timestamp ties, multiple pages, later-page review, and export
before claiming support. Prefer a primary-key-based identity improvement over
adding another dataset-specific special case.

Detail responses are ordered by timestamp and `rowid`, then paged with `LIMIT`
and `OFFSET`. The response includes `offset`, `next_offset`, `has_more`, and
`matching_row_count`. Later pages must have keys disjoint from earlier pages,
and a later-page key must pass `validate_movement_row_keys()` before a graph step
is created.

## Large-dataset invariants

These safeguards were added after testing a 659,770-fix Kays database. Treat
them as regression boundaries:

- The browser reads only the 16-byte SQLite header before sending the original
  `File`; do not restore `file.arrayBuffer()` for the complete database.
- Opening a table returns an overview and zero map rows.
- No fixes load until the user selects individuals.
- A changed selection cancels the obsolete detail request.
- Pages default to 100,000 source rows and append through **Load more fixes**.
- `values` is a compact array aligned with `value_columns`; do not repeat every
  column name in every row.
- Appending a page preserves selected review fixes.
- Entire-individual review stays disabled while `has_more` is true.
- `handleMapClick()` updates indexed review overlays and must not call the full
  `renderData()` path.
- Selecting or flagging an individual uses a track overlay rather than
  regenerating every loaded point.
- Basemap style replacement must reconstruct base sources and all review
  overlays after `style.load`.
- CSV export queries flagged `rowid` values in chunks when possible and streams
  the fallback; it must not `fetchall()` the entire source table.

The relevant limits are:

```text
MOVE_VIZ_MAX_UPLOAD_BYTES  default 512 MiB
MOVE_VIZ_MAX_ROWS          default 100,000 rows per map page
MOVE_VIZ_MAX_REVIEW_ROWS   default 250,000 fixes per graph step
```

Loading every page is an explicit user choice and can still produce a heavy
browser map. Keep each network request bounded and avoid full-source redraws for
selection-only changes.

## Reproducibility and graph invariants

Opening a database fingerprints its bytes and creates or resumes:

```text
data/move_viz_<first-16-sha256>/
  source.sqlite
  .vibecleaning/
```

`source.sqlite` is the immutable raw artifact. The temporary upload session is
not lineage; the fingerprinted project is. Reopening byte-identical SQLite data
must resume the same graph head and annotations.

Flag and unflag operations are persistent changes and therefore call
`create_step()`. Each step:

- declares `source.sqlite` and the prior annotation sidecar when present;
- writes a new `move_viz_review_annotations.json` output;
- creates a child dataset and sets it as the head;
- records user, script, spec, parameters, summary, and step ID; and
- reuses `source.sqlite` by reference instead of rewriting it.

The sidecar schema is owned by [`review_step.py`](review_step.py). Each flag
currently records row key, comment, scope, user, creation time, and the step
that wrote the annotation. Keep issue categories and free-form comments
separate if the schema grows.

CSV export is read-only exploratory output, so it calls `create_analysis()` via
[`export_flags_analysis.py`](export_flags_analysis.py). It must not create a new
dataset. The analysis records its user, source dataset, script, spec, summary,
and CSV output.

Do not move review state to `localStorage`, mutate SQLite outlier columns, or
write lineage files manually. `localStorage` is currently used only to remember
the reviewer name.

Source columns named `manually-marked-outlier` and
`algorithm-marked-outlier` are provenance, not automatic app review. They may
affect map styling and export comments, but they must not automatically create
`suspected` status or graph flags.

## Code ownership map

| File | Responsibility |
| --- | --- |
| `server.py` | App construction, port, static assets, bundled sample |
| `routes.py` | SQLite inspection, schema detection, paging, sessions, graph actions |
| `static/app.js` | Direct-file workflow, map, paging, selection, graph/history UI |
| `static/style.css` | Lightweight viewer layout and controls |
| `review_step.py` | Reproducible flag/unflag sidecar transform |
| `export_flags_analysis.py` | Reproducible flagged-row CSV analysis |
| `scripts/create_sample_sqlite.py` | Explicit CSV-to-SQLite sample builder |
| `tests/test_move_viz.py` | Backend, graph, export, paging, and frontend invariants |

Keep domain-specific code here. Change `app/` only for genuinely generic
lineage or execution behavior used beyond `move_viz`.

## Safe adaptation workflow

1. Inspect the candidate without modifying it:

   ```bash
   sqlite3 path/to/candidate.sqlite '.tables'
   sqlite3 path/to/candidate.sqlite 'PRAGMA table_info(movement);'
   sqlite3 path/to/candidate.sqlite 'SELECT COUNT(*) FROM movement;'
   sqlite3 path/to/candidate.sqlite 'SELECT * FROM movement LIMIT 3;'
   ```

2. Record the movement table, role mappings, coordinate system, row count,
   individual count, largest individual, timestamp representation, presence of
   `rowid`, indexes, and source outlier columns.

3. First try the database unchanged. If detection fails, add broadly useful
   aliases and focused tests. Do not rename or rewrite the user's raw database
   inside the app.

4. Build a small representative SQLite fixture under pytest temporary storage.
   Include awkward identifiers, source outlier values, at least two
   individuals, and enough rows for a second page when pagination changes.

5. Add tests for overview-only opening, detected mappings, color field kinds,
   stable page keys, later-page flagging, graph reuse, and CSV export.

6. Manually test with a realistically large local artifact. If a large sample
   must be generated, put it in a visible, user-approved path and leave it
   uncommitted unless the user explicitly requests otherwise. Remove temporary
   benchmark artifacts when finished.

7. Exercise this sequence in the browser:

   - upload and verify progress;
   - load the overview with an empty map;
   - select one small and one large individual;
   - append at least one page;
   - switch color-by and basemap;
   - select a fix, segment, and fully loaded individual;
   - flag, unflag, undo, and load an older graph stage;
   - restart and reopen the same database;
   - export CSV and inspect provenance comments.

8. Bump the protocol if payload compatibility changed, restart the server, and
   hard-refresh before judging the frontend.

## Required verification

Run at minimum:

```bash
uv sync --locked
uv run python -m compileall -q examples/move_viz tests/test_move_viz.py
uv run pytest -q tests/test_move_viz.py
git diff --check
```

For changes touching shared movement or graph behavior, run:

```bash
uv run pytest -q \
  tests/test_move_viz.py \
  tests/test_slim_movement.py \
  tests/test_movement_fixes.py \
  tests/test_burst_features.py \
  tests/test_candidate_queries.py
```

Before committing, inspect `git status --short` and stage only intended source
and test files. Large local SQLite samples, generated graph projects, notebooks,
container files, and unrelated user edits must not be swept into the commit.

## Acceptance checklist for a new SQLite dataset

- [ ] The original database bytes are unchanged after viewing and review.
- [ ] The correct table and role columns are detected without filename-specific code.
- [ ] All intended color-by fields appear with sensible numeric/categorical treatment.
- [ ] Opening the table loads no fixes.
- [ ] Individual selection and additional pages remain responsive.
- [ ] Page keys are stable and non-overlapping.
- [ ] Fix, segment, and complete-individual review create graph steps.
- [ ] A later-page fix can be flagged and exported.
- [ ] Reopening the same bytes restores the current graph head.
- [ ] Undo and older-stage loading change flags without deleting descendants.
- [ ] Export is an analysis and contains reviewer comments and source-flag provenance.
- [ ] Basemap changes preserve base data and review overlays.
- [ ] No large generated artifact or unrelated file is committed accidentally.
