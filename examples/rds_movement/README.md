# RDS Movement

This thin application wrapper reuses the movement review backend and frontend
with per-individual move2/sf RDS inputs grouped into one project per study.
It treats `burst_` as authoritative, exposes `is_outlier` for color and ranking,
and keeps review state in schema-v6 lineage sidecars. The SQLite files under
`scrubdata/cache/movement/` are disposable fix-level indexes and rebuild
automatically when the RDS bundle changes or the cache is deleted.

Import the flat sample folder and build its disposable SQLite indexes:

```bash
uv run python -m examples.rds_movement.import_studies data/movement_rds --build-index
```

Run the app on its default port:

```bash
uv run python examples/rds_movement/server.py
```

The included import currently contains:

| Study | Individuals/files | Fixes |
| --- | ---: | ---: |
| `268904527` | 48 | 8,148 |
| `481458` | 71 | 1,239,130 |

The original flat files remain in `data/movement_rds/`; the launchable projects
are the two study subdirectories. **Export RDS** saves one `<source>_cleaned.rds`
per source individual directly in `<study>/scrubdata/cleaned_files/`, alongside
`writer_manifest.json` with the source dataset, export ID and file checksums.
The app shows progress and the saved folder path. This is a folder on the machine
running the app; there is no browser download.

Each successful export replaces the previous completed export in `cleaned_files`.
Files are validated before replacement, and a failed write preserves the previous
export. Original rows and RDS metadata are preserved, with the existing review
columns attached. `_cleaned` filenames can be imported back into the app.
Each export also retains its files in its analysis outputs for reproducibility.
