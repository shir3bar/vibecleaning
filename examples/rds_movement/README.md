# RDS Movement

This thin application wrapper reuses the movement review backend and frontend
with per-individual AMT track_xyt or move2/sf RDS inputs grouped into one project per study.
It treats `burst_` as authoritative, exposes `is_outlier` for color and ranking,
and keeps review state in schema-v6 lineage sidecars. The SQLite files under
the configured local cache root are disposable fix-level indexes and rebuild
automatically when the RDS bundle changes or the cache is deleted.

[Study metadata and AMT review](../../docs/STUDY_METADATA.md) describes optional
per-study `metadata.json`, species and deployment details, invalid-position
review, and exports that preserve the input's native R class.
[Study prefiltering](../../docs/STUDY_FILTERS.md#pre-filter-an-annotators-folder)
explains how to run the ordered filters on each annotator's local `movement_rds` folder.

## Install and run

Use the [Windows](../../docs/START_HERE_WINDOWS.md) or
[Mac](../../docs/START_HERE_MAC.md) guide for setup, account creation and daily
startup with the RDS launcher. [Linux setup](../../docs/ENVIRONMENT.md#open-the-movement-review-app)
uses the same uv workflow. The default address is `http://127.0.0.1:8422`.

The included studies are already organized for the app; importing is not a
first-time setup step. For development, complete the
[development setup](../../docs/ENVIRONMENT.md#development-and-tests), including
account creation, then run from the repository root:

```bash
uv run --locked python examples/rds_movement/server.py
```

## Import other flat RDS folders

To group another folder of flat RDS files into studies and build its disposable
indexes, use the development environment and replace the source path below:

```bash
uv run --locked python -m examples.rds_movement.import_studies /path/to/rds-files --family-dir data/movement_rds --build-index
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
