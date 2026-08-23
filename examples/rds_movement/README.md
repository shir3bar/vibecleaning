# RDS Movement

This thin application wrapper reuses the movement review backend and frontend
with per-individual move2/sf RDS inputs grouped into one project per study.
It treats `burst_` as authoritative, exposes `is_outlier` for color and ranking,
and keeps review state in schema-v6 lineage sidecars. The SQLite files under
`.vibecleaning/cache/movement/` are disposable fix-level indexes and rebuild
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
are the two study subdirectories. Reviewed export produces
`movement_reviewed_rds.zip`, with one RDS per source individual and
`writer_manifest.json`.
