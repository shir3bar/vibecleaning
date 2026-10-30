# Study metadata and native AMT review

Put optional `metadata.json` next to a study's RDS tracks:

```text
<annotator>/movement_rds/<study_id>/
  <study_id>_<individual_id>.rds
  metadata.json
```

Select `<annotator>` as the app's data root, including its `movement_rds`
subdirectory. For example, from the application checkout:

```sh
bash scripts/start-vibecleaning.sh --profile Rds --data-root /path/to/sofia
```

The app reads supplied AMT `track_xyt` files directly, as well as existing
`move2/sf` inputs. AMT files need their original `crs_` in EPSG:4326 and the
usual `x_`, `y_`, `t_`, `timestamp`, individual/study IDs, individual identifier,
and `burst_` columns. Numeric IDs retain their 64-bit precision. Source files
are immutable; disposable indexes and map geometry are separate from the R objects.

## Preparing metadata

From the application checkout and its usual uv environment:

```sh
uv run --locked python scripts/export_study_metadata.py /path/to/metadata.rds /path/to/movetraits_Nov2026
```

The second argument contains the annotator folders. The exporter verifies every
track has matching study/individual IDs and a filename in the master table
before writing any sidecar. Repeating the command preserves identical files.
Use `--replace` explicitly to replace different existing metadata.

For a **single annotator's local app data folder** (`./data/movement_rds/<study>`),
give the exact `movement_rds` folder as the second argument:

```sh
uv run --no-sync python scripts/export_study_metadata.py "/path/to/metadata.rds" "./data/movement_rds"
```

The master `metadata.rds` must be copied or accessible separately. The
prefiltering command does not create metadata sidecars. Passing `./data` also
works: its direct `movement_rds` folder takes priority over unrelated nested
projects such as `./data/movebenchmini_rds/movement_rds`. A folder with no study
subfolders containing RDS tracks produces an error rather than zero output.

The version-1 JSON has `study_id`, `source` (master filename and SHA-256),
`units`, and `individuals`, keyed by decimal individual ID. Each individual
contains `individual_id`, `source_file`, `individual_local_identifier`,
`species`, `sex`, and a `deployments` array containing every matching source row.
IDs are strings; missing values are JSON nulls; dates are ISO-8601 UTC.
Source mass is in grams, durations in days, and sampling intervals in minutes.
Legacy `excluded` and other bookkeeping fields are preserved as source information
and do not remove data or create review decisions.

## Reviewing

Species appear in study navigation, the study header, and individual cards.
The header and expandable individual metadata include external scientific-name
search links. The species dropdown also links directly to the Movebank study
using its numeric study ID, even when species metadata is unavailable. Movebank
may require the reviewer to log in or have permission to view the study. These
links require a network only when opened.

Opening the species dropdown also tries to load a small PhyloPic silhouette for
each visible species. Lookups require an exact scientific-name match and an image
assigned specifically to that taxon; ambiguous matches and ancestor silhouettes
are omitted. The image credit links to PhyloPic and its CC0, CC BY, or CC BY-SA
license. Other licenses are omitted. Lookups have a short timeout, at most two run
at once, and results are reused in browser memory. No images are prefetched on
study load or saved in study files, exports, or the application's disk cache.
Offline, failed, or missing lookups leave the existing text and reference links
available. Review and filtering never wait for images.

Individual details show shared values once and retain deployment-specific
differences. Dates and sampling statistics are labelled **Source metadata**:
they describe the source deployment, which may have more fixes or a different
sampling interval than the supplied review track. Free-text life stages retain
their original wording. Missing metadata does not prevent review; malformed or
mismatched metadata produces a visible warning. Existing embedded species are
used when optional metadata is unavailable.

Finite positions outside longitude/latitude bounds remain in tables and exports.
An **invalid positions** button on the individual opens its table with
**Invalid positions only** checked. Longitude and latitude retain their original
values. These fixes support ordinary review actions but have no map or playback
location. Paths and spatial calculations break at invalid positions; source
burst IDs remain intact. Affected bursts have unavailable spatial summaries and
are omitted from anomaly scoring with an explanation. Position validity does
not itself create saved flags or decisions.

## Export and reimport

AMT inputs export as AMT, retaining their R class, attributes, source columns,
types, values, and row order. Export appends or updates `outlier_status`,
`outlier_issue_type`, `outlier_comments`, and `outlier_flag_step_ids`.
Individual decisions remain in `individual_decisions.csv`.

Export retains every input row, including invalid positions. A Remove decision
is recorded for downstream processing; `_cleaned.rds` does not imply physical
row deletion. When valid metadata is available, a JSON snapshot is saved beside
the exported tracks and its checksum is recorded in `writer_manifest.json`.
The snapshot is also an input attachment of the export analysis. Reimport the
exported RDS files and `metadata.json` together inside a study folder.

Metadata refreshes are separate from fix/review identities and do not rebuild
fix indexes. Cache schema upgrades use separately versioned cache filenames and
retain historical source signatures and review state.
