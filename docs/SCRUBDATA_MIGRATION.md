# Upgrading to Scrub Data

1. **Stop every old app instance using the data folder**, including other
   computers on a shared drive. Keep them stopped throughout the upgrade.
2. Update the app with `git pull --ff-only` and start it using your existing
   Windows or Mac launch command.
3. Log in and open your study. Check your saved decisions and one report/export.

The app converts `.vibecleaning/` to **`scrubdata/`** when it first encounters
the old folder. This covers the account registry at the data root and each
study's review history. Allow extra time and disk space for a copy of that
history on the first opening.

Your original history is kept beside it as **`.vibecleaning-backup/`**. Raw input
files, saved scripts, notes, decisions, dataset identifiers and output contents
are preserved. The conversion updates the file references inside history
records and execution specifications. Future work uses `scrubdata/` only.

An interrupted conversion can be retried: a partial copy is rebuilt from the
original, or a completed copy is published. If both `.vibecleaning/` and
`scrubdata/` exist, the app stops with an error rather than combining histories.
Keep both folders and investigate which one contains the work you need.

## Existing commands and scripts

The repository URL, launcher filenames, ports, launcher options, cache locations
and existing `VIBECLEANING_*` deployment settings still work. Movement-app browser
preferences and the review-state format are preserved. The displayed app name is Scrub Data.

New cleaning and report scripts use `SCRUBDATA_SPEC_PATH` and
`SCRUBDATA_SUMMARY_PATH`. The runner supplies the old names too, pointing to the
same files, so historical scripts can still execute unchanged.

## Rollback

Stop all app instances first. Keep a separate copy of the whole data folder.
For every converted folder, move `scrubdata/` aside and rename
`.vibecleaning-backup/` back to `.vibecleaning/`, then use the old app version.
The backup contains the state **before conversion**; later review work remains
in the `scrubdata/` folder you moved aside. Do not delete that folder.

For the pre-upgrade code version from this session, use commit `0edc063`.
It was pushed to `shir3bar/vibecleaning` before the rebranding work began.
