# Running the tests

From the cloned repository, install the locked environment and browser once:

```sh
uv sync --locked
uv run playwright install chromium
```

On Linux, also run `uv run playwright install-deps chromium` if the browser's
system libraries are missing. Windows and Mac do not need that command.

Run everything:

```sh
uv run python -m pytest -q -ra
```

Allow several minutes. Tests use temporary studies; they do not edit your
review data or need a running app. The RDS fixtures are included in Git.

To run just one group:

```sh
uv run python -m pytest -q -m "not browser"
uv run python -m pytest -q -m browser
```

The first command covers filters, review history, accounts and locking, reports,
exports, cache recovery, shutdown, and small JavaScript behavior checks. Node is
taken from Playwright when it is not separately installed.

The browser group starts local servers and exercises CSV and RDS loading, study
switching, queue review, Confirm/Unflag, playback, stationarity and reruns. It uses
real map rendering with software WebGL and placeholder basemap tiles, so external
tile servers are not needed. Check actual basemap appearance during the human
pilot. Browser startup failures fail the suite and include JavaScript, console
and request errors in the test output.

Only these checks may skip: the Windows-specific default-path test on other
platforms, and the optional R export-engine cases when `Rscript` is absent. The
Python RDS export tests still run. Missing required fixtures or Chromium are
failures, not skips.

## GitHub

CI is manual-only: pushes and pull requests do not start it. Open the repository's
**Actions → test → Run workflow**, select your branch, and run it when useful.
GitHub only shows this button once the workflow with `workflow_dispatch` is on
the default branch.

Each run tests Windows, Mac and Linux with locked Python dependencies. The run
log includes failures and slow tests; a `test-results-<platform>` artifact keeps
the results. This complements the human deployment checks in
[P0_ACCEPTANCE.md](P0_ACCEPTANCE.md).

Functional tests wait for the app to become ready. The CSV/RDS progressive-load
tests also record preview timing without imposing a speed target on arbitrary
VMs. Set `VIBECLEANING_PREVIEW_BUDGET_MS` only when intentionally measuring a
budget on known hardware.

## What was cleaned up

The September 2026 audit removed 67 tests that checked exact JavaScript/CSS text,
helper names, or the presence/absence of old controls. It retained functional
tests and architectural checks, and connected the existing JavaScript step-label
test to pytest so it runs with the rest of the suite.

The [latest pre-cleanup GitHub failure](https://github.com/shir3bar/vibecleaning/actions/runs/36321061098)
was one Windows browser test timing out
before a map canvas appeared. Its 435 other tests passed; Linux and Mac passed.
That log did not establish the cause of the missing canvas. The browser harness
now uses software rendering, avoids external basemap requests, allows startup
time separately from performance measurements, and reports browser errors.
A new Windows run is still needed to verify the result there.

Validation on 28 September 2026: a fresh copy containing only tracked files and
these changes, with a new locked Python 3.11.16 environment on Linux, passed
423 tests in five minutes. The only skip was the Windows default-path check.
All 25 browser cases passed, including Car Talk and CSV/RDS stationarity reruns.
