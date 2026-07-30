# Slim Movement

`slim_movement` is the user-facing movement review application. It reuses the
movement example's backend and frontend components with a restricted profile:

- only studies under `data/movement_raw/`
- one raw movement CSV per dataset, selected automatically (OSM-context and
  reviewed exports are ignored)
- all non-OSM color-by fields and all basemap choices
- fix, segment, burst, and individual suspicion annotations
- issue-linked confirmation that retains confirmed rows as invisible analytical
  exclusions and exposes them in a map audit layer
- reviewed CSV export and owner report generation, with browser-rendered map
  captures retained as checksummed analysis inputs
- automatic restoration of the latest compatible burst ranking
- no artifact, train/test, candidate-query, ranking-feature, OSM-derived
  feature, or burst feature-space controls
- no candidate-query, burst feature-space, or OSM-enrichment backend routes;
  the OSM interaction module is not loaded in the browser (OSM raster basemaps
  remain available as ordinary base layers)
- a draggable divider for resizing the Individuals and Checked fixes lists

The full `examples/movement/` application remains the development playground.

## Run

```bash
uv run python examples/slim_movement/server.py
```

The default address is `http://127.0.0.1:8421`.

At startup, the terminal prints a generated temporary login:

```text
Slim Movement temporary login
Username: reviewer
Password: <generated-random-password>
```

Open the app and enter those values in the Vibecleaning login page. The
credential is kept only in JavaScript memory for that browser tab and attached
to protected requests. It is not written to cookies, browser storage, files, or
the data graph. Refreshing or closing the tab, logging out, or receiving an
authentication failure requires another login.

The login page and static JavaScript/style assets are public. Dataset, review,
analysis, report, and export routes all require the credential. Reports and CSV
exports are retrieved through authenticated browser requests; captured report
images are embedded in HTML reports so the fetched report remains viewable.

For a stable shared credential, set either optional override before startup:

```bash
export SLIM_MOVEMENT_USERNAME=reviewer
read -r -s -p "Slim movement password: " SLIM_MOVEMENT_PASSWORD
echo
export SLIM_MOVEMENT_PASSWORD
uv run python examples/slim_movement/server.py
```

An override password must contain at least 12 characters and is never printed.
This temporary gate does not yet provide reviewer/editor accounts, dataset
assignments, or editing locks.

## Sharing securely

The credential is sent in an Authorization header; that header is not encrypted
by plain HTTP. For internet or institution-wide sharing, put the app behind a
Cloudflare Tunnel, another HTTPS reverse proxy, or the institution's
authenticated TLS ingress. Prefer keeping the application bound to `127.0.0.1`
and letting that proxy connect to port 8421.

Do not expose `http://<host>:8421` directly to the public internet. If the local
deployment requires `HOST=0.0.0.0`, firewall the port so only the HTTPS proxy
can reach it.

Credentials exist only in server process memory. To rotate them, stop and
restart the server (and change any override). Do not commit passwords or store
them in repository files.
