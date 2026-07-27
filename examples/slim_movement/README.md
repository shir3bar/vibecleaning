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

Set one deployment-wide username and a strong password. The server refuses to
start without both values, and passwords shorter than 12 characters are
rejected.

```bash
export SLIM_MOVEMENT_USERNAME=reviewer
read -r -s -p "Slim movement password: " SLIM_MOVEMENT_PASSWORD
echo
export SLIM_MOVEMENT_PASSWORD
uv run python examples/slim_movement/server.py
```

The default address is `http://127.0.0.1:8421`.

The browser will show its standard username/password prompt. This temporary
gate protects the page, static assets, and all API routes with the same shared
credential. It does not yet provide reviewer/editor accounts, dataset
assignments, or editing locks.

To generate a strong password without placing it in shell history:

```bash
uv run python -c "import secrets; print(secrets.token_urlsafe(32))"
```

## Sharing securely

HTTP Basic authentication does not encrypt credentials. For internet or
institution-wide sharing, put the app behind an HTTPS reverse proxy or the
institution's authenticated TLS ingress. Prefer keeping the application bound
to `127.0.0.1` and letting that proxy connect to port 8421.

Do not expose `http://<host>:8421` directly to the public internet. If the local
deployment requires `HOST=0.0.0.0`, firewall the port so only the HTTPS proxy
can reach it.

Credentials are read only when the process starts. To rotate them, stop the
server, export new values, and restart it. Do not commit passwords or store them
in repository files.
