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

Before first startup, bootstrap an editor and add reviewer accounts. Passwords
are prompted without echo and stored only as salted scrypt hashes:

```bash
uv run python -m app.auth_cli bootstrap admin --display-name "Review Administrator"
uv run python -m app.auth_cli add reviewer1 --display-name "Taylor Reviewer" --role reviewer
```

Additional operator commands are `list`, `enable`, `disable`, and
`reset-password`; pass `--data-root` before the command when using a data folder
other than `data/`. Restart the app after changing accounts because the registry
is intentionally loaded only once at startup.

Login creates an opaque in-memory session represented by an `HttpOnly`,
`SameSite=Strict` cookie. Restarting the server logs everyone out. The login page
and static assets are public; datasets and all review, analysis, report, and
export routes require authentication.

Editors assign studies from the study header. The assigned reviewer can edit the
active review; editors take explicit control before intervening and release it
when finished. Full role rules and concurrency semantics are documented in
[`docs/multi-user-movement-review-design.md`](../../docs/multi-user-movement-review-design.md).

## Sharing securely

For internet or institution-wide sharing, put the app behind a
Cloudflare Tunnel, another HTTPS reverse proxy, or the institution's
authenticated TLS ingress. Prefer keeping the application bound to `127.0.0.1`
and letting that proxy connect to port 8421.

Set `VIBECLEANING_SECURE_COOKIE=1` when users access the public app over HTTPS.

Do not expose `http://<host>:8421` directly to the public internet. If the local
deployment requires `HOST=0.0.0.0`, firewall the port so only the HTTPS proxy
can reach it.

Sessions exist only in server process memory. Account hashes live in
`data/.vibecleaning/users.json`; keep that operator-owned file private and do not
commit it.
