# Application Contract

Vibecleaning has a default starter app at the repo root. New users should usually extend that app in place first.

## Default Starter App

The default starter app is:

```text
server.py
static/
  index.html
  app.js
```

This app should:

- use the generic core APIs directly
- stay easy for an agent to modify quickly
- hold app-specific UI code at the root instead of pushing it into `app/`

If the starter app needs a domain-specific summary route or named action, add it at the app layer and keep the core backend generic.

## Optional Separate Apps

If the user wants a separate app, place it under `examples/<app>/` or another app-owned directory and compose it on top of the core app factory.

Recommended layout:

```text
examples/<app>/
  server.py
  routes.py
  summary.py
  static/
    index.html
    app.js
    vendor/
```

Separate apps are for:

- richer reference implementations
- domain-specific interfaces that would clutter the starter app
- experiments that deserve isolation from the default root app

## Route Boundaries

Starter apps and separate apps may call the generic core APIs:

- `GET /api/projects`
- `GET /api/project/{project}/state`
- `GET /api/project/{project}/graph`
- `GET /api/project/{project}/dataset/{dataset_id}`
- `GET /api/project/{project}/artifact/{dataset_id}/{logical_name}`
- `GET /api/project/{project}/artifact/{dataset_id}/{logical_name}/meta`
- `GET /api/project/{project}/artifact/{dataset_id}/{logical_name}/preview`
- `POST /api/project/{project}/analyses`
- `POST /api/project/{project}/steps`
- `POST /api/project/{project}/head`
- `POST /api/project/{project}/undo`

Common controls like head changes and undo should use these generic routes directly.

Apps may also define namespaced routes such as:

- `GET /api/project/{project}/apps/<app>/...`
- `POST /api/project/{project}/apps/<app>/actions/...`

Those routes should:

- live outside `app/`
- stay owned by the app
- translate persistent actions into generic `analysis` or `step` executions

## Large-File Behavior

If an app needs a faster read model for very large artifacts, it may implement server-side summary routes or generate derived artifacts through the generic execution harness.

Do not move that domain logic into the core unless it is broadly generic.
