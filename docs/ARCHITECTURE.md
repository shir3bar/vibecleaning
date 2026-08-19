# Architecture

Three layers matter:

1. `app/`
   Generic lineage, execution, preview, and HTTP internals.
2. `server.py` + `static/`
   The default starter app. This is the normal place to add frontend code and app-specific routes.
3. `examples/`
   Reference apps that show richer patterns.

Dependency rule:

- `app/` should not import the starter app or examples.
- The starter app and examples may call or compose the core.

The shared authentication and review-coordination architecture used by the
movement applications is specified in
[`multi-user-movement-review-design.md`](multi-user-movement-review-design.md).

Generic routes:

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

How to wire frontend to backend:

- Put default UI code in `static/app.js`.
- Call the generic routes directly for project browsing, dataset selection, artifact reads, head changes, and undo.
- If the UI needs a domain-specific summary or action, add a route in `server.py` or an example app.
- App-owned routes should stay outside `app/` and compile persistent work down to `POST /analyses` or `POST /steps`.
