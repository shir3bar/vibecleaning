# Plugin Contract

Plugins are browser modules discovered through `plugins/manifest.json`.

Each manifest entry must contain:

```json
{
  "id": "example-plugin",
  "name": "Example Plugin",
  "module": "/plugins/examples/example/plugin.js",
  "description": "Optional description"
}
```

Each plugin module must export:

```js
export async function createPlugin(context) { ... }
```

The host passes:

- `mountEl`
- `plugin`
- `storageKey`
- `fetchJSON(url)`
- `requestJSON(url, options)`
- `fetchText(url)`

Plugins should stay decoupled from internal storage details.

There are two valid plugin styles in this repository:

- core-only plugins that use only the generic APIs
- overlay plugins that use the generic APIs plus routes owned by their application overlay

The generic host does not know which overlay routes exist. Those routes are defined by the overlay, not the core.

## Recommended API Usage

- `GET /api/projects`
- `GET /api/project/{project}/state`
- `GET /api/project/{project}/graph`
- `GET /api/project/{project}/dataset/{dataset_id}`
- `GET /api/project/{project}/artifact/{dataset_id}/{logical_name}`
- `GET /api/project/{project}/artifact/{dataset_id}/{logical_name}/meta`
- `GET /api/project/{project}/artifact/{dataset_id}/{logical_name}/preview`
- `POST /api/project/{project}/analyses`
- `POST /api/project/{project}/steps`

## Overlay Routes

If a plugin is part of an overlay, it may also call overlay-owned routes such as:

- `GET /api/project/{project}/apps/<overlay>/...`
- `POST /api/project/{project}/apps/<overlay>/actions/...`

Those routes should:

- live outside the core backend
- stay namespaced under the overlay
- compile domain actions down to generic `analysis` or `step` executions when they persist changes

For large artifacts, overlays may implement server-side summary routes instead of pushing the work into the browser.
