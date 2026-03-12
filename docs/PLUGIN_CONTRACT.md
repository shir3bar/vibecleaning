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

Plugins should prefer the generic APIs and stay decoupled from internal storage details.

## Recommended API Usage

- `GET /api/projects`
- `GET /api/project/{project}/state`
- `GET /api/project/{project}/graph`
- `GET /api/project/{project}/dataset/{dataset_id}`
- `GET /api/project/{project}/artifact/{dataset_id}/{logical_name}`
- `GET /api/project/{project}/artifact/{dataset_id}/{logical_name}/meta`
- `GET /api/project/{project}/artifact/{dataset_id}/{logical_name}/preview`

If a plugin needs a faster derived representation for large artifacts, prefer creating an analysis output artifact rather than adding a plugin-specific backend endpoint.
