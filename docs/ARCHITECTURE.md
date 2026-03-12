# Architecture

Vibecleaning is organized as five layers.

## 1. Lineage Core

The lineage core stores project state on disk under `data/<project>/.vibecleaning/`.

It owns:

- project bootstrap
- dataset nodes
- step edges
- analysis records
- the active project head
- artifact lookup by `logical_name`

It does not own domain logic.

## 2. Execution Harness

The execution harness accepts an analysis or step request, writes the stored script/spec/summary files, runs the script, and records the result.

The harness is intentionally generic:

- no built-in CSV transforms
- no built-in column operations
- no domain-specific step catalog
- no SQL engine assumptions

The current implementation supports Python scripts as the minimal execution substrate.

## 3. Generic Read APIs

The backend exposes generic APIs for:

- listing projects
- reading project state
- reading graph structure
- reading dataset manifests
- reading artifact metadata
- reading artifact bytes
- reading lightweight previews

These APIs exist so plugins can stay lazy. A plugin should not need to load every artifact eagerly.

## 4. Plugin Host

The frontend shell is a plugin loader. It is not a workflow engine.

Plugins are discovered through `plugins/manifest.json` and mounted into the shell. Each plugin receives:

- a mount element
- JSON request helpers
- text request helpers
- a stable storage key

## 5. Agent Contracts

The markdown files in `docs/` and `AGENTS.md` are part of the product. They define the invariants that an AI agent should preserve when extending the repo.

The intended system behavior is:

1. the user interacts with an AI agent
2. the agent reads the local contracts
3. the agent generates bespoke scripts or plugins
4. the backend persists lineage and execution records
