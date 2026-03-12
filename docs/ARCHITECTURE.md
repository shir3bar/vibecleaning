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
- no built-in domain action catalog
- no SQL engine assumptions

The current implementation supports Python scripts as the minimal execution substrate.

## 3. Generic HTTP App

The core HTTP layer exposes reusable generic routes for:

- listing projects
- reading project state
- reading graph structure
- reading dataset manifests
- reading artifact metadata
- reading artifact bytes
- reading lightweight previews
- creating analyses
- creating steps
- changing the current head
- undoing to the parent dataset

The core app is created through an app factory so overlays can compose with it without the core importing those overlays.

## 4. Application Overlays

An application overlay is a domain-specific package that imports the core app and adds:

- overlay-owned backend routes
- overlay-owned plugins
- overlay-owned action handlers
- overlay-owned summary or preview logic for large artifacts

An overlay may expose named UI actions like `Delete checked`, but those actions should compile down to generic `analysis` or `step` executions.

Dependency direction is one-way:

1. core knows nothing about overlays
2. overlays import the core
3. plugins talk to either generic core routes or their overlay's routes

## 5. Agent Contracts

The markdown files in `docs/` and `AGENTS.md` are part of the product. They define the invariants that an AI agent should preserve when extending the repo.

The intended system behavior is:

1. the user interacts with an AI agent
2. the agent reads the local contracts
3. the agent generates bespoke scripts, plugins, or overlay code
4. the backend persists lineage and execution records
