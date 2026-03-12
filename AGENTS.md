# Vibecleaning Agent Guide

This repository is a minimal substrate for agent-authored local data workflows. The agent is expected to create the domain logic. The framework is only responsible for lineage, execution, and a thin plugin host.

## Product Model

- A project lives under `data/<project>/`.
- Raw project inputs are top-level files under `data/<project>/`.
- Raw inputs are immutable.
- Vibecleaning state lives under `data/<project>/.vibecleaning/`.
- A dataset is a bundle of artifacts, not a single table.
- A dataset node references raw or materialized artifacts by `logical_name`.
- A step creates a new dataset node.
- An analysis does not create a new dataset node.

## Core Invariants

- Never mutate raw files in `data/<project>/`.
- Never write lineage state outside `data/<project>/.vibecleaning/`.
- Every analysis and every step must persist the user attribution string.
- Every analysis and every step must persist code, spec, and summary files.
- Every persistent change must be reproducible from:
  - the parent dataset
  - the stored step script
  - the stored step spec
  - the recorded outputs
- Reuse unchanged artifacts by reference whenever possible.
- Materialize only newly created or changed artifacts.

## Preferred Workflow

- Use an `analysis` for exploratory, read-only work.
- Use a `step` only when the user clearly wants to keep a change in the graph.
- Keep the backend generic.
- Put domain logic in agent-authored scripts or plugins, not in the core backend.
- If a plugin needs a faster read model, prefer generating a derived analysis artifact over adding a domain-specific backend endpoint.

## Project Bootstrap

On first use of a project folder:

- Treat the set of top-level, non-hidden files as the initial dataset.
- Ignore nested directories unless the user explicitly asks to model them.
- Store the initial dataset node automatically under `.vibecleaning/datasets/`.

## Plugin Philosophy

- The frontend shell should stay thin.
- Plugins are bespoke interfaces, not workflow engines.
- Plugins should consume generic APIs:
  - project state
  - graph
  - dataset manifests
  - artifact metadata
  - artifact bytes
  - preview endpoints
- Avoid adding domain-specific endpoints to the core backend just to support one plugin.

## When Modifying This Repo

- Keep the codebase small and inspectable.
- Prefer plain files and explicit JSON over abstraction-heavy infrastructure.
- Do not reintroduce chat provider integration, LAN auth flows, or domain-specific transform catalogs into core.
- If you need more behavior, write it as:
  - a custom script persisted through the execution harness, or
  - a plugin that consumes the generic APIs.
