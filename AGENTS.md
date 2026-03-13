# Vibecleaning Agent Guide

This repository is a minimal substrate for agent-authored local data workflows. The framework is responsible for lineage, execution, and a small default starter app. The agent is expected to create the domain logic.

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
- By default, extend the starter app at `server.py` and `static/`.
- Use `examples/trajectory/` as a reference when the user wants a richer example or a separate app.
- Put domain logic in agent-authored scripts or app-owned routes, not in the core backend.
- If the user needs large-file summaries or named mutation actions, add them in the starter app or in a separate app, not in `app/`.

## Project Bootstrap

On first use of a project folder:

- Treat the set of top-level, non-hidden files as the initial dataset.
- Ignore nested directories unless the user explicitly asks to model them.
- Store the initial dataset node automatically under `.vibecleaning/datasets/`.

## Starter App Philosophy

- The default UI lives in `static/`.
- The default server entrypoint is `server.py`.
- Keep the starter app small and easy to edit.
- Use the generic APIs directly for common controls like project browsing, head changes, and undo.
- Add app-specific routes only when the UI needs domain-specific summaries or named actions.
- Keep those app-specific routes outside `app/`.

## When Modifying This Repo

- Keep the codebase small and inspectable.
- Prefer plain files and explicit JSON over abstraction-heavy infrastructure.
- Do not reintroduce chat provider integration, LAN auth flows, or domain-specific transform catalogs into core.
- Do not move domain logic into `app/` just because the starter app needs it.
- If you need more behavior, write it as:
  - a custom script persisted through the execution harness, or
  - starter-app code at the repo root, or
  - a separate reference app under `examples/`.
