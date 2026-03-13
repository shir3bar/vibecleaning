# Safety

The framework is intentionally strict about a small number of invariants.

## Never Break These Rules

- Do not mutate raw inputs under `data/<project>/`.
- Do not write lineage state outside `data/<project>/.vibecleaning/`.
- Do not create a persistent change without a recorded user attribution string.
- Do not create a persistent change without persisting script, spec, and summary files.
- Do not rewrite unchanged artifacts unnecessarily.
- Do not add domain-specific transformation logic to the core backend when the same behavior can live in an agent-authored script, the root starter app, or a separate reference app.

## Determinism

Persistent steps should be deterministic with respect to:

- the parent dataset
- the stored step script
- the stored step spec
- the local execution environment required by the script

If a transform depends on undeclared network state, hidden notebook state, or a manual external step, it should not be committed as a step without making those dependencies explicit.
