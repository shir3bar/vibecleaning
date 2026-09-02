# Movement review workflow image prompt

Generated with the built-in image generation tool using this prompt:

```text
Use case: infographic-diagram
Asset type: authoritative workflow diagram for the Movement Review app documentation
Primary request: Create a polished, accurate landscape infographic titled exactly "Movement Review Workflow" showing the current multi-user reviewer/editor workflow.
Scene/backdrop: clean warm-white background, no scenery.
Style/medium: crisp vector-like product workflow diagram, restrained modern web-app aesthetic, high information clarity, readable sans-serif typography, subtle rounded cards and precise arrows.
Composition/framing: 16:9 landscape. Two horizontal swimlanes labeled exactly "REVIEWER" and "EDITOR". A slim shared-state spine between them represents the study lineage. Strong left-to-right flow with a compact loop back for continued review. Use generous spacing and avoid dense paragraphs.

Required workflow and exact visible labels:
Top/shared start:
"Sign in"
"Editor assigns study"
"Baseline = current dataset"

Reviewer lane main flow:
"Open assigned study"
"Review queue"
"Inspect individual & fixes"
A three-way decision card titled "Decision" with exactly these choices:
"Reviewed—OK"
"Issues found"
"Second opinion"
Then:
"Annotation step"
"New immutable dataset head"
"Coverage updated"
Diamond:
"Every individual reviewed?"
No arrow loops back to "Review queue"
Yes arrow goes to "Complete review"
Then:
"Study may be reassigned"

Add a small side card connected to "Inspect individual & fixes":
"Analyses"
with subtext:
"Queries • rankings • reports • exports"
and a small note:
"No dataset node"

Add a small side card connected to "Issues found":
"Flag fixes or apply full-dataset filter"
then arrow to "Annotation step"

Editor lane intervention flow:
From "Active review", arrow to:
"Start editor control"
with small note:
"Reason required"
Then:
"Reviewer becomes read-only"
Then:
"Editor edits or updates dataset"
Then:
"Coverage impact"
with three short bullets:
"Added → queued"
"Changed → reopened"
"Removed → no longer required"
Then:
"Release control"
Then arrow upward to reviewer lane:
"Live refresh"
with small note:
"Editing re-enabled"

Add a compact safety footer with exactly:
"Every write checks current dataset ID + review revision"
"Undo/Resume stays within protected lineage boundaries"
"All actions are attributed to the signed-in user"

Visual semantics:
- Reviewer lane uses deep teal and sea-green.
- Editor lane uses warm amber and muted coral.
- Immutable dataset nodes are small dark navy hexagons connected as a lineage.
- Analyses use outlined cards, visually distinct from persistent steps.
- Second opinion uses violet and remains visibly queued while still counting as reviewed.
- Editor control is shown as a temporary gate across the reviewer lane, not a timeout.
- Use arrows that clearly show control release returns the reviewer to the latest dataset head.
- Keep all text horizontal, large, and legible.
Constraints: render the quoted text verbatim; no extra roles, no approval stage, no database, no heartbeat, no polling, no simultaneous branching, no logos, no watermark, no decorative characters, no tiny illegible footnotes.
```
