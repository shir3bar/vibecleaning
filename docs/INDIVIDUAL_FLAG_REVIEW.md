# Review flagged fixes in the individual queue

Open **Individual queue**. The active individual's card lists its unresolved
flags by issue type, with the number of distinct fixes in each group.

- Click the issue name to highlight that group on the map. Click again to clear
  the highlight. This does not save a decision.
- Click **Confirm** to exclude that group's fixes for this individual.
- Click **Unflag** to dismiss that group's allegations for this individual.
- If unsure, leave the group unresolved and add a note or **Needs check**.

Confirm and Unflag save immediately as a new dataset step. They use every saved
flag in the group, even if only part of the track is currently displayed.
They do not rerun the current filter or affect another individual. The usual
history and Undo controls remain available.

Groups combine the same issue type across flagging runs, counting each fix once.
The saved resolutions retain their links to each original flag. A fix can belong
to several issue types: confirming either excludes it; unflagging the other
does not cancel that exclusion.

For different decisions within a group, use the existing point or track-section
selection and Confirm/Unflag controls.

**OK / Fix & Keep / Remove**, notes and **Needs check** remain separate individual
review decisions. **Save decision** saves those decisions; it does not confirm
unresolved flags. Confirming or unflagging a group preserves an unsaved individual
decision so you can finish it afterwards.
