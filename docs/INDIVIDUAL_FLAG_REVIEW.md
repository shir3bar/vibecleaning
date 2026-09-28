# Review flagged fixes in the individual queue

Open **Individual queue**. The active individual's card lists its unresolved
flags by issue type, with the number of distinct fixes in each group.

- Click the issue name to show that group's fixes in red, above the other map
  layers. Other tracks and fixes turn grey. Click again to restore the usual
  colors. This does not save a decision.
- Click **Confirm** to exclude that group's fixes for this individual.
- Click **Unflag** to dismiss that group's allegations for this individual.
- If unsure, leave the group unresolved and add a note or **Needs check**.

Confirm and Unflag save immediately as a new dataset step. They use every saved
flag in the group, even if only part of the track is currently displayed.
They do not rerun the current filter or affect another individual. The usual
history and Undo controls remain available.

In the RDS app, Confirm and Unflag update the loaded track in place. Your map
view, active individual and playback position stay where they were.

If GPS flags or exclusions change the inputs to a saved stationarity run, the
affected card offers **Rerun filter**. It previews that individual's new and
obsolete candidates. Click **Update flags** to save the change or **Cancel** to
leave the saved flags as they are. Your previous confirmations and unflagging
decisions are preserved. Confirming a GPS flag already skipped by stationarity
does not trigger this notice. See [stationarity](STATIONARITY_FILTER.md) for the
gap rule and rerun details.

Groups combine the same issue type across flagging runs, counting each fix once.
The saved resolutions retain their links to each original flag. A fix can belong
to several issue types: confirming either excludes it; unflagging the other
does not cancel that exclusion.

For different decisions within a group, use the existing point or track-section
selection and Confirm/Unflag controls. Expand **Bursts** under **Flag target**
to access burst visibility and flagging controls; this section starts collapsed.

**OK / Fix & Keep / Remove**, notes and **Needs check** remain separate individual
review decisions. **Save decision** saves those decisions; it does not confirm
unresolved flags. Confirming or unflagging a group preserves an unsaved individual
decision so you can finish it afterwards.
