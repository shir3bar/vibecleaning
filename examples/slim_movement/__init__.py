"""User-facing slim movement review application."""


def is_slim_movement_artifact(artifact: dict) -> bool:
    logical_name = str(artifact.get("logical_name") or "").lower()
    return (
        logical_name.endswith(".csv")
        and not logical_name.endswith("_osm_context.csv")
        and not logical_name.endswith("_reviewed.csv")
    )
