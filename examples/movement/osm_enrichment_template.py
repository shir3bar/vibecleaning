import json
import os
from pathlib import Path


OUTPUT_ARTIFACT_NAME = "movement_osm_context.csv"


def _declared_artifact(spec: dict, artifact_list: str, logical_name: str) -> dict | None:
    for artifact in spec.get(artifact_list, []):
        if artifact.get("logical_name") == logical_name:
            return artifact
    return None


def _write_summary(summary_path: Path, summary: dict):
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main():
    spec_path = Path(os.environ["VIBECLEANING_SPEC_PATH"])
    summary_path = Path(os.environ["VIBECLEANING_SUMMARY_PATH"])
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    params = dict(spec["step"].get("parameters") or {})
    from examples.movement.osm_enrichment import (
        OSMEnrichmentError,
        enrich_movement_csv_with_osm_context,
    )

    target_artifact = str(params.get("target_artifact") or "").strip()
    data_root = Path(str(params.get("data_root") or "")).resolve()
    if not str(params.get("data_root") or "").strip():
        raise SystemExit("Shared data root was not provided for local OSM context enrichment")
    source = _declared_artifact(spec, "input_artifacts", target_artifact)
    output = _declared_artifact(spec, "output_artifacts", OUTPUT_ARTIFACT_NAME)
    if source is None:
        raise SystemExit("Target movement artifact was not provided as an input")
    if output is None:
        raise SystemExit("OSM context output artifact was not declared")

    try:
        summary = enrich_movement_csv_with_osm_context(
            input_csv=Path(source["path"]),
            output_csv=Path(output["path"]),
            search_radius_m=params.get("search_radius_m"),
            data_root=data_root,
            input_artifact_name=target_artifact,
            output_artifact_name=OUTPUT_ARTIFACT_NAME,
            confirmed_large_download=bool(params.get("confirmed_large_download", False)),
            catalog_url=os.environ.get("VIBECLEANING_GEOFABRIK_INDEX_URL"),
        )
    except OSMEnrichmentError as exc:
        if exc.summary is not None:
            _write_summary(summary_path, exc.summary)
        raise RuntimeError(str(exc)) from exc
    _write_summary(summary_path, summary)


if __name__ == "__main__":
    main()
