from pathlib import Path

from app.state import load_json, project_paths, save_json
from examples.movement.migrate_legacy_review_step import (
    apply_migration,
    build_migration,
)


def test_migrate_legacy_review_step_reuses_raw_csv_and_writes_sidecar(tmp_path):
    project_dir = tmp_path / "study"
    project_dir.mkdir()
    raw_path = project_dir / "movement.csv"
    raw_path.write_text(
        "eventid,individual,timestamp,longitude,latitude\n"
        "fix_1,alpha,2024-01-01T00:00:00Z,-70,40\n",
        encoding="utf-8",
    )
    paths = project_paths(project_dir)
    for name in ("datasets", "steps", "outputs"):
        paths[name].mkdir(parents=True, exist_ok=True)

    parent_id = "dataset_parent"
    dataset_id = "dataset_legacy"
    step_id = "step_legacy"
    parent_artifact = {
        "logical_name": "movement.csv",
        "path": "movement.csv",
        "storage_type": "raw",
        "size": raw_path.stat().st_size,
        "content_type": "text/csv",
        "metadata": {},
    }
    save_json(
        paths["datasets"] / f"{parent_id}.json",
        {
            "dataset_id": parent_id,
            "parent_dataset_id": None,
            "artifacts": [parent_artifact],
        },
    )
    copied_path = paths["outputs"] / dataset_id / "movement.csv"
    copied_path.parent.mkdir(parents=True)
    copied_path.write_text(raw_path.read_text(encoding="utf-8"), encoding="utf-8")
    save_json(
        paths["datasets"] / f"{dataset_id}.json",
        {
            "dataset_id": dataset_id,
            "parent_dataset_id": parent_id,
            "artifacts": [
                {
                    **parent_artifact,
                    "path": str(copied_path.relative_to(project_dir)),
                    "storage_type": "output",
                }
            ],
        },
    )
    step_dir = paths["steps"] / step_id
    step_dir.mkdir()
    step = {
        "step_id": step_id,
        "output_dataset_id": dataset_id,
        "parent_dataset_id": parent_id,
        "title": "Mark one fix as suspected",
        "user": "reviewer",
        "created_at": "2026-01-02T00:00:00+00:00",
        "parameters": {
            "app": "movement",
            "action": "annotate_fixes",
            "target_artifact": "movement.csv",
            "fix_keys": ["id:fix_1#row:1"],
            "status": "suspected",
            "issue_type": "GPS noise",
            "issue_note": "Check this fix",
            "user": "reviewer",
        },
        "summary": {
            "matched_fix_keys": ["id:fix_1#row:1"],
            "reviewed_at": "2026-01-02T00:00:00+00:00",
        },
    }
    save_json(step_dir / "step.json", step)
    save_json(step_dir / "spec.json", {})
    save_json(step_dir / "summary.json", step["summary"])
    (step_dir / "transform.py").write_text("# legacy\n", encoding="utf-8")

    migration = build_migration(project_dir, dataset_id)

    assert copied_path.is_file()
    assert migration["reclaimed_bytes"] == copied_path.stat().st_size
    assert [item["logical_name"] for item in migration["dataset"]["artifacts"]] == [
        "movement.csv",
        "movement_review_annotations.json",
    ]
    assert migration["dataset"]["artifacts"][0] == parent_artifact

    apply_migration(migration)

    assert not copied_path.exists()
    migrated_dataset = load_json(paths["datasets"] / f"{dataset_id}.json")
    assert migrated_dataset["artifacts"][0] == parent_artifact
    assert migrated_dataset["artifacts"][1]["storage_type"] == "output"
    sidecar = load_json(paths["outputs"] / dataset_id / "movement_review_annotations.json")
    annotation = sidecar["annotations"][0]
    assert annotation["step_id"] == step_id
    assert annotation["status"] == "suspected"
    assert annotation["scope"]["row_ranges"] == [[1, 1]]
    migrated_step = load_json(step_dir / "step.json")
    assert migrated_step["output_artifacts"] == ["movement_review_annotations.json"]
    assert migrated_step["summary"]["materialized_csv"] is False
    assert "_VIBECLEANING_BUNDLED_SOURCES" in (
        step_dir / "transform.py"
    ).read_text(encoding="utf-8")
