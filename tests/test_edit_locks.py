from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path

import pytest

from app.auth import Actor
from app.edit_locks import (
    EditConflictError,
    EditLockedError,
    build_edit_lock_profile,
    create_guarded_step,
    restore_forward_head_guarded,
    resume_from_dataset,
    undo_guarded,
)
from app.execution import create_analysis, create_step
from app.reviews import (
    active_review,
    assign_review,
    load_review_state,
    reconcile_review_state_after_resume,
    review_profile,
)
from app.state import (
    ensure_project_state,
    list_datasets,
    list_history,
    load_project_state,
    project_paths,
)


STEP_SCRIPT = """
import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
output = Path(spec["output_artifacts"][0]["path"])
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(str(spec["step"]["parameters"]["value"]))
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text(
    json.dumps({"value": spec["step"]["parameters"]["value"]})
)
"""


ANALYSIS_SCRIPT = """
import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
output = Path(spec["output_artifacts"][0]["path"])
output.parent.mkdir(parents=True, exist_ok=True)
output.write_bytes(b"x" * 4096)
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text(
    json.dumps({"created": output.name})
)
"""


def _project(tmp_path: Path) -> tuple[Path, str]:
    project_dir = tmp_path / "study"
    project_dir.mkdir()
    (project_dir / "movement.csv").write_text("eventid,value\nfix-1,1\n")
    state = ensure_project_state(project_dir)
    return project_dir, state["current_dataset_id"]


def _step(project_dir: Path, parent_dataset_id: str, value: str) -> dict:
    return create_step(
        project_dir,
        {
            "user": "reviewer",
            "title": f"step {value}",
            "kind": "python",
            "script": STEP_SCRIPT,
            "parameters": {"value": value},
            "parent_dataset_id": parent_dataset_id,
            "input_artifacts": ["movement.csv"],
            "output_artifacts": [f"marker_{value}.txt"],
            "set_as_head": True,
        },
    )


def test_edit_profile_locks_historical_but_not_rewound_current_versions(tmp_path):
    project_dir, root_id = _project(tmp_path)
    first_id = _step(project_dir, root_id, "one")["dataset"]["dataset_id"]
    second_id = _step(project_dir, first_id, "two")["dataset"]["dataset_id"]

    current = build_edit_lock_profile(project_dir, second_id)
    historical = build_edit_lock_profile(project_dir, first_id)

    assert current["editable"] is True
    assert historical["editable"] is False
    assert [item["code"] for item in historical["blockers"]] == ["historical_version"]
    assert historical["resume"]["discard_dataset_count"] == 1

    undone = undo_guarded(
        project_dir,
        expected_current_dataset_id=second_id,
    )
    assert undone["dataset"]["dataset_id"] == first_id
    rewound = build_edit_lock_profile(project_dir, first_id)
    assert rewound["editable"] is True
    assert rewound["blockers"] == []
    assert rewound["resume"]["allowed"] is False

    undone_again = undo_guarded(
        project_dir,
        expected_current_dataset_id=first_id,
    )
    assert undone_again["dataset"]["dataset_id"] == root_id
    root_profile = build_edit_lock_profile(project_dir, root_id)
    assert root_profile["editable"] is True
    assert root_profile["resume"]["allowed"] is False
    assert {item["dataset_id"] for item in list_datasets(project_dir)} == {
        root_id,
        first_id,
        second_id,
    }
    assert not project_paths(project_dir)["archives"].exists()


def test_undo_keeps_the_undone_step_as_an_unarchived_branch(tmp_path):
    project_dir, root_id = _project(tmp_path)
    first_id = _step(project_dir, root_id, "one")["dataset"]["dataset_id"]
    second_id = _step(project_dir, first_id, "two")["dataset"]["dataset_id"]
    history_before = list_history(project_dir)

    undo_guarded(project_dir, expected_current_dataset_id=second_id)
    replacement = create_guarded_step(
        project_dir,
        {
            "user": "reviewer",
            "title": "replacement branch",
            "kind": "python",
            "script": STEP_SCRIPT,
            "parameters": {"value": "replacement"},
            "input_artifacts": ["movement.csv"],
            "output_artifacts": ["replacement.txt"],
            "set_as_head": True,
        },
        selected_dataset_id=first_id,
        expected_current_dataset_id=first_id,
    )

    dataset_ids = {item["dataset_id"] for item in list_datasets(project_dir)}
    step_ids = {item["step_id"] for item in list_history(project_dir)["steps"]}
    assert second_id in dataset_ids
    assert replacement["dataset"]["dataset_id"] in dataset_ids
    assert {item["step_id"] for item in history_before["steps"]} <= step_ids
    assert not project_paths(project_dir)["archives"].exists()


def test_restore_forward_head_moves_rewound_pointer_without_discarding_history(tmp_path):
    project_dir, root_id = _project(tmp_path)
    first_id = _step(project_dir, root_id, "one")["dataset"]["dataset_id"]
    second_id = _step(project_dir, first_id, "two")["dataset"]["dataset_id"]
    history_before = list_history(project_dir)

    undo_guarded(project_dir, expected_current_dataset_id=second_id)
    restored = restore_forward_head_guarded(
        project_dir,
        selected_dataset_id=second_id,
        expected_current_dataset_id=first_id,
    )

    assert restored["dataset"]["dataset_id"] == second_id
    assert load_project_state(project_dir)["current_dataset_id"] == second_id
    assert restored["history"] == history_before
    assert build_edit_lock_profile(project_dir, second_id)["editable"] is True


def test_guarded_step_rejects_locked_or_stale_parent_without_orphans(tmp_path):
    project_dir, root_id = _project(tmp_path)
    first_id = _step(project_dir, root_id, "one")["dataset"]["dataset_id"]
    before_datasets = [item["dataset_id"] for item in list_datasets(project_dir)]
    before_steps = [item["step_id"] for item in list_history(project_dir)["steps"]]

    payload = {
        "user": "reviewer",
        "title": "blocked",
        "kind": "python",
        "script": STEP_SCRIPT,
        "parameters": {"value": "blocked"},
        "input_artifacts": ["movement.csv"],
        "output_artifacts": ["blocked.txt"],
        "set_as_head": True,
    }
    with pytest.raises(EditLockedError):
        create_guarded_step(
            project_dir,
            payload,
            selected_dataset_id=root_id,
            expected_current_dataset_id=first_id,
        )
    with pytest.raises(EditConflictError):
        create_guarded_step(
            project_dir,
            payload,
            selected_dataset_id=first_id,
            expected_current_dataset_id=root_id,
        )

    assert [item["dataset_id"] for item in list_datasets(project_dir)] == before_datasets
    assert [item["step_id"] for item in list_history(project_dir)["steps"]] == before_steps
    assert not (project_paths(project_dir)["outputs"] / "blocked.txt").exists()


def test_simultaneous_guarded_steps_create_one_child_without_orphans(tmp_path):
    project_dir, root_id = _project(tmp_path)

    def create_child(value: str):
        return create_guarded_step(
            project_dir,
            {
                "user": "reviewer",
                "title": f"concurrent {value}",
                "kind": "python",
                "script": STEP_SCRIPT,
                "parameters": {"value": value},
                "input_artifacts": ["movement.csv"],
                "output_artifacts": [f"concurrent_{value}.txt"],
                "set_as_head": True,
            },
            selected_dataset_id=root_id,
            expected_current_dataset_id=root_id,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(create_child, value) for value in ("one", "two")]

    successes = []
    conflicts = []
    for future in futures:
        try:
            successes.append(future.result())
        except EditConflictError as exc:
            conflicts.append(exc)

    assert len(successes) == 1
    assert len(conflicts) == 1
    assert len(list_datasets(project_dir)) == 2
    assert len(list_history(project_dir)["steps"]) == 1
    assert load_project_state(project_dir)["current_dataset_id"] == (
        successes[0]["dataset"]["dataset_id"]
    )


def test_resume_archives_non_target_history_and_deletes_heavy_outputs(tmp_path):
    project_dir, root_id = _project(tmp_path)
    first = _step(project_dir, root_id, "one")
    first_id = first["dataset"]["dataset_id"]
    second = _step(project_dir, first_id, "two")
    second_id = second["dataset"]["dataset_id"]
    analysis = create_analysis(
        project_dir,
        {
            "user": "reviewer",
            "title": "discarded analysis",
            "kind": "python",
            "script": ANALYSIS_SCRIPT,
            "dataset_id": second_id,
            "input_artifacts": ["movement.csv"],
            "output_artifacts": ["heavy.bin"],
        },
    )
    analysis_id = analysis["analysis"]["analysis_id"]
    profile = build_edit_lock_profile(project_dir, first_id)

    with pytest.raises(EditConflictError):
        resume_from_dataset(
            project_dir,
            selected_dataset_id=first_id,
            expected_current_dataset_id=second_id,
            resume_token="stale",
            user="reviewer",
        )
    assert load_project_state(project_dir)["current_dataset_id"] == second_id

    result = resume_from_dataset(
        project_dir,
        selected_dataset_id=first_id,
        expected_current_dataset_id=second_id,
        resume_token=profile["resume"]["token"],
        user="reviewer",
    )

    assert load_project_state(project_dir)["current_dataset_id"] == first_id
    assert {
        item["dataset_id"] for item in list_datasets(project_dir)
    } == {root_id, first_id}
    assert [item["step_id"] for item in list_history(project_dir)["steps"]] == [
        first["step"]["step_id"]
    ]
    assert list_history(project_dir)["analyses"] == []
    assert not (project_paths(project_dir)["outputs"] / second_id).exists()
    assert not (project_paths(project_dir)["analyses"] / analysis_id).exists()
    assert result["profile"]["editable"] is True

    archive_dir = project_paths(project_dir)["archives"] / result["archive"]["archive_id"]
    manifest = json.loads((archive_dir / "manifest.json").read_text())
    assert manifest["target_dataset_id"] == first_id
    assert manifest["discarded_dataset_ids"] == [second_id]
    assert manifest["discarded_analysis_ids"] == [analysis_id]
    assert (archive_dir / "datasets" / f"{second_id}.json").is_file()
    assert (archive_dir / "steps" / second["step"]["step_id"] / "transform.py").is_file()
    assert (archive_dir / "analyses" / analysis_id / "analysis.json").is_file()
    assert not (archive_dir / "analyses" / analysis_id / "outputs").exists()


def test_resume_before_active_review_baseline_cancels_assignment_and_restores_profile(tmp_path):
    project_dir, root_id = _project(tmp_path)
    first_id = _step(project_dir, root_id, "one")["dataset"]["dataset_id"]
    editor = Actor("user_editor", "editor", "Eli Editor", "editor")
    reviewer = Actor("user_reviewer", "reviewer", "Rae Reviewer", "reviewer")
    assigned = assign_review(
        project_dir,
        editor=editor,
        reviewer=reviewer,
        expected_current_dataset_id=first_id,
        expected_review_revision=0,
        individuals=["alpha"],
    )
    second_id = _step(project_dir, first_id, "two")["dataset"]["dataset_id"]
    undo_guarded(project_dir, expected_current_dataset_id=second_id)
    undo_guarded(project_dir, expected_current_dataset_id=first_id)
    rewound_profile = build_edit_lock_profile(project_dir, root_id)
    assert rewound_profile["editable"] is True
    assert rewound_profile["resume"]["allowed"] is False

    restore_forward_head_guarded(
        project_dir,
        selected_dataset_id=second_id,
        expected_current_dataset_id=root_id,
    )
    profile = build_edit_lock_profile(project_dir, root_id)
    assert profile["editable"] is False
    assert profile["resume"]["allowed"] is True

    result = resume_from_dataset(
        project_dir,
        selected_dataset_id=root_id,
        expected_current_dataset_id=second_id,
        resume_token=profile["resume"]["token"],
        user=editor.display_name,
        post_resume=lambda context: reconcile_review_state_after_resume(
            project_dir,
            actor=editor,
            target_dataset_id=context["target_dataset_id"],
            kept_dataset_ids=context["kept_dataset_ids"],
            archive_id=context["archive_id"],
        ),
    )

    state = load_review_state(project_dir)
    review = state["reviews"][0]
    assert active_review(state) is None
    assert review["review_id"] == assigned["review"]["review_id"]
    assert review["status"] == "cancelled"
    assert review["final_dataset_id"] == root_id
    assert review["history_resume_archive_id"] == result["archive"]["archive_id"]
    assert state["events"][-1]["type"] == "review_cancelled_by_history_resume"
    assert result["related_state"]["cancelled_review_ids"] == [review["review_id"]]
    restored_profile = review_profile(project_dir, editor, [])
    assert restored_profile["review"] is None
    assert restored_profile["capabilities"]["can_read"] is True
