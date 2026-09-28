"""Upgrade real persisted histories, including interruption and script replay."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from app import metadata, state
from app.auth import AuthManager, build_user_record, users_path, write_users_file
from app.execution import create_analysis, create_step, run_python_script
from app.runtime import validate_data_root
from app.web import create_app
from examples.movement.catalog import list_studies


LEGACY_STEP = '''import json, os
from pathlib import Path
spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text(encoding="utf-8"))
Path(spec["output_artifacts"][0]["path"]).write_text("confirmed,René\\n", encoding="utf-8")
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text('{"saved": true}', encoding="utf-8")
'''
LEGACY_REPORT = '''import json, os
from pathlib import Path
spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text(encoding="utf-8"))
decision = Path(spec["input_artifacts"][0]["path"]).read_text(encoding="utf-8")
Path(spec["output_artifacts"][0]["path"]).write_text("Report: " + decision, encoding="utf-8")
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text('{"report": true}', encoding="utf-8")
'''


def checksums(directory):
    return {path.relative_to(directory).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in directory.rglob("*") if path.is_file()}


@pytest.fixture
def legacy_study(tmp_path, monkeypatch):
    study = tmp_path / "René study"
    study.mkdir()
    (study / "raw.csv").write_text("id,value\n1,raw\n", encoding="utf-8")
    # Build a history using the old location, through the real execution API.
    with monkeypatch.context() as old:
        old.setattr(state, "metadata_dir", lambda directory: directory / ".vibecleaning")
        created = create_step(study, {
            "user": "René", "title": "Review", "kind": "python", "script": LEGACY_STEP,
            "output_artifacts": ["decisions.csv"],
            "parameters": {"note": "Keep the literal .vibecleaning/old note", "path": ".vibecleaning/user-text"},
        })
        analysis = create_analysis(study, {
            "user": "René", "title": "Report", "kind": "python", "script": LEGACY_REPORT,
            "input_artifacts": ["decisions.csv"], "output_artifacts": ["report.txt"],
        })
    return study, created, analysis


def test_migrate_preserves_history_outputs_notes_scripts_and_backup(legacy_study):
    study, created, analysis = legacy_study
    before = checksums(study / ".vibecleaning")
    head = created["dataset"]["dataset_id"]
    assert state.load_project_state(study)["current_dataset_id"] == head
    assert not (study / ".vibecleaning").exists()
    assert checksums(study / ".vibecleaning-backup") == before
    dataset = state.load_dataset(study, head)
    for artifact in dataset["artifacts"]:
        assert state.resolve_artifact_path(study, artifact).is_file()
    history = state.list_history(study)
    assert history["steps"][0]["parameters"] == created["step"]["parameters"]
    saved_report = history["analyses"][0]
    script = study / saved_report["script_path"]
    assert script.read_text(encoding="utf-8") == LEGACY_REPORT
    assert (study / history["steps"][0]["script_path"]).read_text(encoding="utf-8") == LEGACY_STEP
    output = study / saved_report["realized_output_artifacts"][0]["path"]
    assert output.read_text(encoding="utf-8") == "Report: confirmed,René\n"
    output.unlink()
    run_python_script(script, study / saved_report["spec_path"], study / saved_report["summary_path"])
    assert output.read_text(encoding="utf-8") == "Report: confirmed,René\n"
    after = checksums(study / "scrubdata")
    assert state.load_project_state(study)["current_dataset_id"] == head
    assert checksums(study / "scrubdata") == after
    assert (study / "raw.csv").read_text() == "id,value\n1,raw\n"


@pytest.mark.parametrize("phase", ["prepare", "publish"])
def test_interrupted_conversion_recovers_without_losing_original(legacy_study, monkeypatch, phase):
    study, created, _ = legacy_study
    before = checksums(study / ".vibecleaning")
    with monkeypatch.context() as failure:
        if phase == "prepare":
            original = metadata._prepare_copy
            def interrupted(*args):
                original(*args)
                raise OSError("simulated interruption before archive")
            failure.setattr(metadata, "_prepare_copy", interrupted)
        else:
            original = Path.rename
            def interrupted(path, target):
                if path.name == metadata.STAGING_DIR_NAME:
                    raise OSError("simulated interruption before publish")
                return original(path, target)
            failure.setattr(Path, "rename", interrupted)
        with pytest.raises(metadata.MetadataMigrationError, match="simulated interruption"):
            state.load_project_state(study)
    remaining = study / (".vibecleaning" if phase == "prepare" else ".vibecleaning-backup")
    assert checksums(remaining) == before
    assert not (study / "scrubdata").exists()
    assert state.load_project_state(study)["current_dataset_id"] == created["dataset"]["dataset_id"]
    assert checksums(study / ".vibecleaning-backup") == before


def test_both_histories_are_rejected_without_changing_either(legacy_study):
    study, *_ = legacy_study
    (study / "scrubdata").mkdir()
    (study / "scrubdata/project.json").write_text('{"current_dataset_id": "other"}')
    old, new = checksums(study / ".vibecleaning"), checksums(study / "scrubdata")
    with pytest.raises(metadata.MetadataMigrationError, match="Both"):
        state.load_project_state(study)
    assert checksums(study / ".vibecleaning") == old
    assert checksums(study / "scrubdata") == new


def test_parallel_openers_share_one_migrated_head(legacy_study):
    study, created, _ = legacy_study
    with ThreadPoolExecutor(max_workers=4) as pool:
        heads = list(pool.map(lambda _: state.load_project_state(study)["current_dataset_id"], range(8)))
    assert heads == [created["dataset"]["dataset_id"]] * 8


def test_auth_registry_and_query_library_move_together(tmp_path):
    root = tmp_path / "data"
    legacy = root / ".vibecleaning"
    legacy.mkdir(parents=True)
    write_users_file(legacy / "users.json", [build_user_record(
        username="editor", display_name="René", role="editor", password="test-password",
    )])
    (legacy / "query_library.json").write_text('{"note": ".vibecleaning/keep-this-text"}')
    before = checksums(legacy)
    assert validate_data_root(root) == root
    assert users_path(root) == root / "scrubdata/users.json"
    assert AuthManager.from_data_root(root).authenticate("editor", "test-password").display_name == "René"
    for name, digest in before.items():
        assert hashlib.sha256((root / "scrubdata" / name).read_bytes()).hexdigest() == digest
    assert state.list_projects(root) == []
    assert not (root / "scrubdata/scrubdata").exists()


def test_specs_saved_on_windows_rebase_without_rewriting_user_parameters(legacy_study):
    study, created, analysis = legacy_study
    spec_path = study / analysis["analysis"]["spec_path"]
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    former = "C:\\Users\\René\\data\\study"
    for artifact in spec["input_artifacts"] + spec["output_artifacts"]:
        artifact["path"] = former + "\\" + Path(artifact["path"]).relative_to(study).as_posix().replace("/", "\\")
    spec["project_dir"] = former
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    state.load_project_state(study)
    record = state.list_history(study)["analyses"][0]
    spec = json.loads((study / record["spec_path"]).read_text(encoding="utf-8"))
    assert spec["project_dir"] == str(study)
    assert all(Path(item["path"]).is_file() for item in spec["input_artifacts"] + spec["output_artifacts"])
    assert state.list_history(study)["steps"][0]["parameters"] == created["step"]["parameters"]


def test_missing_or_corrupt_history_does_not_become_a_new_project(legacy_study):
    study, *_ = legacy_study
    dataset = next((study / ".vibecleaning/datasets").glob("*.json"))
    dataset.write_text("interrupted JSON", encoding="utf-8")
    with pytest.raises(metadata.MetadataMigrationError):
        state.load_project_state(study)
    assert not (study / "scrubdata").exists()
    assert dataset.read_text() == "interrupted JSON"


def test_incomplete_backup_is_not_silently_replaced_by_empty_state(tmp_path):
    (tmp_path / ".vibecleaning-backup").mkdir()
    with pytest.raises(metadata.MetadataMigrationError, match="restore"):
        state.ensure_project_state(tmp_path)
    assert not (tmp_path / "scrubdata").exists()


def test_metadata_is_not_a_study_and_conflicts_are_visible(tmp_path):
    family = tmp_path / "movement_raw"
    metadata_root = family / "scrubdata"
    metadata_root.mkdir(parents=True)
    (metadata_root / "users.json").write_text('{}')
    assert list_studies(tmp_path, "movement_raw") == []
    study = family / "example"
    study.mkdir()
    (study / "data.csv").write_text("id,value\n1,raw\n")
    (study / ".vibecleaning").mkdir()
    (study / "scrubdata").mkdir()
    with pytest.raises(metadata.MetadataMigrationError, match="Both"):
        list_studies(tmp_path, "movement_raw")
    app = create_app(data_root=family, static_root=Path(__file__).resolve().parents[1] / "static")
    response = TestClient(app).get("/api/projects")
    assert response.status_code == 409
    assert "Both" in response.json()["error"]


@pytest.mark.parametrize("source_format", ["csv", "rds"])
def test_review_reports_exports_and_further_edits_survive_conversion(tmp_path, monkeypatch, source_format):
    from test_movement_fixes import create_movement_test_client
    from test_movement_issue_groups import CSV, post_review
    from test_rds_movement import _client as rds_client

    with monkeypatch.context() as old:
        old.setattr(state, "metadata_dir", lambda directory: directory / ".vibecleaning")
        if source_format == "csv":
            client, _ = create_movement_test_client(tmp_path, csv_content=CSV)
            study = tmp_path / "data/movement_clean/test_study"
            base = "/api/apps/movement/family/movement_clean/study/test_study"
        else:
            client, study = rds_client(tmp_path)
            base = "/api/apps/movement/family/movement_rds/study/268904527"
        raw_hashes = {path.name: path.read_bytes() for path in state.iter_source_files(study)}
        loaded = client.get(base + "/load").json()
        head, logical = loaded["dataset_id"], loaded["logical_name"]
        fix = client.get(f"{base}/dataset/{head}/fixes", params={"logical_name": logical}).json()["fixes"][0]
        def body():
            return {"dataset_id": head, "expected_current_dataset_id": head, "logical_name": logical, "user": "reviewer"}
        flagged = post_review(client, base, "annotate-scope", {
            **body(), "scope": {"kind": "fix", "fix_keys": [fix["fix_key"]]},
            "status": "suspected", "origin": "manual", "issue_type": "GPS spikes",
            "comment": "Review GPS spike before upgrade",
        })
        assert flagged.status_code == 200, flagged.text
        head = flagged.json()["dataset"]["dataset_id"]
        parent = flagged.json()["step"]["summary"]["annotation_id"]
        saved = post_review(client, base, "review-individual", {
            **body(), "decision": {"individual": fix["individual"], "review_decision": "fix_keep",
                                   "needs_check": True, "comment": "René reviewed 150°"},
        })
        assert saved.status_code == 200, saved.text
        head = saved.json()["dataset"]["dataset_id"]
        def report():
            result = client.post(base + "/actions/generate-report", json={
                **body(), "report_type": "individual_profile", "individuals": [fix["individual"]],
            })
            assert result.status_code == 200, result.text
            return result.json()["analysis"]["analysis_id"]
        old_report = report()
        url = f"{base}/analysis/{old_report}/artifact/movement_individual_reports.html"
        downloaded = client.get(url)
        assert downloaded.status_code == 200, downloaded.text
        assert "René reviewed 150°" in downloaded.text
        report_before = downloaded.content
        _, sidecar = state.get_dataset_artifact(study, head, "movement_review_annotations.json")
        decisions_before = sidecar.read_bytes()
        export_action = "export-reviewed-rds" if source_format == "rds" else "export-reviewed-csv"
        exported = client.post(base + "/actions/" + export_action, json={**body(), "writer": "python"})
        assert exported.status_code == 200, exported.text
        export_record = exported.json()["analysis"]
        output_name = export_record["realized_output_artifacts"][0]["logical_name"]
        export_url = f'{base}/analysis/{export_record["analysis_id"]}/artifact/{output_name}'
        downloaded = client.get(export_url)
        assert downloaded.status_code == 200, downloaded.text
        export_before = downloaded.content

    assert client.get(base + "/load").json()["dataset_id"] == head
    assert client.get(url).content == report_before
    assert client.get(export_url).content == export_before
    _, sidecar = state.get_dataset_artifact(study, head, "movement_review_annotations.json")
    assert sidecar.read_bytes() == decisions_before
    confirmed = post_review(client, base, "confirm-issues", {
        **body(), "confirmations": [{"parent_annotation_id": parent, "fix_keys": [fix["fix_key"]]}],
    })
    assert confirmed.status_code == 200, confirmed.text
    head = confirmed.json()["dataset"]["dataset_id"]
    new_report = report()
    rendered = client.get(f"{base}/analysis/{new_report}/artifact/movement_individual_reports.html")
    assert rendered.status_code == 200
    assert "René reviewed 150°" in rendered.text
    assert "Fix &amp; Keep" in rendered.text
    exported = client.post(base + "/actions/" + export_action, json={**body(), "writer": "python"})
    assert exported.status_code == 200, exported.text
    assert raw_hashes == {path.name: path.read_bytes() for path in state.iter_source_files(study)}
