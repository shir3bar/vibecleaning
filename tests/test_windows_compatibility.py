from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import time

import portalocker
import pytest

from app import auth_cli
from app.auth import Actor, build_user_record, read_users_file, users_path, write_users_file
from app.cache_cli import clear_rds_cache, main as cache_main
from app.events import shared_study_state
from app.filesystem import (
    FileLockTimeoutError,
    atomic_write_text,
    exclusive_file_lock,
)
from app.query_library import list_queries, save_query
from app.reviews import assign_review, load_review_state, start_editor_control
from app.runtime import (
    RuntimeConfigurationError,
    resolve_cache_root,
    resolve_data_root,
    shared_locking_mode,
    validate_cache_root,
    validate_data_root,
)
from app.state import ensure_project_state, load_dataset, save_dataset, update_project_state


def _hold_process_lock(path: str, ready, release) -> None:
    with exclusive_file_lock(Path(path), timeout=5):
        ready.set()
        release.wait(10)


def _attempt_process_lock(path: str, results) -> None:
    try:
        with exclusive_file_lock(Path(path), timeout=0.25):
            results.put("acquired")
    except FileLockTimeoutError:
        results.put("timeout")


def _query_payload(name: str) -> dict:
    return {
        "query_id": name,
        "app": "movement",
        "name": name,
        "description": "concurrency test",
        "candidate_kind": "fix",
        "evaluator": {"type": "fix_string_comparison"},
        "definition": {"field": "individual", "op": "==", "value": name},
        "parameters": {},
        "required_fields": ["individual"],
        "created_by": "test",
    }


def test_runtime_path_precedence_and_non_ascii_names(tmp_path, monkeypatch):
    environment_root = tmp_path / "shared data ü" / "projects"
    explicit_root = tmp_path / "explicit data 漢字"
    environment_cache = tmp_path / "cache from env"
    explicit_cache = tmp_path / "explicit cache ü"
    environment_root.mkdir(parents=True)
    explicit_root.mkdir(parents=True)
    monkeypatch.setenv("VIBECLEANING_DATA_ROOT", str(environment_root))
    monkeypatch.setenv("VIBECLEANING_CACHE_ROOT", str(environment_cache))

    assert resolve_data_root() == environment_root.resolve()
    assert resolve_data_root(explicit_root) == explicit_root.resolve()
    assert resolve_cache_root() == environment_cache.resolve()
    assert resolve_cache_root(explicit_cache) == explicit_cache.resolve()
    assert validate_data_root(environment_root) == environment_root.resolve()
    assert validate_cache_root(environment_cache) == environment_cache.resolve()


def test_unc_style_data_root_is_accepted_without_posix_only_validation():
    value = r"\\university-server\research\movement data ü"
    resolved = resolve_data_root(value)

    assert isinstance(resolved, Path)
    assert "university-server" in str(resolved)
    assert "movement data ü" in str(resolved)


@pytest.mark.skipif(os.name != "nt", reason="Windows platform default")
def test_windows_cache_default_uses_local_app_data(tmp_path, monkeypatch):
    monkeypatch.delenv("VIBECLEANING_CACHE_ROOT", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "Local App Data"))

    assert resolve_cache_root() == (
        tmp_path / "Local App Data" / "Vibecleaning" / "cache"
    ).resolve()


def test_runtime_rejects_invalid_locking_mode_and_missing_data_root(tmp_path):
    with pytest.raises(RuntimeConfigurationError):
        shared_locking_mode("sometimes")
    with pytest.raises(RuntimeConfigurationError, match="does not exist"):
        validate_data_root(tmp_path / "missing")


def test_shared_lock_is_reentrant_and_times_out_across_processes(tmp_path):
    lock_path = tmp_path / "share with spaces" / "project.lock"
    with exclusive_file_lock(lock_path):
        with exclusive_file_lock(lock_path):
            assert lock_path.exists()

    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    results = context.Queue()
    holder = context.Process(
        target=_hold_process_lock,
        args=(str(lock_path), ready, release),
    )
    holder.start()
    assert ready.wait(10)
    contender = context.Process(
        target=_attempt_process_lock,
        args=(str(lock_path), results),
    )
    contender.start()
    contender.join(10)
    assert contender.exitcode == 0
    assert results.get(timeout=2) == "timeout"
    release.set()
    holder.join(10)
    assert holder.exitcode == 0

    with exclusive_file_lock(lock_path, timeout=1):
        pass


def test_cooperative_mode_skips_only_external_file_lock(tmp_path, monkeypatch, caplog):
    lock_path = tmp_path / "cooperative.lock"
    monkeypatch.setenv("VIBECLEANING_SHARED_LOCKING", "disabled")
    monkeypatch.setattr("app.filesystem._cooperative_warning_emitted", False)
    external = portalocker.Lock(str(lock_path), mode="a+", timeout=1)
    with external:
        with exclusive_file_lock(lock_path, timeout=0.05):
            atomic_write_text(tmp_path / "authoritative.json", '{"ok": true}\n')

    assert json.loads((tmp_path / "authoritative.json").read_text()) == {"ok": True}
    assert "one active writer" in caplog.text


def test_atomic_write_retries_transient_replace_errors(tmp_path, monkeypatch):
    destination = tmp_path / "state.json"
    destination.write_text("old", encoding="utf-8")
    real_replace = os.replace
    calls = 0

    def flaky_replace(source, target):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise PermissionError("temporarily in use")
        real_replace(source, target)

    monkeypatch.setattr("app.filesystem.os.replace", flaky_replace)
    monkeypatch.setattr("app.filesystem.time.sleep", lambda _delay: None)
    atomic_write_text(destination, "new")

    assert calls == 3
    assert destination.read_text(encoding="utf-8") == "new"


def test_atomic_write_preserves_destination_after_retry_exhaustion(tmp_path, monkeypatch):
    destination = tmp_path / "state.json"
    destination.write_text("old", encoding="utf-8")
    monkeypatch.setattr(
        "app.filesystem.os.replace",
        lambda _source, _target: (_ for _ in ()).throw(PermissionError("busy")),
    )
    monkeypatch.setattr("app.filesystem.time.sleep", lambda _delay: None)

    with pytest.raises(PermissionError):
        atomic_write_text(destination, "new")

    assert destination.read_text(encoding="utf-8") == "old"
    assert list(tmp_path.glob(".state.json.*.tmp")) == []


def test_query_library_concurrent_updates_are_not_lost(tmp_path):
    data_root = tmp_path / "shared"
    data_root.mkdir()
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda name: save_query(data_root, _query_payload(name)), ["one", "two"]))

    assert {item["query_id"] for item in results} == {"one", "two"}
    persisted = {item["query_id"] for item in list_queries(data_root)}
    assert {"one", "two"} <= persisted


def test_account_cli_serializes_registry_updates(tmp_path):
    data_root = tmp_path / "shared"
    path = users_path(data_root)
    write_users_file(
        path,
        [
            build_user_record(
                username="reviewer",
                display_name="Reviewer",
                role="reviewer",
                password="long-test-password",
            )
        ],
    )

    def update(command: str) -> int:
        return auth_cli.main(["--data-root", str(data_root), command, "reviewer"])

    with ThreadPoolExecutor(max_workers=2) as executor:
        return_codes = list(executor.map(update, ["enable", "disable"]))

    assert return_codes == [0, 0]
    users = read_users_file(path)
    assert len(users) == 1
    assert isinstance(users[0]["enabled"], bool)
    assert users[0]["auth_version"] == 3


def test_clear_rds_cache_requires_scope_and_honors_age(tmp_path, capsys):
    cache_root = tmp_path / "cache"
    directory = cache_root / "movement" / "rds"
    directory.mkdir(parents=True)
    old = directory / "old.sqlite"
    fresh = directory / "fresh.sqlite"
    old.write_bytes(b"old")
    fresh.write_bytes(b"fresh")
    old_timestamp = time.time() - (10 * 24 * 60 * 60)
    os.utime(old, (old_timestamp, old_timestamp))

    with pytest.raises(SystemExit):
        cache_main(["--cache-root", str(cache_root), "clear-rds"])
    assert clear_rds_cache(cache_root, older_than_days=5) == [old]
    assert not old.exists()
    assert fresh.exists()
    assert clear_rds_cache(cache_root, remove_all=True) == [fresh]
    assert "Removed" not in capsys.readouterr().out


def test_shared_study_fingerprint_tracks_assignment_control_and_head(tmp_path):
    project = tmp_path / "study"
    project.mkdir()
    (project / "movement.csv").write_text("individual,value\nalpha,1\n")
    initial = ensure_project_state(project)
    editor = Actor("editor-id", "editor", "Editor", "editor")
    reviewer = Actor("reviewer-id", "reviewer", "Reviewer", "reviewer")
    before = shared_study_state(project)

    assigned = assign_review(
        project,
        editor=editor,
        reviewer=reviewer,
        expected_current_dataset_id=initial["current_dataset_id"],
        expected_review_revision=0,
        individuals=["alpha"],
    )
    after_assignment = shared_study_state(project)
    assert after_assignment["current_dataset_id"] == before["current_dataset_id"]
    assert after_assignment["assignment_fingerprint"] != before["assignment_fingerprint"]
    assert after_assignment["state_fingerprint"] != before["state_fingerprint"]

    revision = load_review_state(project)["revision"]
    start_editor_control(
        project,
        editor=editor,
        expected_current_dataset_id=initial["current_dataset_id"],
        expected_review_revision=revision,
        reason="test",
    )
    after_control = shared_study_state(project)
    assert after_control["editor_control_fingerprint"] != after_assignment[
        "editor_control_fingerprint"
    ]

    external_dataset = load_dataset(project, initial["current_dataset_id"])
    external_dataset.update(
        {
            "dataset_id": "external-head",
            "parent_dataset_id": initial["current_dataset_id"],
        }
    )
    save_dataset(project, external_dataset)
    update_project_state(project, {"current_dataset_id": "external-head"})
    after_head = shared_study_state(project)
    assert after_head["current_dataset_id"] == "external-head"
    assert after_head["state_fingerprint"] != after_control["state_fingerprint"]


def test_local_sqlite_cache_can_be_deleted_and_rebuilt(tmp_path):
    cache = tmp_path / "cache" / "movement" / "rds" / "signature.sqlite"
    cache.parent.mkdir(parents=True)
    with sqlite3.connect(cache) as connection:
        connection.execute("CREATE TABLE marker (value TEXT)")
    cache.unlink()
    with sqlite3.connect(cache) as connection:
        connection.execute("CREATE TABLE marker (value TEXT)")
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_windows_launcher_and_ci_cover_supported_interfaces():
    root = Path(__file__).resolve().parents[1]
    launcher = (root / "scripts" / "start-vibecleaning.ps1").read_text(encoding="utf-8")
    workflow = (root / ".github" / "workflows" / "test.yml").read_text(encoding="utf-8")

    for parameter in ("$Profile", "$DataRoot", "$CacheRoot", "$Port", "$SharedLocking"):
        assert parameter in launcher
    assert '$env:HOST = "127.0.0.1"' in launcher
    assert "TcpListener" in launcher
    assert "windows-latest" in workflow
    assert "ubuntu-latest" in workflow
    assert "macos-latest" in workflow
    assert "playwright install chromium" in workflow
