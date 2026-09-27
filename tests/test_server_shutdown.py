"""Exercise normal server shutdown with a browser's event connection left open."""
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.auth import build_user_record, users_path, write_users_file
from app.server import create_server
from app.web import create_app
from examples.movement.routes import register_movement_routes


@pytest.mark.parametrize("profile,family", [
    ("rds_movement", "movement_rds"),
    ("slim_movement", "movement_raw"),
])
def test_one_interrupt_closes_open_study_events(tmp_path, profile, family):
    data = tmp_path / "data"
    shutil.copytree(ROOT / "tests/fixtures/deployment/data" / family, data / family)
    write_users_file(users_path(data), [build_user_record(
        username="reviewer", display_name="Reviewer", role="editor", password="test-password",
    )])
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    env = {**os.environ, "VIBECLEANING_DATA_ROOT": str(data),
           "VIBECLEANING_CACHE_ROOT": str(tmp_path / "cache"),
           "HOST": "127.0.0.1", "PORT": str(port), "PYTHONUTF8": "1"}
    env.pop("PYTHONPATH", None)
    options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {}
    log = tmp_path / "server.log"
    with log.open("w", encoding="utf-8") as output:
        process = subprocess.Popen(
            [sys.executable, str(ROOT / "examples" / profile / "server.py")],
            cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, **options,
        )
        try:
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10) as client:
                deadline = time.monotonic() + 30
                while True:
                    assert process.poll() is None, log.read_text(encoding="utf-8")
                    try:
                        response = client.get("/")
                        if response.status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    assert time.monotonic() < deadline, log.read_text(encoding="utf-8")
                    time.sleep(0.05)
                response = client.post("/api/auth/login", json={
                    "username": "reviewer", "password": "test-password",
                })
                response.raise_for_status()
                base = f"/api/apps/movement/family/{family}/study/acceptance"
                client.get(f"{base}/load").raise_for_status()
                with client.stream("GET", f"{base}/events") as stream:
                    stream.raise_for_status()
                    lines = stream.iter_lines()
                    assert next(lines) == "event: study_state_changed"
                    # CTRL_BREAK is the console signal available to a Windows
                    # subprocess group; SIGINT is the Unix Ctrl+C equivalent.
                    process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
                    try:
                        process.wait(timeout=8)
                    except subprocess.TimeoutExpired:
                        pytest.fail("Server still waits for an open event stream after one interrupt:\n"
                                    + log.read_text(encoding="utf-8"))
                    # A graceful close finishes the HTTP response, not a reset.
                    list(lines)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    text = log.read_text(encoding="utf-8")
    assert "Application shutdown complete" in text, text
    assert "timeout graceful shutdown exceeded" not in text, text
    assert "CancelledError" not in text, text


def test_shutdown_finishes_streams_without_cancelling_active_requests(tmp_path):
    data = tmp_path / "data"
    shutil.copytree(ROOT / "tests/fixtures/deployment/data/movement_raw", data / "movement_raw")
    app = create_app(data_root=data, static_root=ROOT / "static")
    register_movement_routes(app, data_root=data, cache_root=tmp_path / "cache")
    request_started = threading.Event()
    finish_request = threading.Event()

    @app.get("/slow-request")
    async def slow_request():
        request_started.set()
        assert await asyncio.to_thread(finish_request.wait, 15)
        return {"finished": True}

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = create_server(app, host="127.0.0.1", port=port, log_level="warning")
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        deadline = time.monotonic() + 10
        while not server.started:
            assert thread.is_alive() and time.monotonic() < deadline
            time.sleep(0.01)
        base_url = f"http://127.0.0.1:{port}"
        with httpx.Client(base_url=base_url, timeout=10) as client:
            base = "/api/apps/movement/family/movement_raw/study/acceptance"
            client.get(f"{base}/load").raise_for_status()
            with client.stream("GET", f"{base}/events") as stream:
                stream.raise_for_status()
                lines = stream.iter_lines()
                assert next(lines) == "event: study_state_changed"
                pending = executor.submit(httpx.get, f"{base_url}/slow-request", timeout=15)
                assert request_started.wait(timeout=5)
                server.handle_exit(signal.SIGINT, None)
                list(lines)  # SSE finishes while the other request is still running.
                assert thread.is_alive() and not pending.done()
                finish_request.set()
                response = pending.result(timeout=5)
                assert response.status_code == 200
                assert response.json() == {"finished": True}
        thread.join(timeout=5)
        assert not thread.is_alive()
    finally:
        finish_request.set()
        server.should_exit = True
        server.force_exit = True
        executor.shutdown(wait=True)
        thread.join(timeout=5)
