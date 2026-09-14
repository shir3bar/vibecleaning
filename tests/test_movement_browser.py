from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import asyncio
import json
import shutil
import socket
import sys
import threading
import time

import pytest
import uvicorn

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.auth import AuthManager
from app.execution import create_analysis, create_step
from app.state import ensure_project_state
from app.web import create_app
from examples.movement.analysis_history import BURST_FEATURE_SIGNATURE
from examples.movement.routes import register_movement_routes
from examples.rds_movement.app import create_rds_movement_app


STATIC_ROOT = REPO_ROOT / "examples" / "movement" / "static"
INDEX_PATH = STATIC_ROOT / "index.html"
RDS_SAMPLE_ROOT = REPO_ROOT / "data" / "movement_rds"

CSV_BROWSER_FIXTURE = """eventid,individual,timestamp,longitude,latitude,set
a1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
a2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
b1,beta,2024-01-01T00:30:00Z,-71.0,41.0,train
b2,beta,2024-01-01T01:30:00Z,-71.1,41.1,train
c1,gamma,2024-01-01T00:45:00Z,-72.0,42.0,train
c2,gamma,2024-01-01T01:45:00Z,-72.1,42.1,train
"""

CSV_QUEUE_TRANSITION_FIXTURE = """eventid,individual,timestamp,longitude,latitude,set
a1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
a2,alpha,2024-01-01T01:00:00Z,-70.1,40.1,train
b1,beta,2024-01-01T00:00:00Z,-71.0,41.0,train
b2,beta,2024-01-01T01:00:00Z,-71.1,41.1,train
c1,gamma,2024-01-01T00:00:00Z,-72.0,42.0,train
c2,gamma,2024-01-01T01:00:00Z,-72.1,42.1,train
d1,delta,2024-01-01T00:00:00Z,-73.0,43.0,train
d2,delta,2024-01-01T01:00:00Z,-73.1,43.1,train
e1,epsilon,2024-01-01T00:00:00Z,-74.0,44.0,train
e2,epsilon,2024-01-01T01:00:00Z,-74.1,44.1,train
z1,zeta,2024-01-01T00:00:00Z,-75.0,45.0,train
z2,zeta,2024-01-01T01:00:00Z,-75.1,45.1,train
"""

CSV_TRACK_PLAYER_FIXTURE = (
    """eventid,individual,timestamp,longitude,latitude,set
a0,alpha,2024-01-01T00:00:00Z,-70.0000,40.0000,train
a2,alpha,2024-01-01T02:00:00Z,-70.0004,40.0003,train
a1,alpha,2024-01-01T01:00:00Z,-70.0002,40.0001,test
a3,alpha,2024-01-01T10:00:00Z,-70.0005,40.0005,test
a1b,alpha,2024-01-01T01:00:00Z,-70.0002,40.0001,test
"""
    + "".join(
        f"ax{index},alpha,2024-01-02T{index:02d}:00:00Z,"
        f"{-70.001 - (index * 0.0001):.4f},{40.001 + (index * 0.0001):.4f},train\n"
        for index in range(20)
    )
    + "b0,beta,2024-01-01T00:30:00Z,-71.0000,41.0000,train\n"
)

FORWARD_HEAD_STEP_SCRIPT = '''import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
output = Path(spec["output_artifacts"][0]["path"])
output.write_text("forward history")
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text(
    json.dumps({"restorable": True})
)
'''


def _create_browser_ranking(study_dir: Path) -> None:
    dataset_id = ensure_project_state(study_dir)["current_dataset_id"]
    script = '''import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
output = next(
    item for item in spec["output_artifacts"]
    if item["logical_name"] == "burst_anomaly_ranking.json"
)
individuals = ["alpha", "beta", "gamma"]
worst = [
    {"rank": index + 1, "individual": individual, "individual_score": score, "top_burst_score": score}
    for index, (individual, score) in enumerate(zip(individuals, [0.9, 0.6, 0.3]))
]
margin = [
    {"rank": index + 1, "individual": individual, "individual_score": score, "top_burst_score": score}
    for index, (individual, score) in enumerate(zip(individuals, [0.75, 0.5, 0.25]))
]
Path(output["path"]).write_text(json.dumps({
    "run_status": "completed",
    "ranking_method": "isolation_forest",
    "scored_bursts": [],
    "individual_rankings": {
        "isolation_forest": {"ranked_individuals": worst},
        "isolation_forest_decision_margin": {"ranked_individuals": margin},
    },
}))
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text(
    json.dumps({"run_status": "completed"})
)
'''
    create_analysis(
        study_dir,
        {
            "user": "browser-reviewer",
            "title": "Browser ranking fixture",
            "kind": "python",
            "script": script,
            "dataset_id": dataset_id,
            "input_artifacts": ["movement.csv"],
            "output_artifacts": ["burst_anomaly_ranking.json"],
            "parameters": {
                "app": "movement",
                "action": "run_burst_anomaly_ranking",
                "target_artifact": "movement.csv",
                "burst_gap_mode": "quantile",
                "burst_gap_seconds": 3600,
                "burst_gap_quantile": 0.999,
                "feature_set": "movement_only",
                "ranking_method": "isolation_forest",
                "ranking_provider": "isolation_forest",
                "burst_feature_signature": BURST_FEATURE_SIGNATURE,
            },
        },
    )


def _auth_manager() -> AuthManager:
    return AuthManager.for_testing(
        username="browser-reviewer",
        password="test-password-long",
        role="editor",
    )


@contextmanager
def _serve(app):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
    ))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.01)
    if not server.started:
        raise RuntimeError("Browser test server did not start")
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def _open_browser(playwright):
    try:
        return playwright.chromium.launch(headless=True)
    except Exception as exc:  # pragma: no cover - depends on local browser install
        pytest.skip(f"Playwright Chromium is unavailable: {exc}")


def _login_and_wait(page, base_url: str, study_name: str) -> None:
    page.goto(base_url, wait_until="domcontentloaded")
    page.locator("#login-username").fill("browser-reviewer")
    page.locator("#login-password").fill("test-password-long")
    page.locator("#login-submit").click()
    page.locator(".movement-root").wait_for(state="visible", timeout=20_000)
    page.locator('[data-role="map"] canvas').first.wait_for(state="attached", timeout=20_000)
    page.wait_for_function("window.__movementDiagnostics !== undefined")
    page.locator(f'[data-role="study"] option[value="{study_name}"]').wait_for(
        state="attached", timeout=20_000
    )
    page.locator('[data-role="study"]').select_option(study_name)


def _delay_binary_responses(app, seconds: float = 0.3) -> None:
    @app.middleware("http")
    async def delay_binary(request, call_next):
        if request.url.path.endswith("/fixes-binary"):
            await asyncio.sleep(seconds)
        return await call_next(request)


def _layer_ids(page) -> list[str]:
    return page.evaluate("[...window.__movementDiagnostics.renderedLayerIds]")


def _wait_for_layer(page, fragment: str) -> None:
    page.wait_for_function(
        "fragment => window.__movementDiagnostics.renderedLayerIds.some(id => id.includes(fragment))",
        arg=fragment,
        timeout=20_000,
    )


def test_csv_progressive_loading_preserves_dom_and_warm_blocks(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    study_dir = tmp_path / "data" / "movement_clean" / "browser_study"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(CSV_BROWSER_FIXTURE, encoding="utf-8")
    _create_browser_ranking(study_dir)
    app = create_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )
    register_movement_routes(
        app,
        data_root=tmp_path / "data",
        overview_fix_limit=1,
        overview_series_points=250,
    )
    _delay_binary_responses(app)

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        binary_requests = []
        page.on("request", lambda request: binary_requests.append(request.url)
                if "/fixes-binary?" in request.url else None)
        _login_and_wait(page, base_url, "browser_study")
        page.wait_for_timeout(1_000)
        assert page.locator("[data-individual-checkbox]").count(), {
            "page_errors": page_errors,
            "status": page.locator('[data-role="status"]').text_content(),
        }
        map_node = page.locator('[data-role="map"]')
        canvas = page.locator('[data-role="map"] canvas').first
        page.evaluate("""
          window.__movementMapNode = document.querySelector('[data-role=map]');
          window.__movementCanvasNode = document.querySelector('[data-role=map] canvas');
          window.__movementGeometryGaps = [];
          window.__movementMonitorActive = true;
          const monitorGeometry = () => {
            if (!window.__movementMonitorActive) return;
            const selected = [...document.querySelectorAll('[data-individual-checkbox]')]
              .some(input => input.checked);
            const ids = window.__movementDiagnostics.renderedLayerIds;
            const geometry = ids.some(id => id.startsWith('movement-overview-preview-')
              || id.startsWith('movement-binary-paths-'));
            if (selected && !geometry) window.__movementGeometryGaps.push(performance.now());
            requestAnimationFrame(monitorGeometry);
          };
          requestAnimationFrame(monitorGeometry);
        """)

        alpha = page.locator('[data-individual-checkbox="alpha"]')
        page.evaluate("""
          window.__movementSelectionStart = performance.now();
          document.querySelector('[data-individual-checkbox="alpha"]').click();
        """)
        page.wait_for_function(
            "() => window.__movementDiagnostics.renderedLayerIds.includes('movement-overview-preview-0')"
        )
        assert page.evaluate(
            "window.__movementDiagnostics.lastPreviewActivationMs - window.__movementSelectionStart"
        ) < 100
        assert map_node.count() == 1
        assert canvas.count() == 1
        assert page.evaluate("""
          window.__movementMapNode === document.querySelector('[data-role=map]')
          && window.__movementCanvasNode === document.querySelector('[data-role=map] canvas')
        """)
        _wait_for_layer(page, "movement-binary-paths-individual-0")
        assert page.evaluate("window.__movementGeometryGaps.length") == 0
        assert len(binary_requests) == 1

        beta = page.locator('[data-individual-checkbox="beta"]')
        beta.check()
        page.wait_for_function(
            "() => window.__movementDiagnostics.renderedLayerIds.includes('movement-overview-preview-1')"
        )
        assert any("movement-binary-paths-individual-0" in item for item in _layer_ids(page))
        _wait_for_layer(page, "movement-binary-paths-individual-1")
        assert len(binary_requests) == 2
        assert page.evaluate("""
          window.__movementMapNode === document.querySelector('[data-role=map]')
          && window.__movementCanvasNode === document.querySelector('[data-role=map] canvas')
        """)

        alpha.uncheck()
        alpha.check()
        _wait_for_layer(page, "movement-binary-paths-individual-0")
        assert len(binary_requests) == 2

        page.locator('[data-role="select-all"]').click()
        page.wait_for_function(
            "() => window.__movementDiagnostics.renderedLayerIds.some(id => id.startsWith('movement-overview-preview-'))"
        )
        _wait_for_layer(page, "movement-binary-paths-full")
        assert len(binary_requests) == 3
        builds_after_full = page.evaluate("window.__movementDiagnostics.binaryAttributeBuilds")

        page.locator('[data-role="select-none"]').click()
        page.locator('[data-role="select-all"]').click()
        _wait_for_layer(page, "movement-binary-paths-full")
        assert len(binary_requests) == 3
        assert page.evaluate("window.__movementDiagnostics.binaryAttributeBuilds") == builds_after_full
        snapshot = page.evaluate("window.__movementDiagnosticsSnapshot()")
        assert snapshot["binaryBlockCount"] >= 1
        assert snapshot["binaryRowCount"] >= 6
        assert snapshot["workerBlockCount"] >= 1
        assert snapshot["renderCalls"] > 0
        assert snapshot["renderedLayerCount"] == len(snapshot["renderedLayerIds"])
        assert snapshot["documentNodeCount"] > snapshot["queueCardCount"]

        page.locator('[data-role="individual-view-queue"]').click()
        entire_individual = page.locator('button[data-queue-flag-individual]').first
        entire_individual.wait_for(state="visible", timeout=20_000)
        assert "is-active" in (
            page.locator('button[data-queue-scope="solo"]').get_attribute("class") or ""
        )
        entire_individual.click()
        page.wait_for_function(
            "() => document.querySelector('button[data-queue-flag-individual]')?.classList.contains('is-active')"
        )
        assert "Flag entire individual" in page.locator(
            '[data-role="mark-suspected"]'
        ).text_content()
        page.locator('button[data-queue-flag-individual]').first.click()
        page.wait_for_function(
            "() => !document.querySelector('button[data-queue-flag-individual]')?.classList.contains('is-active')"
        )
        assert page.locator('[data-role="mark-suspected"]').text_content() == "Choose what to flag"
        assert page.locator('[data-role="mark-suspected"]').is_disabled()

        page.locator('[data-role="ranking-method"]').select_option(
            "isolation_forest",
            force=True,
        )
        page.locator('[data-role="individual-queue-order"]').select_option(
            "isolation_forest_decision_margin"
        )
        page.locator("[data-queue-ranking-score]").first.wait_for(
            state="visible", timeout=20_000
        )
        assert page.locator("[data-queue-ranking-score]").count() == 3
        assert page.locator("[data-queue-ranking-score]").first.text_content() == (
            "#1 · score 0.75"
        )
        page.wait_for_function("""
          () => Object.values(localStorage).some(raw => {
            try {
              const state = JSON.parse(raw);
              return state.individualQueueOrder === 'ranking'
                && state.individualQueueRankingMethod === 'isolation_forest_decision_margin'
                && state.rankingMethod === 'isolation_forest';
            } catch {
              return false;
            }
          })
        """)
        page.reload(wait_until="domcontentloaded")
        page.locator(".movement-root").wait_for(state="visible", timeout=20_000)
        page.locator('[data-role="map"] canvas').first.wait_for(
            state="attached", timeout=20_000
        )
        page.locator(
            '[data-role="study"] option[value="browser_study"]'
        ).wait_for(state="attached", timeout=20_000)
        page.locator('[data-role="study"]').select_option("browser_study")
        page.locator('[data-individual-checkbox="alpha"]').wait_for(
            state="attached", timeout=20_000
        )
        assert page.locator('[data-role="ranking-method"]').input_value() == (
            "isolation_forest"
        )
        assert page.locator('[data-role="individual-queue-order"]').input_value() == "dataset"
        page.locator('[data-role="individual-view-queue"]').click()
        page.locator('button[data-queue-scope="solo"]').wait_for(
            state="visible", timeout=20_000
        )
        assert "is-active" in (
            page.locator('button[data-queue-scope="solo"]').get_attribute("class") or ""
        )
        assert page.locator("[data-queue-ranking-score]").count() == 0

        page.evaluate("window.__movementMonitorActive = false")
        browser.close()


def test_queue_track_player_uses_fix_index_timeline_and_ignores_sets(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    study_dir = tmp_path / "data" / "movement_clean" / "track_player"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(
        CSV_TRACK_PLAYER_FIXTURE,
        encoding="utf-8",
    )
    app = create_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )
    register_movement_routes(app, data_root=tmp_path / "data")

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        binary_requests = []
        page.on(
            "request",
            lambda request: binary_requests.append(request.url)
            if "/fixes-binary?" in request.url else None,
        )
        _login_and_wait(page, base_url, "track_player")
        page.locator('[data-individual-checkbox="alpha"]').wait_for(
            state="attached", timeout=20_000
        )
        page.locator('[data-role="individual-view-queue"]').click()
        player = page.locator('[data-role="track-player"]')
        player.wait_for(state="visible", timeout=20_000)
        page.wait_for_function(
            "() => document.querySelector('[data-role=track-player-canvas]')?.dataset.count === '25'",
            timeout=20_000,
        )
        assert page.locator('[data-role="legend"]').evaluate(
            "element => element.classList.contains('hidden')"
        )
        assert page.locator('[data-role="threshold-pane"]').evaluate(
            "element => element.classList.contains('hidden')"
        )
        page.locator('[data-role="color-by"]').select_option("speed_mps")
        page.wait_for_function(
            "() => window.__movementDiagnostics.queueContextGrayCount === 0"
            " && window.__movementDiagnostics.queueContextColoredCount > 0",
            timeout=20_000,
        )

        timeline_slider = page.locator('[data-role="slider"]')
        slider = page.locator('[data-role="track-player-slider"]')
        canvas = page.locator('[data-role="track-player-canvas"]')
        assert slider.get_attribute("min") == "0"
        assert slider.get_attribute("max") == "24"
        assert slider.get_attribute("step") == "1"
        assert page.locator('[data-role="track-player-position"]').text_content() == "fix 1 of 25"
        assert int(canvas.get_attribute("data-time-ms")) == 1_704_067_200_000
        assert canvas.get_attribute("data-window-start") == "0"
        assert canvas.get_attribute("data-window-end") == "23"
        assert page.locator("canvas.maplibregl-canvas").count() == 1

        canvas.click(position={"x": 40, "y": 40})
        assert page.evaluate(
            "() => document.activeElement?.dataset?.role === 'track-player'"
        )
        page.keyboard.press("ArrowRight")
        assert canvas.get_attribute("data-index") == "1"
        assert slider.input_value() == "1"
        page.keyboard.press("ArrowLeft")
        assert canvas.get_attribute("data-index") == "0"
        assert slider.input_value() == "0"

        page.locator('[data-role="track-player-view"]').select_option("trail")
        assert canvas.get_attribute("data-window-end") == "0"
        page.locator('[data-role="track-player-view"]').select_option("context")
        assert canvas.get_attribute("data-window-end") == "23"

        page.locator('[data-role="track-player-hide"]').click()
        player.wait_for(state="hidden", timeout=20_000)
        show_player = page.locator('[data-role="track-player-show"]')
        show_player.wait_for(state="visible", timeout=20_000)
        show_player.click()
        player.wait_for(state="visible", timeout=20_000)

        page.wait_for_function("() => window.__movementDiagnostics.mapView !== null")
        map_view_before = page.evaluate(
            "() => structuredClone(window.__movementDiagnostics.mapView)"
        )
        canvas.click(position={"x": 40, "y": 40})
        page.keyboard.press("ArrowRight")
        page.wait_for_function(
            "() => document.querySelector('[data-role=track-player-canvas]')?.dataset.index === '1'"
        )
        assert int(canvas.get_attribute("data-time-ms")) == 1_704_070_800_000
        first_equal_time_source_row = int(canvas.get_attribute("data-source-row"))
        page.keyboard.press("ArrowRight")
        assert int(canvas.get_attribute("data-time-ms")) == 1_704_070_800_000
        assert int(canvas.get_attribute("data-source-row")) > first_equal_time_source_row
        map_view_after = page.evaluate(
            "() => structuredClone(window.__movementDiagnostics.mapView)"
        )
        assert map_view_after["center"] == pytest.approx(map_view_before["center"])
        assert map_view_after["zoom"] == pytest.approx(map_view_before["zoom"])

        page.locator('[data-queue-individual="beta"]').click()
        page.wait_for_function(
            "() => document.querySelector('[data-role=track-player-canvas]')?.dataset.individual === 'beta' && document.querySelector('[data-role=track-player-canvas]')?.dataset.count === '1'",
            timeout=20_000,
        )
        assert canvas.get_attribute("data-index") == "0"
        assert page.locator('[data-role="track-player-play"]').is_disabled()
        page.locator('[data-queue-individual="alpha"]').click()
        page.wait_for_function(
            "() => document.querySelector('[data-role=track-player-canvas]')?.dataset.individual === 'alpha' && document.querySelector('[data-role=track-player-canvas]')?.dataset.count === '25'",
            timeout=20_000,
        )
        assert canvas.get_attribute("data-index") == "0"

        page.locator('[data-role="track-player-play"]').click()
        page.wait_for_function(
            "() => window.__movementDiagnosticsSnapshot().trackPlayerPlaying === true"
        )
        page.keyboard.press("ArrowRight")
        assert page.evaluate(
            "() => window.__movementDiagnosticsSnapshot().trackPlayerPlaying"
        ) is False

        request_count = len(binary_requests)
        page.locator('[data-role="show-train"]').uncheck()
        assert canvas.get_attribute("data-count") == "25"
        assert slider.get_attribute("max") == "24"
        slider.evaluate("element => { element.value = '23'; element.dispatchEvent(new Event('input', {bubbles: true})); }")
        assert canvas.get_attribute("data-window-start") == "0"
        assert canvas.get_attribute("data-window-end") == "23"
        canvas.click(position={"x": 40, "y": 40})
        page.keyboard.press("ArrowRight")
        assert canvas.get_attribute("data-index") == "24"
        assert canvas.get_attribute("data-window-start") == "24"
        assert canvas.get_attribute("data-window-end") == "24"
        page.locator('[data-role="track-player-window"]').select_option("72")
        assert page.locator('[data-role="track-player-window"]').input_value() == "72"
        page.locator('[data-role="track-player-playback"]').select_option("scan")
        assert page.locator('[data-role="track-player-playback"]').input_value() == "scan"
        page.locator('[data-role="track-player-speed"]').select_option("2")
        assert page.locator('[data-role="track-player-speed"]').input_value() == "2"

        slider.evaluate("element => { element.value = '24'; element.dispatchEvent(new Event('input', {bubbles: true})); }")
        page.wait_for_function(
            "() => document.querySelector('[data-role=track-player-canvas]')?.dataset.index === '24'"
        )
        page.locator('[data-role="track-player-play"]').click()
        page.wait_for_function(
            "() => window.__movementDiagnosticsSnapshot().trackPlayerPlaying === true"
        )
        page.wait_for_function(
            "() => { const value = window.__movementDiagnosticsSnapshot(); return !value.trackPlayerPlaying && value.trackPlayerIndex === 24; }",
            timeout=5_000,
        )
        assert len(binary_requests) == request_count
        assert canvas.get_attribute("data-window-start") == "0"
        assert canvas.get_attribute("data-window-end") == "24"

        page.locator('[data-role="individual-view-browse"]').click()
        player.wait_for(state="hidden", timeout=20_000)
        page.locator('[data-role="legend"]').wait_for(state="visible", timeout=20_000)
        page.locator('[data-role="threshold-pane"]').wait_for(state="visible", timeout=20_000)
        assert int(timeline_slider.get_attribute("max")) > 1_000_000_000_000
        assert page.evaluate(
            "() => JSON.parse(localStorage.getItem('vibecleaning_movement_example_state') || '{}').trackPlayerWindowSize",
        ) == 72
        assert page.evaluate(
            "() => JSON.parse(localStorage.getItem('vibecleaning_movement_example_state') || '{}').trackPlayerSpeed",
        ) == 2
        assert page.evaluate(
            "() => JSON.parse(localStorage.getItem('vibecleaning_movement_example_state') || '{}').trackPlayerViewMode",
        ) == "context"
        assert page.evaluate(
            "() => JSON.parse(localStorage.getItem('vibecleaning_movement_example_state') || '{}').trackPlayerPlaybackMode",
        ) == "scan"
        assert page.evaluate(
            "() => JSON.parse(localStorage.getItem('vibecleaning_movement_example_state') || '{}').trackPlayerHidden",
        ) is False
        browser.close()


def test_roi_drawing_uses_queue_and_browse_scopes(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    study_dir = tmp_path / "data" / "movement_clean" / "roi_ui"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(
        CSV_TRACK_PLAYER_FIXTURE,
        encoding="utf-8",
    )
    app = create_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )
    register_movement_routes(app, data_root=tmp_path / "data")

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        _login_and_wait(page, base_url, "roi_ui")
        page.locator('[data-role="individual-view-queue"]').click()
        page.locator('[data-role="track-player"]').wait_for(
            state="visible", timeout=20_000
        )
        page.locator('[data-role="track-player-hide"]').click()
        page.locator('[data-role="roi-draw"]').click()
        roi_panel = page.locator('[data-role="roi-panel"]')
        roi_panel.wait_for(state="visible", timeout=20_000)
        roi_scope = page.locator('[data-role="roi-scope"]')
        assert roi_scope.input_value() == "active_individual"
        assert roi_scope.is_disabled()

        map_box = page.locator('[data-role="map"]').bounding_box()
        assert map_box is not None
        for x_fraction, y_fraction in (
            (0.10, 0.30),
            (0.90, 0.30),
            (0.90, 0.90),
            (0.10, 0.90),
        ):
            page.mouse.click(
                map_box["x"] + (map_box["width"] * x_fraction),
                map_box["y"] + (map_box["height"] * y_fraction),
            )
        page.wait_for_function(
            "() => window.__movementDiagnosticsSnapshot().roiVertexCount === 4"
        )
        page.locator('[data-role="roi-finish"]').click()
        page.wait_for_function(
            "() => !window.__movementDiagnosticsSnapshot().roiDrawing"
            " && document.querySelector('[data-role=roi-status]').textContent.includes('fixes inside ROI')",
            timeout=20_000,
        )

        page.locator('[data-role="individual-view-browse"]').click()
        roi_panel.wait_for(state="hidden", timeout=20_000)
        assert page.evaluate(
            "() => window.__movementDiagnosticsSnapshot().roiVertexCount"
        ) == 0
        page.locator('[data-role="roi-draw"]').click()
        roi_panel.wait_for(state="visible", timeout=20_000)
        assert roi_scope.input_value() == "whole_study"
        assert not roi_scope.is_disabled()
        browser.close()


def test_queue_decision_retains_exact_blocks_across_review_step(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    study_dir = tmp_path / "data" / "movement_clean" / "queue_transition"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(
        CSV_QUEUE_TRANSITION_FIXTURE,
        encoding="utf-8",
    )
    app = create_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )
    register_movement_routes(
        app,
        data_root=tmp_path / "data",
        overview_fix_limit=1,
        overview_series_points=250,
    )
    _delay_binary_responses(app, seconds=1.0)

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        binary_requests = []
        page.on(
            "request",
            lambda request: binary_requests.append(request.url)
            if "/fixes-binary?" in request.url else None,
        )
        _login_and_wait(page, base_url, "queue_transition")
        page.locator('[data-individual-checkbox="alpha"]').wait_for(
            state="attached", timeout=20_000
        )
        initial_dataset_id = page.locator('[data-role="dataset"]').input_value()

        page.locator('[data-role="individual-view-queue"]').click()
        page.evaluate("""() => {
          window.__queueIndividualsNode = document.querySelector('[data-role=individuals]');
          window.__queueAlphaCard = document.querySelector('[data-queue-individual="alpha"]');
          window.__queueBetaCard = document.querySelector('[data-queue-individual="beta"]');
          window.__queueMapCanvas = document.querySelector('[data-role=map] canvas');
        }""")
        page.keyboard.press("1")
        assert "OK" in page.locator(
            '[data-queue-individual="alpha"] .movement-review-state'
        ).text_content()
        assert "unsaved" in page.locator(
            '[data-queue-individual="alpha"] .movement-review-state'
        ).text_content()
        save = page.locator('[data-role="individual-queue-save"]')
        save.wait_for(state="visible", timeout=20_000)
        assert save.is_enabled()
        save.click()

        page.wait_for_function(
            """() => document.querySelector(
              '.movement-card.queue-active .movement-title'
            )?.textContent === 'beta'""",
            timeout=20_000,
        )
        _wait_for_layer(page, "movement-binary-points-individual-1")
        page.wait_for_function(
            """() => !window.__movementDiagnostics.renderedLayerIds.includes(
              'movement-overview-preview-1'
            )""",
            timeout=20_000,
        )
        final_dataset_id = page.locator('[data-role="dataset"]').input_value()
        assert final_dataset_id != initial_dataset_id
        assert len(binary_requests) == 2
        assert sum("individuals=alpha" in url for url in binary_requests) == 1
        assert sum("individuals=beta" in url for url in binary_requests) == 1
        assert page.evaluate("""() => (
          window.__queueIndividualsNode === document.querySelector('[data-role=individuals]')
          && window.__queueAlphaCard === document.querySelector('[data-queue-individual="alpha"]')
          && window.__queueBetaCard === document.querySelector('[data-queue-individual="beta"]')
          && window.__queueMapCanvas === document.querySelector('[data-role=map] canvas')
        )""")
        browser.close()


def test_queue_navigation_auto_saves_active_review_decision(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    study_dir = tmp_path / "data" / "movement_clean" / "queue_auto_save"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(
        CSV_QUEUE_TRANSITION_FIXTURE,
        encoding="utf-8",
    )
    app = create_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )
    register_movement_routes(
        app,
        data_root=tmp_path / "data",
        overview_fix_limit=1,
        overview_series_points=250,
    )

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        review_requests = []
        page.on(
            "request",
            lambda request: review_requests.append(request.url)
            if request.method == "POST" and request.url.endswith("/actions/review-individual")
            else None,
        )
        _login_and_wait(page, base_url, "queue_auto_save")
        page.locator('[data-individual-checkbox="alpha"]').wait_for(
            state="attached", timeout=20_000
        )
        page.locator('[data-role="individual-view-queue"]').click()
        page.evaluate("""() => {
          window.__queueIndividualsNode = document.querySelector('[data-role=individuals]');
          window.__queueAlphaCard = document.querySelector('[data-queue-individual="alpha"]');
          window.__queueBetaCard = document.querySelector('[data-queue-individual="beta"]');
          window.__queueMapCanvas = document.querySelector('[data-role=map] canvas');
        }""")

        page.wait_for_function("() => window.__movementDiagnostics.mapView !== null")
        scope_view_before = page.evaluate(
            "() => structuredClone(window.__movementDiagnostics.mapView)"
        )
        page.locator('button[data-queue-scope="solo"]').click()
        page.wait_for_function(
            """() => document.querySelector(
              'button[data-queue-scope="solo"]'
            )?.classList.contains('is-active')"""
        )
        scope_view_after = page.evaluate(
            "() => structuredClone(window.__movementDiagnostics.mapView)"
        )
        assert scope_view_after["center"] == pytest.approx(scope_view_before["center"])
        assert scope_view_after["zoom"] == pytest.approx(scope_view_before["zoom"])
        page.locator('[data-queue-individual="beta"]').click()
        page.wait_for_function(
            """() => document.querySelector(
              '.movement-card.queue-active .movement-title'
            )?.textContent === 'beta'""",
            timeout=20_000,
        )
        solo_navigation_view = page.evaluate(
            "() => structuredClone(window.__movementDiagnostics.mapView)"
        )
        assert solo_navigation_view["center"] == pytest.approx(scope_view_before["center"])
        assert solo_navigation_view["zoom"] == pytest.approx(scope_view_before["zoom"])
        page.locator('[data-queue-individual="alpha"]').click()
        page.wait_for_function(
            """() => document.querySelector(
              '.movement-card.queue-active .movement-title'
            )?.textContent === 'alpha'""",
            timeout=20_000,
        )
        page.locator('button[data-queue-scope="group"]').click()

        active_card = page.locator('[data-queue-individual="alpha"]')
        burst_visibility = active_card.locator(
            "input[data-queue-burst-visible]"
        ).first
        burst_visibility.wait_for(state="visible", timeout=20_000)
        page.evaluate("""() => {
          window.__queueBurstControls = document.querySelector(
            '[data-queue-individual="alpha"] [data-queue-burst-controls]'
          );
          window.__queueBurstVisibility = window.__queueBurstControls.querySelector(
            'input[data-queue-burst-visible]'
          );
          window.__queueBurstFlag = window.__queueBurstControls.querySelector(
            'input[data-queue-flag-burst]'
          );
        }""")
        burst_visibility.uncheck()
        burst_visibility.check()
        burst_flag = active_card.locator("input[data-queue-flag-burst]").first
        burst_flag.check()
        burst_flag.uncheck()
        assert page.evaluate("""() => (
          window.__queueBurstControls === document.querySelector(
            '[data-queue-individual="alpha"] [data-queue-burst-controls]'
          )
          && window.__queueBurstVisibility === document.querySelector(
            '[data-queue-individual="alpha"] input[data-queue-burst-visible]'
          )
          && window.__queueBurstFlag === document.querySelector(
            '[data-queue-individual="alpha"] input[data-queue-flag-burst]'
          )
          && window.__queueMapCanvas === document.querySelector('[data-role=map] canvas')
        )""")

        page.locator(
            'button[data-review-decision="ok"][data-individual="alpha"]'
        ).click()
        page.wait_for_function("() => window.__movementDiagnostics.mapView !== null")
        group_view_before = page.evaluate(
            "() => structuredClone(window.__movementDiagnostics.mapView)"
        )
        page.keyboard.press("ArrowRight")
        page.wait_for_function(
            """() => document.querySelector(
              '.movement-card.queue-active .movement-title'
            )?.textContent === 'beta'""",
            timeout=20_000,
        )
        assert len(review_requests) == 1
        alpha_card = page.locator(".movement-card", has_text="alpha")
        assert "OK" in alpha_card.locator(".movement-review-state").text_content()
        assert "unsaved" not in alpha_card.locator(".movement-review-state").text_content()
        beta_bursts = page.locator(
            '[data-queue-individual="beta"] input[data-queue-burst-visible]'
        )
        beta_bursts.first.wait_for(state="visible", timeout=20_000)
        assert beta_bursts.count() > 0
        assert "No bursts are available" not in page.locator(
            '[data-queue-individual="beta"]'
        ).text_content()
        group_view_after = page.evaluate(
            "() => structuredClone(window.__movementDiagnostics.mapView)"
        )
        assert group_view_after["center"] == pytest.approx(group_view_before["center"])
        assert group_view_after["zoom"] == pytest.approx(group_view_before["zoom"])
        assert page.evaluate("""() => (
          window.__queueIndividualsNode === document.querySelector('[data-role=individuals]')
          && window.__queueAlphaCard === document.querySelector('[data-queue-individual="alpha"]')
          && window.__queueBetaCard === document.querySelector('[data-queue-individual="beta"]')
          && window.__queueMapCanvas === document.querySelector('[data-role=map] canvas')
        )""")

        page.keyboard.press("ArrowLeft")
        page.wait_for_function(
            """() => document.querySelector(
              '.movement-card.queue-active .movement-title'
            )?.textContent === 'alpha'""",
            timeout=20_000,
        )
        page.keyboard.press("ArrowRight")
        page.wait_for_function(
            """() => document.querySelector(
              '.movement-card.queue-active .movement-title'
            )?.textContent === 'beta'""",
            timeout=20_000,
        )
        assert len(review_requests) == 1

        beta_card = page.locator('[data-queue-individual="beta"]')
        beta_card.locator("button[data-queue-comment]").click()
        comment_input = beta_card.locator("input[data-queue-comment-input]")
        comment_input.wait_for(state="visible", timeout=20_000)
        comment_input.focus()
        page.keyboard.press("ArrowLeft")
        page.keyboard.press("1")
        assert page.locator(
            ".movement-card.queue-active .movement-title"
        ).text_content() == "beta"
        assert len(review_requests) == 1
        assert comment_input.input_value() == "1"
        beta_card.locator("button[data-queue-comment]").click()

        page.keyboard.press("4")
        assert beta_card.locator("input[data-review-needs-check]").is_checked()
        page.keyboard.press("4")
        assert not beta_card.locator("input[data-review-needs-check]").is_checked()
        page.keyboard.press("4")
        assert beta_card.locator("input[data-review-needs-check]").is_checked()
        page.keyboard.press("2")
        assert "Fix & Keep" in beta_card.locator(
            ".movement-review-state"
        ).text_content()
        page.locator('button[data-queue-nav="next-individual"]').wait_for(
            state="visible", timeout=20_000
        )
        page.wait_for_function(
            """() => !document.querySelector(
              'button[data-queue-nav="next-individual"]'
            )?.disabled""",
            timeout=20_000,
        )
        page.locator('button[data-queue-nav="next-individual"]').click()
        page.wait_for_function(
            """() => document.querySelector(
              '.movement-card.queue-active .movement-title'
            )?.textContent === 'delta'""",
            timeout=20_000,
        )
        assert len(review_requests) == 2
        delta_bursts = page.locator(
            '[data-queue-individual="delta"] input[data-queue-burst-visible]'
        )
        delta_bursts.first.wait_for(state="visible", timeout=20_000)
        assert delta_bursts.count() > 0
        assert "No bursts are available" not in page.locator(
            '[data-queue-individual="delta"]'
        ).text_content()

        page.route(
            "**/actions/review-individual",
            lambda route: route.fulfill(
                status=500,
                content_type="application/json",
                body='{"error":"forced review save failure"}',
            ),
        )
        page.keyboard.press("3")
        assert "Remove" in page.locator(
            '[data-queue-individual="delta"] .movement-review-state'
        ).text_content()
        page.locator(".movement-card .movement-title", has_text="epsilon").click()
        page.wait_for_function(
            """() => document.querySelector('[data-role=status]')?.textContent.includes(
              'Could not save the individual review decision'
            )""",
            timeout=20_000,
        )
        assert page.locator(
            ".movement-card.queue-active .movement-title"
        ).text_content() == "delta"
        assert "unsaved" in page.locator(
            ".movement-card.queue-active .movement-review-state"
        ).text_content()
        browser.close()


def test_second_round_prior_ok_order_toggle_does_not_touch_map(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    study_dir = tmp_path / "data" / "movement_clean" / "second_round_queue"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(
        CSV_QUEUE_TRANSITION_FIXTURE,
        encoding="utf-8",
    )
    app = create_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )
    register_movement_routes(
        app,
        data_root=tmp_path / "data",
        overview_fix_limit=1,
        overview_series_points=250,
    )

    from fastapi.testclient import TestClient

    setup = TestClient(app)
    assert setup.post(
        "/api/auth/login",
        json={"username": "browser-reviewer", "password": "test-password-long"},
    ).status_code == 200
    loaded = setup.get(
        "/api/apps/movement/family/movement_clean/study/second_round_queue/load"
    ).json()
    reviewer = setup.get("/api/apps/movement/reviewers").json()["reviewers"][0]
    assigned = setup.post(
        "/api/apps/movement/family/movement_clean/study/second_round_queue/review/assign",
        json={
            "reviewer_user_id": reviewer["user_id"],
            "logical_name": "movement.csv",
            "expected_current_dataset_id": loaded["dataset_id"],
            "expected_review_revision": loaded["edit_profile"]["review_revision"],
        },
    ).json()
    dataset_id = loaded["dataset_id"]
    decisions = {
        "alpha": "ok",
        "beta": "fix_keep",
        "gamma": "ok",
        "delta": "remove",
        "epsilon": "ok",
        "zeta": "fix_keep",
    }
    for individual, decision in decisions.items():
        response = setup.post(
            "/api/apps/movement/family/movement_clean/study/second_round_queue/actions/review-individual",
            json={
                "dataset_id": dataset_id,
                "expected_current_dataset_id": dataset_id,
                "expected_review_revision": assigned["state"]["revision"],
                "logical_name": "movement.csv",
                "decision": {
                    "individual": individual,
                    "review_decision": decision,
                    "needs_check": False,
                    "comment": "",
                },
            },
        )
        assert response.status_code == 200, response.text
        dataset_id = response.json()["dataset"]["dataset_id"]
    profile = setup.get(
        "/api/apps/movement/family/movement_clean/study/second_round_queue/edit-profile",
        params={"dataset_id": dataset_id},
    ).json()
    completed = setup.post(
        "/api/apps/movement/family/movement_clean/study/second_round_queue/review/complete",
        json={
            "expected_current_dataset_id": dataset_id,
            "expected_review_revision": profile["review_revision"],
        },
    ).json()
    second = setup.post(
        "/api/apps/movement/family/movement_clean/study/second_round_queue/review/assign",
        json={
            "reviewer_user_id": reviewer["user_id"],
            "logical_name": "movement.csv",
            "expected_current_dataset_id": dataset_id,
            "expected_review_revision": completed["state"]["revision"],
        },
    )
    assert second.status_code == 200, second.text
    setup.close()

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        _login_and_wait(page, base_url, "second_round_queue")
        page.locator('[data-individual-checkbox="alpha"]').wait_for(
            state="attached", timeout=20_000
        )
        page.locator('[data-role="individual-view-queue"]').click()
        toggle = page.locator('[data-role="individual-queue-prior-ok-last"]')
        toggle.wait_for(state="visible", timeout=20_000)
        assert toggle.is_checked()
        assert page.locator(".movement-card .movement-title").first.text_content() == "beta"
        _wait_for_layer(page, "movement-binary-paths-individual-")
        page.wait_for_function("""() => (
          !window.__movementDiagnostics.renderedLayerIds.some(
            id => id.startsWith('movement-overview-preview-')
          )
          && window.__movementDiagnostics.renderedLayerIds.includes(
            'movement-track-player-position'
          )
        )""")
        before = page.evaluate("""() => ({
          canvas: document.querySelector('[data-role=map] canvas'),
          map: document.querySelector('[data-role=map]'),
          mapView: structuredClone(window.__movementDiagnostics.mapView),
          layerIds: [...window.__movementDiagnostics.renderedLayerIds],
          binaryRequests: window.__movementDiagnostics.binaryRequests,
          active: document.querySelector('.movement-card.queue-active .movement-title')?.textContent,
        })""")
        page.evaluate("""() => {
          window.__secondRoundCanvas = document.querySelector('[data-role=map] canvas');
          window.__secondRoundMap = document.querySelector('[data-role=map]');
        }""")
        toggle.uncheck()
        assert page.locator(".movement-card .movement-title").first.text_content() == "alpha"
        after = page.evaluate("""() => ({
          sameCanvas: window.__secondRoundCanvas === document.querySelector('[data-role=map] canvas'),
          sameMap: window.__secondRoundMap === document.querySelector('[data-role=map]'),
          mapView: structuredClone(window.__movementDiagnostics.mapView),
          layerIds: [...window.__movementDiagnostics.renderedLayerIds],
          binaryRequests: window.__movementDiagnostics.binaryRequests,
          active: document.querySelector('.movement-card.queue-active .movement-title')?.textContent,
        })""")
        assert after["sameCanvas"] is True
        assert after["sameMap"] is True
        assert after["mapView"] == before["mapView"]
        assert after["layerIds"] == before["layerIds"]
        assert after["binaryRequests"] == before["binaryRequests"]
        assert after["active"] == before["active"] == "beta"
        browser.close()


def test_admin_dashboard_refreshes_active_review_without_rebuilding_rows(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    study_dir = tmp_path / "data" / "movement_clean" / "dashboard_live"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(CSV_BROWSER_FIXTURE, encoding="utf-8")
    app = create_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )
    register_movement_routes(
        app,
        data_root=tmp_path / "data",
        overview_fix_limit=1,
        overview_series_points=250,
    )

    summaries = [
        {
            "studies": [{
                "family": "movement_clean",
                "study": "dashboard_live",
                "current_dataset_id": "dataset_one",
                "review": {
                    "review_id": "review_live",
                    "status": "active",
                    "reviewer": {"display_name": "Working Reviewer"},
                },
                "counts": {
                    "required": 3, "reviewed": 0, "undecided": 3,
                    "ok": 0, "fix_keep": 0, "remove": 0, "needs_check": 0,
                },
            }],
        },
        {
            "studies": [{
                "family": "movement_clean",
                "study": "dashboard_live",
                "current_dataset_id": "dataset_two",
                "review": {
                    "review_id": "review_live",
                    "status": "active",
                    "reviewer": {"display_name": "Working Reviewer"},
                },
                "counts": {
                    "required": 3, "reviewed": 1, "undecided": 2,
                    "ok": 1, "fix_keep": 0, "remove": 0, "needs_check": 0,
                },
            }],
        },
    ]
    request_count = 0

    def dashboard_route(route):
        nonlocal request_count
        payload = summaries[min(request_count, len(summaries) - 1)]
        request_count += 1
        route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.route("**/api/apps/movement/admin/review-summary", dashboard_route)
        _login_and_wait(page, base_url, "dashboard_live")
        page.locator('[data-role="admin-dashboard"]').click()
        progress = page.locator('[data-admin-field="progress"]')
        progress.wait_for(state="visible", timeout=20_000)
        assert progress.text_content() == "0/3"
        page.evaluate("""() => {
          window.__adminDashboardRow = document.querySelector('tr[data-admin-study-row]');
        }""")

        page.locator('[data-role="admin-dashboard-refresh"]').click()
        page.wait_for_function(
            """() => document.querySelector('[data-admin-field="progress"]')?.textContent === '1/3'""",
            timeout=20_000,
        )
        assert page.locator('[data-admin-field="review"]').text_content() == "active"
        assert page.locator('[data-admin-field="ok"]').text_content() == "1"
        assert page.locator('[data-admin-field="undecided"]').count() == 0
        assert page.evaluate("""() => (
          window.__adminDashboardRow === document.querySelector('tr[data-admin-study-row]')
        )""")
        assert request_count >= 2
        browser.close()


def test_dataset_dropdown_restores_rewound_forward_tip(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    study_dir = tmp_path / "data" / "movement_clean" / "forward_tip"
    study_dir.mkdir(parents=True)
    (study_dir / "movement.csv").write_text(CSV_BROWSER_FIXTURE, encoding="utf-8")
    root_id = ensure_project_state(study_dir)["current_dataset_id"]
    latest_id = create_step(
        study_dir,
        {
            "user": "browser-reviewer",
            "title": "Restorable forward step",
            "kind": "python",
            "script": FORWARD_HEAD_STEP_SCRIPT,
            "parent_dataset_id": root_id,
            "input_artifacts": ["movement.csv"],
            "output_artifacts": ["forward.txt"],
            "parameters": {"value": "forward"},
            "set_as_head": True,
        },
    )["dataset"]["dataset_id"]
    app = create_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )
    register_movement_routes(
        app,
        data_root=tmp_path / "data",
        overview_fix_limit=1,
        overview_series_points=250,
    )

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        head_requests = []
        page.on(
            "request",
            lambda request: head_requests.append(request.url)
            if request.url.endswith("/head") else None,
        )
        _login_and_wait(page, base_url, "forward_tip")
        page.locator(f'[data-role="dataset"] option[value="{latest_id}"]').wait_for(
            state="attached", timeout=20_000
        )
        page.locator('[data-role="undo"]').click()
        page.wait_for_function(
            "rootId => document.querySelector('[data-role=dataset]')?.value === rootId",
            arg=root_id,
            timeout=20_000,
        )
        page.locator('[data-role="edit-lock-profile"]').wait_for(
            state="hidden", timeout=20_000
        )
        page.locator('[data-role="resume-history"]').wait_for(
            state="hidden", timeout=20_000
        )

        page.locator('[data-role="dataset"]').select_option(latest_id)
        page.wait_for_function(
            "latestId => document.querySelector('[data-role=dataset]')?.value === latestId",
            arg=latest_id,
            timeout=20_000,
        )
        page.locator('[data-role="edit-lock-profile"]').wait_for(
            state="hidden", timeout=20_000
        )
        assert len(head_requests) == 1
        browser.close()


def test_rds_progressive_loading_keeps_preview_until_exact(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    samples = sorted(RDS_SAMPLE_ROOT.glob("268904527_*.rds"), key=lambda path: path.stat().st_size)
    if len(samples) < 2:
        pytest.skip("RDS movement browser fixtures are unavailable")
    study_dir = tmp_path / "data" / "movement_rds" / "268904527"
    study_dir.mkdir(parents=True)
    outlier_sample = RDS_SAMPLE_ROOT / "268904527_269302895.rds"
    selected_samples = [*samples[:2]]
    if outlier_sample.exists() and outlier_sample not in selected_samples:
        selected_samples.append(outlier_sample)
    for sample in selected_samples:
        shutil.copy2(sample, study_dir / sample.name)
    app = create_rds_movement_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )
    _delay_binary_responses(app)

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        binary_requests = []
        page.on("request", lambda request: binary_requests.append(request.url)
                if "/fixes-binary?" in request.url else None)
        _login_and_wait(page, base_url, "268904527")
        page.wait_for_timeout(1_000)
        assert page.locator("[data-individual-checkbox]").count(), {
            "page_errors": page_errors,
            "status": page.locator('[data-role="status"]').text_content(),
        }
        first = page.locator("[data-individual-checkbox]").first
        individual = first.get_attribute("data-individual-checkbox")
        assert individual
        page.evaluate("""
          window.__movementMapNode = document.querySelector('[data-role=map]');
          window.__movementGeometryGaps = [];
          window.__movementMonitorActive = true;
          const monitorGeometry = () => {
            if (!window.__movementMonitorActive) return;
            const selected = [...document.querySelectorAll('[data-individual-checkbox]')]
              .some(input => input.checked);
            const ids = window.__movementDiagnostics.renderedLayerIds;
            const geometry = ids.some(id => id.startsWith('movement-overview-preview-')
              || id.startsWith('movement-binary-paths-'));
            if (selected && !geometry) window.__movementGeometryGaps.push(performance.now());
            requestAnimationFrame(monitorGeometry);
          };
          requestAnimationFrame(monitorGeometry);
          window.__movementSelectionStart = performance.now();
          document.querySelector('[data-individual-checkbox]').click();
        """)
        page.wait_for_function(
            "individual => window.__movementDiagnostics.renderedLayerIds.includes('movement-overview-preview-0')",
            arg=individual,
        )
        assert page.evaluate(
            "window.__movementDiagnostics.lastPreviewActivationMs - window.__movementSelectionStart"
        ) < 100
        assert page.evaluate("window.__movementMapNode === document.querySelector('[data-role=map]')")
        _wait_for_layer(page, "movement-binary-paths-individual-")
        page.wait_for_function(
            "() => !window.__movementDiagnostics.renderedLayerIds.some(id => id.startsWith('movement-overview-preview-'))"
        )
        assert len(binary_requests) == 1
        assert page.evaluate("window.__movementGeometryGaps.length") == 0

        first.uncheck()
        first.check()
        _wait_for_layer(page, "movement-binary-paths-individual-")
        assert len(binary_requests) == 1

        page.locator('[data-role="color-by"]').select_option("gps_spike_step_turn")
        turn_angle = page.locator(
            'input[data-action="set-gps-spike-turn-angle"]'
        )
        turn_angle.wait_for(state="visible", timeout=20_000)
        turn_angle.fill("0")
        turn_angle.press("Tab")
        threshold_chart = page.locator('[data-role="threshold-chart"]')
        threshold_chart.wait_for(state="visible", timeout=20_000)
        threshold_value = float(threshold_chart.get_attribute("data-min"))
        threshold_input = page.locator(
            'input[data-action="set-threshold-value"]'
        )
        threshold_input.fill(str(threshold_value))
        threshold_input.press("Tab")
        page.wait_for_function(
            "() => window.__movementDiagnostics.binaryThresholdMatchCount > 0",
            timeout=20_000,
        )
        highlighted_count = page.evaluate(
            "window.__movementDiagnostics.binaryThresholdMatchCount"
        )
        page.locator('button[data-action="check-above-threshold"]').click()
        _wait_for_layer(page, "movement-binary-checked-threshold")
        selected_count = page.evaluate(
            "window.__movementDiagnosticsSnapshot().selectedFixCount"
        )
        assert selected_count == min(highlighted_count, 150)

        if outlier_sample.exists():
            outlier_individual = page.locator('[data-individual-checkbox="MF006"]')
            outlier_individual.check()
            page.locator('[data-role="color-by"]').select_option("is_outlier")
            true_level = page.locator(
                'input[data-action="toggle-threshold-level"][data-level="True"]'
            )
            true_level.wait_for(state="attached", timeout=20_000)
            true_level.check()
            flag_button = page.locator('[data-role="mark-suspected"]')
            assert flag_button.is_enabled()
            assert flag_button.text_content() == "Flag thresholded fixes"
            assert not any(
                "movement-binary-threshold" in layer_id
                for layer_id in _layer_ids(page)
            )
            page.locator('button[data-action="check-above-threshold"]').click()
            assert page.evaluate(
                "window.__movementDiagnosticsSnapshot().selectedFixCount"
            ) == 3
            assert flag_button.is_enabled()
            assert flag_button.text_content() == "Flag thresholded fixes"
            assert "checked fixes" not in flag_button.text_content()
            flag_button.click()
            page.locator('[data-role="issue-modal"]').wait_for(state="visible")
            assert page.locator('[data-role="issue-type"]').input_value() == "Filter is_outlier"
            assert page.locator('[data-role="issue-note"]').input_value() == (
                "Filter applied: is_outlier = True."
            )
            assert page.locator('[data-role="issue-question"]').input_value() == ""
            requests_before_flag = len(binary_requests)
            page.locator('[data-role="issue-submit"]').click()
            page.locator('[data-role="issue-modal"]').wait_for(
                state="hidden", timeout=20_000
            )
            page.wait_for_function(
                "() => document.querySelector('[data-role=status]').textContent.includes('Flagged 3 fixes')",
                timeout=20_000,
            )
            assert len(binary_requests) == requests_before_flag
            assert outlier_individual.is_checked()
            _wait_for_layer(page, "movement-binary-suspected")
            layer_ids = _layer_ids(page)
            assert not any("movement-binary-threshold" in layer_id for layer_id in layer_ids)
            assert "movement-suspected-outline" not in layer_ids
            assert sum("movement-binary-suspected" in layer_id for layer_id in layer_ids) == 1
            assert layer_ids[-1].startswith("movement-binary-suspected-")
            page.wait_for_function(
                "() => document.querySelector('[data-role=select-suspicious]').textContent.includes('(3)')"
            )

            page.locator('[data-role="individual-view-queue"]').click()
            queue_outlier = page.locator('[data-queue-individual="MF006"]')
            queue_outlier.wait_for(state="visible", timeout=20_000)
            queue_outlier.click()
            page.wait_for_function(
                "() => document.querySelector('[data-queue-individual=\"MF006\"]')?.classList.contains('queue-active')",
                timeout=20_000,
            )
            page.wait_for_function(
                "() => window.__movementDiagnostics.queueContextColoredCount > 3"
                " && window.__movementDiagnostics.queueContextGrayCount === 0",
                timeout=20_000,
            )
            page.locator('[data-role="select-suspicious"]').click()
            queue_unflag_button = page.locator('[data-role="dismiss-suspected"]')
            page.wait_for_function(
                "() => !document.querySelector('[data-role=dismiss-suspected]').disabled",
                timeout=20_000,
            )
            queue_unflag_button.click()
            page.locator('[data-role="dismiss-modal"]').wait_for(state="visible")
            page.locator('[data-role="dismiss-close"]').click()
            page.locator('[data-role="individual-view-browse"]').click()
            page.locator('[data-individual-checkbox="MF006"]').wait_for(
                state="attached", timeout=20_000
            )

            page.evaluate("window.__suspiciousMapNode = document.querySelector('[data-role=map]')")
            hide_suspicious = page.locator('[data-role="hide-suspected"]')
            hide_suspicious.check()
            page.wait_for_function(
                "() => !window.__movementDiagnostics.renderedLayerIds.some(id => id.includes('movement-binary-suspected'))"
            )
            assert len(binary_requests) == requests_before_flag
            assert page.evaluate(
                "window.__suspiciousMapNode === document.querySelector('[data-role=map]')"
            )
            hide_suspicious.uncheck()
            _wait_for_layer(page, "movement-binary-suspected")
            assert len(binary_requests) == requests_before_flag

            page.locator('[data-role="select-suspicious"]').click()
            unflag_button = page.locator('[data-role="dismiss-suspected"]')
            page.wait_for_function(
                "() => !document.querySelector('[data-role=dismiss-suspected]').disabled",
                timeout=20_000,
            )
            assert unflag_button.text_content() == "Unflag suspicious (3)"
            layer_ids = _layer_ids(page)
            assert layer_ids[-1] == "movement-checked-suspicious-indicator"
            unflag_button.click()
            page.locator('[data-role="dismiss-modal"]').wait_for(state="visible")
            page.locator('[data-role="dismiss-submit"]').click()
            page.locator('[data-role="dismiss-modal"]').wait_for(
                state="hidden", timeout=20_000
            )
            page.wait_for_function(
                "() => document.querySelector('[data-role=status]').textContent.includes('Unflagged selected suspicions')",
                timeout=20_000,
            )
            assert not any(
                "movement-binary-suspected" in layer_id for layer_id in _layer_ids(page)
            )
            assert len(binary_requests) == requests_before_flag

            page.locator('[data-role="undo"]').click()
            page.wait_for_function(
                "() => document.querySelector('[data-role=status]').textContent.startsWith('Undid to')",
                timeout=20_000,
            )
            _wait_for_layer(page, "movement-binary-suspected")
            page.locator('[data-role="undo"]').click()
            page.wait_for_function(
                "() => document.querySelector('[data-role=status]').textContent.startsWith('Undid to')",
                timeout=20_000,
            )
            assert len(binary_requests) == requests_before_flag
            assert outlier_individual.is_checked()
            assert not any(
                "movement-binary-suspected" in layer_id for layer_id in _layer_ids(page)
            )

        page.evaluate("window.__movementMonitorActive = false")
        browser.close()


def test_rds_whole_study_filter_updates_hidden_retained_individuals(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    samples = [
        RDS_SAMPLE_ROOT / "268904527_269302895.rds",  # MF006: 3 source outliers
        RDS_SAMPLE_ROOT / "268904527_269302904.rds",  # MF011: 23 source outliers
    ]
    if not all(sample.exists() for sample in samples):
        pytest.skip("RDS whole-study browser fixtures are unavailable")
    study_dir = tmp_path / "data" / "movement_rds" / "268904527"
    study_dir.mkdir(parents=True)
    for sample in samples:
        shutil.copy2(sample, study_dir / sample.name)
    app = create_rds_movement_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        projection_requests = []
        page.on(
            "request",
            lambda request: projection_requests.append(request.url)
            if "/review-projection?" in request.url else None,
        )
        _login_and_wait(page, base_url, "268904527")
        page.wait_for_timeout(500)

        checkbox_order = page.locator("[data-individual-checkbox]").evaluate_all(
            "inputs => inputs.map(input => input.dataset.individualCheckbox)"
        )
        mf006_index = checkbox_order.index("MF006")
        mf011_index = checkbox_order.index("MF011")
        mf006 = page.locator('[data-individual-checkbox="MF006"]')
        mf011 = page.locator('[data-individual-checkbox="MF011"]')

        # Retain MF011's exact block in browser memory, then hide it before the
        # whole-study mutation. This is the stale-cache case reported by users.
        mf011.check()
        _wait_for_layer(page, f"movement-binary-paths-individual-{mf011_index}")
        mf011.uncheck()
        mf006.check()
        _wait_for_layer(page, f"movement-binary-paths-individual-{mf006_index}")
        page.locator('[data-role="color-by"]').select_option("is_outlier")
        true_level = page.locator(
            'input[data-action="toggle-threshold-level"][data-level="True"]'
        )
        true_level.wait_for(state="visible", timeout=20_000)
        true_level.check()
        page.locator('button[data-action="check-above-threshold"]').click()
        page.locator('[data-role="mark-suspected"]').click()
        page.locator('[data-role="issue-modal"]').wait_for(state="visible")
        issue_meta = page.locator('[data-role="issue-meta"]').text_content()
        assert "Exact fixes to flag: 26" in issue_meta
        assert "all matching fixes in the whole study" in issue_meta
        page.locator('[data-role="issue-submit"]').click()
        page.locator('[data-role="issue-modal"]').wait_for(
            state="hidden", timeout=20_000
        )
        page.wait_for_function(
            "() => document.querySelector('[data-role=status]').textContent.includes('Flagged 26 fixes')",
            timeout=20_000,
        )

        assert not mf011.is_checked()
        assert "MF011" in projection_requests[-1], projection_requests
        mf006.uncheck()
        mf011.check()
        page.wait_for_function(
            "layerId => window.__movementDiagnostics.renderedLayerIds.includes(layerId)",
            arg=f"movement-binary-suspected-individual-{mf011_index}",
            timeout=20_000,
        )
        page.wait_for_function(
            "() => document.querySelector('[data-role=select-suspicious]').textContent.includes('(26)')",
            timeout=20_000,
        )
        browser.close()


def test_rds_queue_navigation_does_not_fan_out_attribute_renders(tmp_path):
    playwright_api = pytest.importorskip("playwright.sync_api")
    samples = sorted(
        RDS_SAMPLE_ROOT.glob("268904527_*.rds"),
        key=lambda path: path.stat().st_size,
    )[:15]
    if len(samples) < 15:
        pytest.skip("Fifteen RDS movement browser fixtures are unavailable")
    study_dir = tmp_path / "data" / "movement_rds" / "268904527"
    study_dir.mkdir(parents=True)
    for sample in samples:
        shutil.copy2(sample, study_dir / sample.name)
    app = create_rds_movement_app(
        data_root=tmp_path / "data",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        _login_and_wait(page, base_url, "268904527")
        page.locator("[data-individual-checkbox]").first.wait_for(
            state="attached", timeout=30_000
        )
        page.locator('[data-role="individual-view-queue"]').click()
        page.locator("[data-queue-individual].queue-active").wait_for(
            state="attached", timeout=20_000
        )
        page.wait_for_function(
            "() => window.__movementDiagnosticsSnapshot().focusedObjectEntries >= 1",
            timeout=30_000,
        )

        for count in range(2, 16):
            previous = page.locator(
                "[data-queue-individual].queue-active"
            ).get_attribute("data-queue-individual")
            page.keyboard.press("ArrowRight")
            page.wait_for_function(
                "previous => document.querySelector('[data-queue-individual].queue-active')?.dataset.queueIndividual !== previous",
                arg=previous,
                timeout=30_000,
            )
            page.wait_for_function(
                "count => window.__movementDiagnosticsSnapshot().focusedObjectEntries >= count",
                arg=count,
                timeout=30_000,
            )

        page.wait_for_timeout(100)
        snapshot = page.evaluate("window.__movementDiagnosticsSnapshot()")
        assert snapshot["focusedObjectEntries"] == 15
        assert snapshot["binaryBlockCount"] == 15
        assert snapshot["binaryRequests"] == 15
        assert snapshot["jsonDetailRequests"] == 15
        assert snapshot["binaryAttributeBuilds"] <= 30
        assert snapshot["binaryAttributeRenderSubscriptions"] <= 15
        assert snapshot["renderCacheEntries"] <= 30
        assert snapshot["renderCalls"] < 200
        browser.close()
