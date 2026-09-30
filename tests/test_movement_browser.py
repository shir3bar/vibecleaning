from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import asyncio
import base64
import json
import logging
import os
import re
import shutil
import socket
import sys
import threading
import time

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.auth import AuthManager
from app.execution import create_analysis, create_step
from app.server import create_server
from app.state import ensure_project_state
from app.web import create_app
from examples.movement.analysis_history import BURST_FEATURE_SIGNATURE
from examples.movement.routes import register_movement_routes
from examples.rds_movement.app import create_rds_movement_app
from examples.slim_movement.app import create_slim_movement_app


STATIC_ROOT = REPO_ROOT / "examples" / "movement" / "static"
INDEX_PATH = STATIC_ROOT / "index.html"
RDS_SAMPLE_ROOT = REPO_ROOT / "data" / "movement_rds"

pytestmark = pytest.mark.browser
LOGGER = logging.getLogger(__name__)
BASEMAP_REQUEST = re.compile(
    r"https://(?:tile\.openstreetmap\.org|basemaps\.cartocdn\.com|"
    r"services\.arcgisonline\.com|tile\.opentopomap\.org)/"
)
TRANSPARENT_TILE = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
    "+A8AAQUBAScY42YAAAAASUVORK5CYII="
)

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
    server = create_server(
        app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started and thread.is_alive() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not server.started:
            raise RuntimeError("Browser test server did not start")
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        assert not thread.is_alive(), "Browser test server did not shut down"


def _open_browser(playwright):
    try:
        return playwright.chromium.launch(
            headless=True,
            # CI and VMs need a working WebGL context without a physical GPU.
            args=["--use-angle=swiftshader", "--enable-unsafe-swiftshader"],
        )
    except Exception as exc:  # pragma: no cover - depends on local browser install
        pytest.fail(
            "Chromium could not start. Run `uv run playwright install chromium` "
            "(Linux also needs `uv run playwright install-deps chromium`). "
            f"Browser error: {exc}",
            pytrace=False,
        )


def _new_page(browser, **kwargs):
    page = browser.new_page(**kwargs)
    # Keep real MapLibre/Deck rendering and local app requests. These tests do
    # not check third-party map imagery or depend on a provider being online.
    def basemap_response(route):
        if route.request.url.endswith(".json"):
            route.fulfill(json={"version": 8, "sources": {}, "layers": []})
        else:
            route.fulfill(content_type="image/png", body=TRANSPARENT_TILE)

    page.route(BASEMAP_REQUEST, basemap_response)
    # Pytest includes these logs with failures, including startup timeouts.
    page.on("pageerror", lambda error: LOGGER.error("Browser JavaScript: %s", error))
    page.on("console", lambda message: LOGGER.error("Browser console: %s", message.text)
            if message.type == "error" else None)
    page.on("requestfailed", lambda request: LOGGER.error(
        "Browser request failed: %s %s (%s)", request.method, request.url, request.failure
    ))
    page.on("response", lambda response: LOGGER.error(
        "Browser HTTP %s: %s", response.status, response.url
    ) if response.status >= 400 else None)
    return page


def _login_and_wait(page, base_url: str, study_name: str) -> None:
    page.goto(base_url, wait_until="domcontentloaded")
    page.locator("#login-username").fill("browser-reviewer")
    page.locator("#login-password").fill("test-password-long")
    page.locator("#login-submit").click()
    # Readiness is a functional condition, not a startup performance budget.
    page.locator(".movement-root").wait_for(state="visible", timeout=60_000)
    page.locator('[data-role="map"] canvas').first.wait_for(state="attached", timeout=60_000)
    page.wait_for_function("window.__movementDiagnostics !== undefined", timeout=60_000)
    page.locator(f'[data-role="study"] option[value="{study_name}"]').wait_for(
        state="attached", timeout=60_000
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


def _open_binary_fix_popup(page, individual, source_row):
    # Coincident fixes cannot be reliably distinguished by screen coordinates.
    # Choose a real binary fix, then exercise the actual map context-menu event.
    target = page.evaluate("""({individual, sourceRow}) => {
      for (const layer of window.__testMapLayers || []) {
        if (!layer.id.startsWith('movement-binary-points-') || !layer.props.visible) continue;
        const binary = layer.props.userData.binaryBlock;
        const arrays = binary.arrays;
        const index = arrays.source_rows.findIndex((row, i) => row === sourceRow
          && binary.header.individuals[arrays.individual_codes[i]] === individual);
        if (index < 0) continue;
        const original = deck.MapboxOverlay.prototype.pickObject;
        deck.MapboxOverlay.prototype.pickObject = function() {
          deck.MapboxOverlay.prototype.pickObject = original;
          return {layer, index: index - (layer.props.userData.binaryPointOffset || 0)};
        };
        return {timeMs: arrays.time_ms[index], sourceRow};
      }
      throw new Error('Requested fix was not loaded');
    }""", {"individual": individual, "sourceRow": source_row})
    page.locator('[data-role="map"] canvas').first.click(button="right", position={"x": 250, "y": 150})
    page.locator('[data-role="fix-popup-jump"]').wait_for(state="visible")
    return target


def _record_preview_latency(page, record_property):
    latency = page.evaluate(
        "window.__movementDiagnostics.lastPreviewActivationMs - window.__movementSelectionStart"
    )
    record_property("preview_activation_ms", latency)
    assert latency >= 0
    # Functional tests verify preview continuity below. Performance is measured
    # separately; opt into a budget on known hardware, rather than timing a VM.
    budget = os.environ.get("VIBECLEANING_PREVIEW_BUDGET_MS")
    if budget:
        assert latency < float(budget), f"Preview activation took {latency:.1f} ms"


@pytest.mark.parametrize("source_format", ["csv", "rds"])
@pytest.mark.parametrize("starting_view", ["browse", "queue"])
def test_study_switch_resets_threshold_but_retains_color_field(tmp_path, source_format, starting_view):
    import playwright.sync_api as playwright_api
    data_root = tmp_path / "data"
    if source_format == "rds":
        study_sources = [
            ("first_study", "268904527_269302895.rds"),
            ("second_study", "268904527_269302904.rds"),
        ]
        for study_name, filename in study_sources:
            sample = RDS_SAMPLE_ROOT / filename
            if not sample.exists():
                pytest.fail("RDS movement browser fixtures are unavailable")
            study_dir = data_root / "movement_rds" / study_name
            study_dir.mkdir(parents=True)
            shutil.copy2(sample, study_dir / filename)
        app = create_rds_movement_app(
            data_root=data_root, cache_root=tmp_path / "cache",
            static_root=STATIC_ROOT, index_path=INDEX_PATH,
            auth_manager=_auth_manager(),
        )
        first_individual, second_individual = "MF006", "MF011"
    else:
        for study_name, prefix in [("first_study", ""), ("second_study", "new-")]:
            study_dir = data_root / "movement_raw" / study_name
            study_dir.mkdir(parents=True)
            content = CSV_BROWSER_FIXTURE
            for individual in ["alpha", "beta", "gamma"]:
                content = content.replace(individual, prefix + individual)
            if prefix:
                for old, new in [("-70.", "10."), ("-71.", "11."), ("-72.", "12.")]:
                    content = content.replace(old, new)
            (study_dir / "movement.csv").write_text(content, encoding="utf-8")
        app = create_slim_movement_app(
            data_root=data_root, static_root=STATIC_ROOT, index_path=INDEX_PATH,
            auth_manager=_auth_manager(),
        )
        first_individual, second_individual = "alpha", "new-alpha"

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
        page_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        _login_and_wait(page, base_url, "first_study")
        first = page.locator(f'[data-individual-checkbox="{first_individual}"]')
        first.wait_for(state="visible", timeout=30_000)
        page.wait_for_function("() => window.__movementDiagnosticsSnapshot().mapReady")
        first.check()
        _wait_for_layer(page, "movement-binary-paths-")
        page.locator('[data-role="color-by"]').select_option("speed_mps")
        cutoff = page.locator('input[data-action="set-threshold-value"]')
        cutoff.fill("12.5")
        cutoff.press("Tab")
        page.locator('input[data-action="toggle-threshold-reverse"]').check()

        if starting_view == "queue":
            browse_view = page.evaluate("window.__movementDiagnosticsSnapshot().mapView")
            page.locator('[data-role="individual-view-queue"]').click()
            page.wait_for_function("() => window.__movementDiagnosticsSnapshot().trackPlayerFixCount > 0")
            page.locator('.maplibregl-ctrl-zoom-in').click()
            page.wait_for_function(
                "zoom => window.__movementDiagnosticsSnapshot().mapView.zoom > zoom + 0.9",
                arg=browse_view["zoom"],
            )
            queue_view = page.evaluate("window.__movementDiagnosticsSnapshot().mapView")
            # Populate both saved views, then leave the first study in its queue.
            page.locator('[data-role="individual-view-browse"]').click()
            page.wait_for_function(
                "zoom => Math.abs(window.__movementDiagnosticsSnapshot().mapView.zoom - zoom) < 1e-6",
                arg=browse_view["zoom"],
            )
            page.locator('[data-role="individual-view-queue"]').click()
            page.wait_for_function(
                "zoom => Math.abs(window.__movementDiagnosticsSnapshot().mapView.zoom - zoom) < 1e-6",
                arg=queue_view["zoom"],
            )

        with page.expect_response(lambda response: "/study/second_study/" in response.url
                                  and "/overview?" in response.url) as loaded:
            page.locator('[data-role="study"]').select_option("second_study")
        expected_view = loaded.value.json()["initial_view"]
        page.wait_for_function("""() => document.querySelector(
          '[data-role="individual-view-browse"]'
        ).classList.contains('is-active')""")
        second = page.locator(f'[data-individual-checkbox="{second_individual}"]')
        second.wait_for(state="visible", timeout=30_000)
        page.wait_for_function("() => window.__movementDiagnosticsSnapshot().mapReady")
        second.check()
        _wait_for_layer(page, "movement-binary-paths-")
        cutoff.wait_for(state="visible", timeout=30_000)
        assert page.locator('[data-role="color-by"]').input_value() == "speed_mps"
        assert cutoff.input_value() == ""
        assert not page.locator('input[data-action="toggle-threshold-reverse"]').is_checked()
        assert page.locator('button[data-action="check-above-threshold"]').is_disabled()
        # The new study must not inherit either map position from the old study.
        for view_mode in ["browse", "queue", "browse"]:
            page.locator(f'[data-role="individual-view-{view_mode}"]').click()
            if view_mode == "queue":
                page.wait_for_function("() => window.__movementDiagnosticsSnapshot().trackPlayerFixCount > 0")
            else:
                second.wait_for(state="visible")
            view = page.evaluate("window.__movementDiagnosticsSnapshot().mapView")
            assert view["center"] == pytest.approx([
                expected_view["longitude"], expected_view["latitude"],
            ])
            assert view["zoom"] == pytest.approx(expected_view["zoom"])
        assert not page_errors
        browser.close()


def test_csv_progressive_loading_preserves_dom_and_warm_blocks(tmp_path, record_property):
    import playwright.sync_api as playwright_api
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
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
        page_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        binary_requests = []
        page.on("request", lambda request: binary_requests.append(request.url)
                if "/fixes-binary?" in request.url else None)
        _login_and_wait(page, base_url, "browser_study")
        page.locator("[data-individual-checkbox]").first.wait_for(state="visible", timeout=30_000)
        page.wait_for_function("() => window.__movementDiagnosticsSnapshot().mapReady", timeout=30_000)
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
        _record_preview_latency(page, record_property)
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
    import playwright.sync_api as playwright_api
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
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
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
        page.evaluate("""() => {
          const original = deck.MapboxOverlay.prototype.setProps;
          deck.MapboxOverlay.prototype.setProps = function(props) {
            if (props.layers) window.__testMapLayers = props.layers;
            return original.call(this, props);
          };
        }""")
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

        # Jump to the second of two coincident, equal-time fixes by source row.
        slider.evaluate("element => { element.value = '0'; element.dispatchEvent(new Event('input', {bubbles: true})); }")
        page.locator('[data-role="track-player-hide"]').click()
        target = _open_binary_fix_popup(page, "alpha", 5)
        requests_before_jump = len(binary_requests)
        page.locator('[data-role="fix-popup-jump"]').click()
        player.wait_for(state="visible")
        assert canvas.get_attribute("data-index") == "2"
        assert int(canvas.get_attribute("data-source-row")) == 5
        assert int(canvas.get_attribute("data-time-ms")) == target["timeMs"]
        assert len(binary_requests) == requests_before_jump
        assert page.locator('[data-role="fix-popup"]').is_hidden()
        assert page.evaluate("window.__movementDiagnosticsSnapshot().mapView") == map_view_after

        # From Browse all, jump opens the correct individual's review player.
        page.locator('[data-role="individual-view-browse"]').click()
        page.locator('[data-role="select-all"]').click()
        _open_binary_fix_popup(page, "beta", 26)
        page.locator('[data-role="fix-popup-jump"]').click()
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
    import playwright.sync_api as playwright_api
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
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
        _login_and_wait(page, base_url, "roi_ui")
        page.locator("[data-individual-checkbox]").first.wait_for(
            state="attached", timeout=20_000
        )
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
    import playwright.sync_api as playwright_api
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
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
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
    import playwright.sync_api as playwright_api
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
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
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
        burst_menu = active_card.locator('[data-queue-bursts]')
        assert not burst_menu.evaluate("element => element.open")
        burst_menu.locator('summary').click()
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
        page.locator('[data-queue-individual="beta"] [data-queue-bursts] summary').click()
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
        page.locator('[data-queue-individual="delta"] [data-queue-bursts] summary').click()
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
    import playwright.sync_api as playwright_api
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
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
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
    import playwright.sync_api as playwright_api
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
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
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
    import playwright.sync_api as playwright_api
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
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
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


@pytest.mark.parametrize("fail_export", [False, True])
def test_rds_export_shows_live_progress_and_completion(tmp_path, monkeypatch, fail_export):
    import playwright.sync_api as playwright_api
    import app.execution as execution
    from app.filesystem import atomic_write_json

    samples = sorted(RDS_SAMPLE_ROOT.glob("268904527_*.rds"), key=lambda path: path.stat().st_size)[:2]
    assert len(samples) == 2
    study_dir = tmp_path / "data" / "movement_rds" / "268904527"
    study_dir.mkdir(parents=True)
    for sample in samples:
        shutil.copy2(sample, study_dir / sample.name)

    # Hold the real export subprocess at its start so each progress state is
    # observable, without timing assertions that depend on machine speed.
    release = threading.Event()
    progress_paths = []
    original_run = execution.run_python_script

    def held_export(script_path, spec_path, summary_path):
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        if spec.get("analysis", {}).get("parameters", {}).get("action") == "export_reviewed_rds":
            progress_path = spec_path.with_name("progress.json")
            atomic_write_json(progress_path, {
                "stage": "preparing", "completed_files": 0, "total_files": 2,
                "logical_name": samples[0].name,
            })
            progress_paths.append(progress_path)
            if not release.wait(30):
                raise RuntimeError("Test did not release export")
            if fail_export:
                raise RuntimeError("Original data check failed")
        return original_run(script_path, spec_path, summary_path)

    monkeypatch.setattr(execution, "run_python_script", held_export)
    monkeypatch.setenv("VIBECLEANING_RDS_WRITER", "python")
    app = create_rds_movement_app(
        data_root=tmp_path / "data", cache_root=tmp_path / "cache",
        static_root=STATIC_ROOT, index_path=INDEX_PATH, auth_manager=_auth_manager(),
    )
    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
        try:
            _login_and_wait(page, base_url, "268904527")
            button = page.locator('[data-role="export-reviewed-csv"]')
            playwright_api.expect(button).to_be_enabled(timeout=30_000)
            with page.expect_response(lambda response: response.url.endswith("/actions/export-reviewed-rds")) as started:
                button.click()
            assert started.value.status == 202
            text = page.locator('[data-role="export-progress-text"]')
            bar = page.locator('[data-role="export-progress-bar"]')
            playwright_api.expect(text).to_contain_text("Preparing — file 1 of 2")
            playwright_api.expect(button).to_be_disabled()
            assert progress_paths
            for stage, label in (("writing", "Writing"), ("checking", "Checking original data in"), ("saving", "Saving cleaned files")):
                atomic_write_json(progress_paths[0], {
                    "stage": stage, "completed_files": 1, "total_files": 2,
                    "logical_name": samples[1].name,
                })
                playwright_api.expect(text).to_contain_text(f"{label} — file 2 of 2")
                playwright_api.expect(bar).to_have_attribute("value", "1")
                playwright_api.expect(bar).to_have_attribute("max", "2")
            # Map interaction still works and cannot re-enable a second export.
            first = page.locator("[data-individual-checkbox]").first
            first.check()
            _wait_for_layer(page, "movement-binary-paths-individual-")
            playwright_api.expect(button).to_be_disabled()
            assert page.locator('[data-role="map"]').bounding_box()["height"] > 300
            page.screenshot(path=str(tmp_path / "rds-export-progress.png"))
            release.set()
            if fail_export:
                playwright_api.expect(text).to_contain_text("Original data check failed", timeout=30_000)
                playwright_api.expect(bar).to_be_hidden()
                assert page.locator('[data-role="output-links"] a').count() == 0
            else:
                playwright_api.expect(text).to_contain_text("Saved 2 RDS files to scrubdata/cleaned_files/", timeout=30_000)
                playwright_api.expect(bar).to_have_attribute("value", "1")
                playwright_api.expect(bar).to_have_attribute("max", "1")
                cleaned_dir = study_dir / "scrubdata" / "cleaned_files"
                playwright_api.expect(page.locator('[data-role="output-links"]')).to_contain_text(str(cleaned_dir))
                assert page.locator('[data-role="output-links"] a').count() == 0
                assert sorted(path.name for path in cleaned_dir.glob("*.rds")) == sorted(
                    f"{path.stem}_cleaned.rds" for path in samples)
            playwright_api.expect(button).to_be_enabled()
        finally:
            release.set()
            browser.close()


def test_rds_progressive_loading_keeps_preview_until_exact(tmp_path, record_property):
    import playwright.sync_api as playwright_api
    samples = sorted(RDS_SAMPLE_ROOT.glob("268904527_*.rds"), key=lambda path: path.stat().st_size)
    if len(samples) < 2:
        pytest.fail("RDS movement browser fixtures are unavailable")
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
        cache_root=tmp_path / "cache",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )
    _delay_binary_responses(app)

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
        page_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        binary_requests = []
        page.on("request", lambda request: binary_requests.append(request.url)
                if "/fixes-binary?" in request.url else None)
        _login_and_wait(page, base_url, "268904527")
        page.locator("[data-individual-checkbox]").first.wait_for(state="visible", timeout=30_000)
        page.wait_for_function("() => window.__movementDiagnosticsSnapshot().mapReady", timeout=30_000)
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
        _record_preview_latency(page, record_property)
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
        # The default percentile can already have matches. Wait for the map's
        # asynchronous colors to reflect the newly entered cutoff, not that old count.
        highlighted_count = int(page.locator('.movement-threshold [role="status"]')
                                .inner_text().split()[0].replace(",", ""))
        assert highlighted_count > 0
        page.wait_for_function(
            "expected => window.__movementDiagnostics.binaryThresholdMatchCount === expected",
            arg=highlighted_count, timeout=20_000,
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
            # Target the individual name; the expanded card's center can be a
            # review-decision button, which would leave an unintended draft.
            queue_outlier.locator('.movement-title').click()
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
            show_flagged = page.locator('[data-role="show-flagged"]')
            show_flagged.uncheck()
            page.wait_for_function(
                "() => !window.__movementDiagnostics.renderedLayerIds.some(id => id.includes('movement-binary-suspected'))"
            )
            assert len(binary_requests) == requests_before_flag
            assert page.evaluate(
                "window.__suspiciousMapNode === document.querySelector('[data-role=map]')"
            )
            show_flagged.check()
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
            dismissal_error = page.locator('[data-role="dismiss-status"].error')
            assert not dismissal_error.count(), dismissal_error.text_content()
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


def test_rds_filter_only_updates_visible_individuals(tmp_path):
    import playwright.sync_api as playwright_api
    samples = [
        RDS_SAMPLE_ROOT / "268904527_269302895.rds",  # MF006: 3 source outliers
        RDS_SAMPLE_ROOT / "268904527_269302904.rds",  # MF011: 23 source outliers
    ]
    if not all(sample.exists() for sample in samples):
        pytest.fail("RDS filter browser fixtures are unavailable")
    study_dir = tmp_path / "data" / "movement_rds" / "268904527"
    study_dir.mkdir(parents=True)
    for sample in samples:
        shutil.copy2(sample, study_dir / sample.name)
    app = create_rds_movement_app(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
        _login_and_wait(page, base_url, "268904527")
        page.locator('[data-individual-checkbox="MF006"]').wait_for(state="visible", timeout=30_000)
        page.locator('[data-individual-checkbox="MF011"]').wait_for(state="visible", timeout=30_000)
        page.wait_for_function("() => window.__movementDiagnosticsSnapshot().mapReady", timeout=30_000)

        checkbox_order = page.locator("[data-individual-checkbox]").evaluate_all(
            "inputs => inputs.map(input => input.dataset.individualCheckbox)"
        )
        mf006_index = checkbox_order.index("MF006")
        mf011_index = checkbox_order.index("MF011")
        mf006 = page.locator('[data-individual-checkbox="MF006"]')
        mf011 = page.locator('[data-individual-checkbox="MF011"]')

        # Previously viewed individuals remain cached but must not be flagged
        # after they are hidden from the map.
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
        assert page.locator('[data-action="set-threshold-flag-scope"]').count() == 0
        page.locator('button[data-action="check-above-threshold"]').click()
        page.locator('[data-role="mark-suspected"]').click()
        page.locator('[data-role="issue-modal"]').wait_for(state="visible")
        issue_meta = page.locator('[data-role="issue-meta"]').text_content()
        assert "Exact fixes to flag: 3" in issue_meta
        assert "all matching fixes for 1 visible individual(s)" in issue_meta
        with page.expect_response(lambda response: response.url.endswith("/actions/annotate-scope")) as saved:
            page.locator('[data-role="issue-submit"]').click()
        assert saved.value.status == 200
        step = saved.value.json()["step"]
        assert step["summary"]["resolved_fix_count"] == 3
        assert step["parameters"]["scope"]["filter"]["individuals"] == ["MF006"]
        page.locator('[data-role="issue-modal"]').wait_for(
            state="hidden", timeout=20_000
        )
        page.wait_for_function(
            "() => document.querySelector('[data-role=status]').textContent.includes('Flagged 3 fixes')",
            timeout=20_000,
        )

        assert not mf011.is_checked()
        _wait_for_layer(page, f"movement-binary-suspected-individual-{mf006_index}")
        mf006.uncheck()
        mf011.check()
        _wait_for_layer(page, f"movement-binary-paths-individual-{mf011_index}")
        # MF006's layer is retained with visible=false. MF011 must have no
        # suspected layer of its own when shown again.
        assert f"movement-binary-suspected-individual-{mf011_index}" not in _layer_ids(page)
        page.wait_for_function(
            "() => document.querySelector('[data-role=select-suspicious]').textContent.includes('(3)')",
            timeout=20_000,
        )
        browser.close()


def test_rds_queue_navigation_does_not_fan_out_attribute_renders(tmp_path):
    import playwright.sync_api as playwright_api
    samples = sorted(
        RDS_SAMPLE_ROOT.glob("268904527_*.rds"),
        key=lambda path: path.stat().st_size,
    )[:15]
    if len(samples) < 15:
        pytest.fail("Fifteen RDS movement browser fixtures are unavailable")
    study_dir = tmp_path / "data" / "movement_rds" / "268904527"
    study_dir.mkdir(parents=True)
    for sample in samples:
        shutil.copy2(sample, study_dir / sample.name)
    app = create_rds_movement_app(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        static_root=STATIC_ROOT,
        index_path=INDEX_PATH,
        auth_manager=_auth_manager(),
    )

    with _serve(app) as base_url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 900})
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

        page.wait_for_function(
            "() => window.__movementDiagnosticsSnapshot().binaryBlockCount === 15",
            timeout=30_000,
        )
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
