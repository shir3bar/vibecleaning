from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import asyncio
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
from app.web import create_app
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
        page.evaluate("window.__movementMonitorActive = false")
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
            page.locator('button[data-action="check-above-threshold"]').click()
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

        page.evaluate("window.__movementMonitorActive = false")
        browser.close()
