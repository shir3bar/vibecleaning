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
from app.execution import create_analysis, create_step
from app.state import ensure_project_state
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

        page.locator('[data-role="individual-view-queue"]').click()
        entire_individual = page.locator('button[data-queue-flag-individual]').first
        entire_individual.wait_for(state="visible", timeout=20_000)
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
        assert page.locator('[data-role="individual-queue-order"]').input_value() == (
            "isolation_forest_decision_margin"
        )
        page.locator('[data-role="individual-view-queue"]').click()
        page.locator("[data-queue-ranking-score]").first.wait_for(
            state="visible", timeout=20_000
        )
        assert page.locator("[data-queue-ranking-score]").first.text_content() == (
            "#1 · score 0.75"
        )

        page.evaluate("window.__movementMonitorActive = false")
        browser.close()


def test_queue_decision_reuses_inflight_exact_blocks_across_review_step(tmp_path):
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
        page.locator(
            'button[data-review-decision="ok"][data-individual="alpha"]'
        ).click()
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
        assert len(binary_requests) == 1
        assert f"/dataset/{initial_dataset_id}/" in binary_requests[0]
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

        page.locator(
            'button[data-review-decision="ok"][data-individual="alpha"]'
        ).click()
        page.locator(".movement-card .movement-title", has_text="beta").click()
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
        assert page.evaluate("""() => (
          window.__queueIndividualsNode === document.querySelector('[data-role=individuals]')
          && window.__queueAlphaCard === document.querySelector('[data-queue-individual="alpha"]')
          && window.__queueBetaCard === document.querySelector('[data-queue-individual="beta"]')
          && window.__queueMapCanvas === document.querySelector('[data-role=map] canvas')
        )""")

        page.locator(
            'button[data-review-decision="fix_keep"][data-individual="beta"]'
        ).click()
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

        page.route(
            "**/actions/review-individual",
            lambda route: route.fulfill(
                status=500,
                content_type="application/json",
                body='{"error":"forced review save failure"}',
            ),
        )
        page.locator(
            'button[data-review-decision="ok"][data-individual="delta"]'
        ).click()
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
            state="visible", timeout=20_000
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
            layer_ids = _layer_ids(page)
            assert not any("movement-binary-threshold" in layer_id for layer_id in layer_ids)
            assert "movement-suspected-outline" not in layer_ids
            assert sum("movement-binary-suspected" in layer_id for layer_id in layer_ids) == 1
            page.wait_for_function(
                "() => document.querySelector('[data-role=select-suspicious]').textContent.includes('(3)')"
            )

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
