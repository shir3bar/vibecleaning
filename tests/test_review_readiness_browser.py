"""Exercise the reviewer-facing changes through the real app and saved steps."""
import json
from pathlib import Path
import shutil
import struct

import numpy as np
import pytest

from app.reviews import active_review, load_review_state
from examples.movement.rds_index import read_movement_rds
from examples.slim_movement.app import create_slim_movement_app
from examples.rds_movement.app import create_rds_movement_app
from test_movement_browser import (
    _auth_manager, _serve, _open_browser, _new_page, _login_and_wait,
    _delay_binary_responses, STATIC_ROOT, INDEX_PATH, CSV_TRACK_PLAYER_FIXTURE,
)
from test_rds_movement import _sample_files, KAMI_SAMPLE_ROOT

pytestmark = pytest.mark.browser


def make_app(tmp_path, source_format):
    family = "movement_rds" if source_format == "rds" else "movement_raw"
    study = tmp_path / "data" / family / "readiness"
    study.mkdir(parents=True)
    if source_format == "rds":
        for path in _sample_files():
            shutil.copy2(path, study / path.name)
        app = create_rds_movement_app(data_root=tmp_path / "data", cache_root=tmp_path / "cache",
            static_root=STATIC_ROOT, index_path=INDEX_PATH, auth_manager=_auth_manager())
    else:
        lines = CSV_TRACK_PLAYER_FIXTURE.strip().splitlines()
        (study / "movement.csv").write_text(
            lines[0] + ",is_outlier\n" + "".join(line + ",False\n" for line in lines[1:]), encoding="utf-8")
        app = create_slim_movement_app(data_root=tmp_path / "data", static_root=STATIC_ROOT,
            index_path=INDEX_PATH, auth_manager=_auth_manager())
    return app, study


@pytest.mark.parametrize("levels", [["geometric_spike"], ["geometric_spike", "consensus"]])
def test_rds_error_categories_can_be_flagged_and_reopened(tmp_path, levels):
    import playwright.sync_api as pw
    study = tmp_path / "data" / "movement_rds" / "kami_categories"
    study.mkdir(parents=True)
    sources = sorted(KAMI_SAMPLE_ROOT.glob("*_KAMI.rds"))[:2]
    assert len(sources) == 2, "KAMI detector-score RDS samples are unavailable"
    for source in sources:
        shutil.copy2(source, study / source.name.replace("_KAMI.rds", ".rds"))
    source = study / sources[0].name.replace("_KAMI.rds", ".rds")
    frame = read_movement_rds(source)
    individual = str(frame["individual_local_identifier"].iloc[0])
    expected = {f"file:{source.name}#row:{i + 1}"
                for i, matched in enumerate(frame["error_class"].isin(levels)) if matched}
    assert expected
    originals = {path.name: path.read_bytes() for path in study.glob("*.rds")}
    app = create_rds_movement_app(data_root=tmp_path / "data", cache_root=tmp_path / "cache",
        static_root=STATIC_ROOT, index_path=INDEX_PATH, auth_manager=_auth_manager())
    api = "/api/apps/movement/family/movement_rds/study/kami_categories"
    with _serve(app) as base_url, pw.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 1000})
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        _login_and_wait(page, base_url, "kami_categories")
        selected = page.locator(f'[data-individual-checkbox="{individual}"]')
        selected.wait_for(state="visible", timeout=30_000)
        page.locator('[data-role="select-none"]').click()
        selected.check()
        page.locator('[data-role="color-by"]').select_option("error_class")
        for level in levels:
            page.locator(f'[data-action="toggle-threshold-level"][data-level="{level}"]').check()
        page.locator('[data-action="check-above-threshold"]').click()
        before = page.locator('[data-role="dataset"]').input_value()
        with page.expect_response(lambda response: response.url.endswith("/actions/preview-filter")) as preview:
            page.locator('[data-role="mark-suspected"]').click()
        assert preview.value.status == 200, preview.value.text()
        assert preview.value.json()["match_count"] == len(expected)
        page.locator('[data-role="issue-modal"]').wait_for(state="visible")
        assert page.locator('[data-role="dataset"]').input_value() == before
        with page.expect_response(lambda response: response.url.endswith("/actions/annotate-scope")) as saved:
            page.locator('[data-role="issue-submit"]').click()
        response = saved.value
        assert response.status == 200, response.text()
        result = response.json()
        spec = result["step"]["parameters"]["scope"]["filter"]
        assert spec["field_key"] == "error_class"
        assert spec["field_kind"] == "categorical"
        assert set(spec["selected_levels"]) == set(levels)
        assert spec["individuals"] == [individual]
        assert result["step"]["summary"]["resolved_fix_count"] == len(expected)
        dataset = result["dataset"]["dataset_id"]
        page.wait_for_function("document.querySelector('[data-role=dataset]').value === " + json.dumps(dataset))
        page.reload(wait_until="domcontentloaded")
        page.locator('[data-role="study"] option[value="kami_categories"]').wait_for(state="attached")
        page.locator('[data-role="study"]').select_option("kami_categories")
        selected.wait_for(state="visible", timeout=30_000)
        assert page.locator('[data-role="dataset"]').input_value() == dataset
        flagged = page.request.get(base_url + api + f"/dataset/{dataset}/fixes", params={
            "logical_name": source.name, "review_status": "suspected",
        })
        assert flagged.status == 200, flagged.text()
        assert {fix["fix_key"] for fix in flagged.json()["fixes"]} == expected
        assert not errors
        browser.close()
    assert {name: (study / name).read_bytes() for name in originals} == originals


@pytest.mark.parametrize("source_format", ["csv", "rds"])
@pytest.mark.parametrize("percentile", [95, 99])
def test_zero_filter_assignment_and_shared_percentile_steps(tmp_path, source_format, percentile):
    import playwright.sync_api as pw
    app, study = make_app(tmp_path, source_format)
    with _serve(app) as base_url, pw.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 1000})
        errors, saved = [], []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("response", lambda response: saved.append(response)
                if response.url.endswith("/actions/annotate-scope") else None)
        _login_and_wait(page, base_url, "readiness")
        page.locator("[data-individual-checkbox]").first.wait_for(state="attached", timeout=30_000)
        with page.expect_response(lambda response: "/fixes-binary?" in response.url) as binary_response:
            page.locator('[data-role="select-all"]').click()
        binary = binary_response.value.body()
        page.locator('[data-role="fixes-progress"]').wait_for(state="hidden", timeout=30_000)
        header_length = struct.unpack_from("<I", binary, 4)[0]
        header = json.loads(binary[8:8 + header_length])
        offset = ((8 + header_length + 7) // 8) * 8
        arrays = {key: np.frombuffer(binary, dtype=column["dtype"], count=column["length"],
                    offset=offset + column["offset"]) for key, column in header["arrays"].items()}
        steps = arrays[header["color_columns"]["step_length_m"]["array"]]
        steps = steps[np.isfinite(steps) & (steps >= 0) & (arrays["review_status"] != 2)]
        expected = np.quantile(steps, [0.95, 0.99])

        page.locator('[data-role="color-by"]').select_option("speed_mps")
        cutoff = page.locator('[data-action="set-threshold-value"]')
        cutoff.fill("1e20")
        cutoff.press("Tab")
        page.get_by_role("button", name="Save filter (0 flags)", exact=True).click()
        page.locator('[data-role="issue-submit"]').click()
        page.wait_for_function("document.querySelector('[data-role=status]').textContent.includes('0 flags')")
        assert saved[0].json()["step"]["summary"]["resolved_fix_count"] == 0
        assert "0 flags" in page.locator('[data-role="dataset"] option:checked').text_content()
        assert "active review: browser-reviewer" in page.locator('[data-role="study"] option:checked').text_content()
        assert active_review(load_review_state(study)) is not None

        # An unobserved boolean level remains selectable to audit a zero run.
        page.locator('[data-role="color-by"]').select_option("is_outlier")
        page.locator('[data-action="toggle-threshold-level"][data-level="Missing"]').check()
        page.get_by_role("button", name="Save filter (0 flags)", exact=True).click()
        page.locator('[data-role="issue-submit"]').click()
        page.wait_for_function("document.querySelector('[data-role=status]').textContent.includes('0 flags')")
        assert saved[1].json()["step"]["summary"]["resolved_fix_count"] == 0
        assert page.locator('[data-action="set-threshold-percentile"]').count() == 0

        # Numeric presets are exploration controls: use the pooled source values,
        # retain signed values, and never open a save dialog or create a step.
        dataset_before_preview = page.locator('[data-role="dataset"]').input_value()
        for field in ["speed_mps", "step_length_m", "time_delta_s", "turn_angle_deg"]:
            values = arrays[header["color_columns"][field]["array"]]
            values = values[np.isfinite(values) & (arrays["review_status"] != 2)]
            page.locator('[data-role="color-by"]').select_option(field)
            for preset in [95, 99]:
                button = page.locator(f'[data-action="set-threshold-percentile"][data-percentile="{preset}"]')
                button.click()
                assert float(cutoff.input_value()) == pytest.approx(np.quantile(values, preset / 100))
                pw.expect(button).to_have_attribute("aria-pressed", "true")
                pw.expect(page.locator('[data-role="issue-modal"]')).to_be_hidden()
                assert page.locator('[data-role="dataset"]').input_value() == dataset_before_preview
                assert len(saved) == 2
            assert "without flagging" in button.get_attribute("title")
            assert page.locator('.movement-threshold-note').count() == 0
            help_icon = page.locator('.movement-threshold .movement-field-help')
            assert "Applies to visible individuals (2)" in help_icon.get_attribute("data-tooltip")
            help_icon.hover()
            page.wait_for_function("getComputedStyle(document.querySelector('.movement-threshold .movement-field-help'), '::after').opacity === '1'")

        page.locator('[data-role="color-by"]').select_option("gps_spike_step_turn")
        page.locator('[data-action="set-gps-spike-turn-angle"]').fill("0")
        page.locator('[data-action="set-gps-spike-turn-angle"]').press("Tab")
        assert float(page.locator('[data-action="set-threshold-value"]').input_value()) == pytest.approx(expected[1])
        # Histogram zoom must not change the population or the numerical cutoff.
        page.locator('[data-action="set-histogram-mode"][data-mode="clipped"]').click()
        assert float(page.locator('[data-action="set-threshold-value"]').input_value()) == pytest.approx(expected[1])
        assert page.locator('[data-action="set-threshold-percentile"]').evaluate_all(
            "buttons => buttons.map(button => button.dataset.percentile)") == ["95", "99"]
        page.locator(f'[data-action="set-threshold-percentile"][data-percentile="{percentile}"]').click()
        assert float(cutoff.input_value()) == pytest.approx(expected[0 if percentile == 95 else 1])
        pw.expect(page.locator('[data-role="issue-modal"]')).to_be_hidden()
        assert page.locator('[data-role="dataset"]').input_value() == dataset_before_preview
        assert len(saved) == 2
        # Changing the turn angle must leave the percentile cutoff unchanged.
        page.locator('[data-action="set-gps-spike-turn-angle"]').fill("150")
        page.locator('[data-action="set-gps-spike-turn-angle"]').press("Tab")
        assert float(cutoff.input_value()) == pytest.approx(expected[0 if percentile == 95 else 1])
        page.locator('[data-action="set-gps-spike-turn-angle"]').fill("0")
        page.locator('[data-action="set-gps-spike-turn-angle"]').press("Tab")
        # Only the separate flag action starts the save workflow.
        page.locator('[data-role="mark-suspected"]').click()
        page.locator('[data-role="issue-modal"]').wait_for(state="visible")
        with page.expect_response(lambda response: response.url.endswith("/actions/annotate-scope")) as percentile_response:
            page.locator('[data-role="issue-submit"]').click()
        response = percentile_response.value
        assert response.status == 200, response.text()
        page.locator('[data-role="issue-modal"]').wait_for(state="hidden")
        step = response.json()["step"]
        page.wait_for_function("document.querySelector('[data-role=dataset]').value === "
                               + json.dumps(response.json()["dataset"]["dataset_id"]))
        assert len(saved) == 3
        spec = step["parameters"]["scope"]["filter"]
        assert spec["step_length_threshold_m"] == pytest.approx(expected[0 if percentile == 95 else 1])
        assert spec["percentile"]["probability"] == percentile / 100
        assert spec["percentile"]["sample_count"] == len(steps)
        assert len(spec["individuals"]) == 2
        assert f"{percentile}th" in step["parameters"]["issue_threshold"]
        assert step["parameters"]["issue_type"] == "Filter GPS spike (step + turn)"

        # Non-GPS saved steps retain the chosen percentile and actual cutoff too.
        page.locator('[data-role="color-by"]').select_option("speed_mps")
        page.locator(f'[data-action="set-threshold-percentile"][data-percentile="{percentile}"]').click()
        speed_cutoff = float(cutoff.input_value())
        page.locator('[data-role="mark-suspected"]').click()
        with page.expect_response(lambda response: response.url.endswith("/actions/annotate-scope")) as speed_response:
            page.locator('[data-role="issue-submit"]').click()
        response = speed_response.value
        assert response.status == 200, response.text()
        page.wait_for_function("document.querySelector('[data-role=dataset]').value === "
                               + json.dumps(response.json()["dataset"]["dataset_id"]))
        step = response.json()["step"]
        spec = step["parameters"]["scope"]["filter"]
        assert spec["field_key"] == "speed_mps"
        assert spec["threshold_value"] == pytest.approx(speed_cutoff)
        assert spec["percentile"]["probability"] == percentile / 100
        assert spec["percentile"]["population"] == "finite-values-at-unconfirmed-fixes"
        assert f"; {percentile}th percentile" in step["parameters"]["issue_threshold"]
        assert float(
            step["parameters"]["issue_threshold"].split(";")[0].split()[1]) == pytest.approx(speed_cutoff)
        assert "both steps" not in step["parameters"]["issue_threshold"]
        assert len(saved) == 4
        page.locator('[data-role="individual-view-queue"]').click()
        page.locator('[data-role="individual-queue-order"]').select_option("flagged")
        assert page.locator('[data-role="individual-queue-order"]').input_value() == "flagged"
        assert not errors
        browser.close()


def test_queue_cancels_whole_study_loading_and_has_progress(tmp_path):
    import playwright.sync_api as pw
    app, _ = make_app(tmp_path, "rds")
    _delay_binary_responses(app, seconds=1.5)
    with _serve(app) as base_url, pw.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser)
        failures = []
        page.on("requestfailed", lambda request: failures.append(request.url))
        _login_and_wait(page, base_url, "readiness")
        page.locator("[data-individual-checkbox]").first.wait_for(state="attached", timeout=30_000)
        with page.expect_request(lambda request: "/fixes-binary?" in request.url) as full:
            page.locator('[data-role="select-all"]').click()
        page.locator('[data-role="fixes-progress"]').wait_for(state="visible")
        assert "Preparing fixes" in page.locator('[data-role="fixes-progress-text"]').text_content()
        assert page.locator('[data-role="fixes-progress-bar"]').get_attribute("value") is None
        page.locator('[data-role="individual-view-queue"]').click()
        page.wait_for_function("window.__movementDiagnosticsSnapshot().focusedObjectEntries >= 1", timeout=30_000)
        assert full.value.url in failures
        page.locator('[data-role="fixes-progress"]').wait_for(state="hidden", timeout=30_000)
        assert page.locator('[data-queue-individual].queue-active').count() == 1
        browser.close()
