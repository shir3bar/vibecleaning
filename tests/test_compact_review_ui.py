"""Compact controls remain accessible without changing review or filter state."""
import re

import pytest

from test_study_filters import prepared, copied, api_client
from test_movement_browser import _serve, _open_browser, _new_page, _login_and_wait, _open_toolbar_menu

pytestmark = pytest.mark.browser


def test_compact_controls_version_species_exports_and_keyboard(prepared, tmp_path):
    from playwright.sync_api import sync_playwright, expect
    h = copied(prepared, tmp_path)
    _, app = api_client(h, tmp_path)
    with _serve(app) as url, sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 1000})
        errors, calculations = [], []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("request", lambda request: calculations.append(request.url) if request.method == "POST" and "study-filter" in request.url else None)
        _login_and_wait(page, url, "910001")
        page.locator('[data-individual-checkbox]').first.wait_for(state="attached", timeout=30000)
        assert page.locator(".movement-toolbar").bounding_box()["height"] <= 115
        version = page.locator('[data-role="dataset"]')
        assert version.bounding_box()["width"] <= 305
        label = version.evaluate("el => el.options[el.selectedIndex].text")
        assert label == f"Latest · GPS Spike · {h.head.split('_', 1)[1]}"
        assert h.head not in label and "|" not in label
        color = page.locator('[data-role="color-by"]')
        color.select_option("individual")
        expect(page.locator('[data-role="threshold-pane"]')).to_be_hidden()
        color.select_option("step_length_m")
        expect(page.locator('[data-role="threshold-pane"]')).to_be_visible()
        color.select_option("individual")
        _open_toolbar_menu(page, "version-menu")
        expect(page.locator('[data-role="version-details"]')).to_contain_text("GPS Spike")
        page.keyboard.press("Escape")
        expect(page.locator('[data-role="version-menu"] > summary')).to_be_focused()
        assert not page.locator('[data-role="version-menu"]').evaluate("el => el.open")

        layers = page.locator('[data-role="layers-menu"]')
        layers.locator(":scope > summary").focus()
        page.keyboard.press("Enter")
        lines = page.locator('[data-role="show-track-lines"]')
        expect(lines).to_be_visible()
        assert lines.bounding_box()["width"] == 16
        lines.uncheck()
        expect(lines).not_to_be_checked()
        page.keyboard.press("Escape")
        assert not layers.evaluate("el => el.open")

        _open_toolbar_menu(page, "export-menu")
        expect(page.locator('[data-role="export-menu"] .movement-popover-panel button:visible')).to_have_count(2)
        expect(page.locator('[data-role="export-reviewed-csv"]')).to_contain_text(".rds ZIP")
        with page.expect_download() as download:
            page.locator('[data-role="export-individual-decisions"]').click()
        assert download.value.suggested_filename.endswith(".csv")
        assert not page.locator('[data-role="export-menu"]').evaluate("el => el.open")

        _open_toolbar_menu(page, "species-menu")
        expect(page.locator('[data-role="species-menu"] a:visible')).to_have_count(4)
        movebank = page.get_by_role("link", name="Open study 910001 on Movebank")
        assert movebank.get_attribute("href") == "https://www.movebank.org/cms/webapp?gwt_fragment=page%3Dstudies%2Cpath%3Dstudy910001"
        assert "q=Vulpes%20vulpes" in page.get_by_role("link", name="GBIF", exact=True).get_attribute("href")
        page.locator('[data-study-filter-panel] > summary').click()
        assert not page.locator('[data-role="species-menu"]').evaluate("el => el.open")
        expect(page.locator('[data-study-filter-panel] .movement-filter-row')).to_have_count(3)
        expect(page.locator('[data-study-filter-panel]')).to_contain_text("Optional")
        expect(page.locator('[data-study-filter="speed"] [data-study-filter-rule] p').first).not_to_be_visible()
        page.locator('[data-study-filter="speed"] [data-study-filter-rule] > summary').click()
        expect(page.locator('[data-study-filter="speed"]')).to_contain_text("Q3 + 100")
        page.locator('[data-study-filter="gps_spike"] [data-study-filter-rule] > summary').click()
        gps_details = page.locator('[data-study-filter="gps_spike"] [data-study-filter-rule]')
        expect(gps_details).to_contain_text(re.compile(r"P99 speed [\d,.]+ m/s"))
        assert not calculations
        assert not errors
        browser.close()


@pytest.mark.parametrize("width", [1024, 560])
def test_menus_stay_on_screen_and_leave_map_size_unchanged(prepared, tmp_path, width):
    from playwright.sync_api import sync_playwright
    h = copied(prepared, tmp_path)
    _, app = api_client(h, tmp_path)
    with _serve(app) as url, sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": width, "height": 850})
        _login_and_wait(page, url, "910001")
        page.locator('[data-individual-checkbox]').first.wait_for(state="attached", timeout=30000)
        baseline = page.locator('[data-role="map"]').bounding_box()
        for role in ("layers-menu", "version-menu", "export-menu", "more-menu", "species-menu"):
            _open_toolbar_menu(page, role)
            panel = page.locator(f'[data-role="{role}"] > .movement-popover-panel')
            bounds = panel.bounding_box()
            assert bounds["x"] >= 11
            assert bounds["x"] + bounds["width"] <= width - 11
            assert page.locator('[data-role="map"]').bounding_box() == baseline
            page.keyboard.press("Escape")
        page.locator('[data-study-filter-panel] > summary').click()
        bounds = page.locator(".movement-filter-drawer").bounding_box()
        assert bounds["x"] >= 11 and bounds["x"] + bounds["width"] <= width - 11
        assert page.locator('[data-role="map"]').bounding_box() == baseline
        assert not page.evaluate("document.documentElement.scrollWidth > innerWidth")
        browser.close()
