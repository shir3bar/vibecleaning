"""Optional silhouettes never interfere with loading or saved review state."""
import json
from pathlib import Path
import subprocess
from urllib.parse import urlsplit, parse_qs, urlencode

import pytest

from test_study_filters import prepared, copied, api_client
from test_movement_browser import (
    _serve, _open_browser, _new_page, _login_and_wait, _open_toolbar_menu, TRANSPARENT_TILE,
)

ROOT = Path(__file__).resolve().parents[1]
NODE = "70ada787-ba19-4dd5-817f-17a1e7456b41"
IMAGE = "51b1b6e4-129d-41a6-bbbd-c3fab459c25f"
BIRD_NODE = "00000000-0000-0000-0000-000000000001"
BIRD_IMAGE = "00000000-0000-0000-0000-000000000002"
CORS = {"Access-Control-Allow-Origin": "*"}


def test_species_lookup_isolated_failures_and_exact_matches(node_runtime):
    result = subprocess.run([node_runtime, str(ROOT / "tests/frontend/species_portraits.cjs"),
        str(ROOT / "examples/movement/static/species_portraits.js")], capture_output=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def provider(page, *, mode="success", calls=None):
    calls = calls if calls is not None else []
    def respond(route):
        u = urlsplit(route.request.url)
        calls.append(route.request.url)
        if mode == "offline":
            route.abort()
            return
        if mode == "timeout": return
        if mode == "malformed":
            route.fulfill(json={"totalItems": 1, "totalPages": 1, "_links": "bad"}, headers=CORS)
            return
        name = parse_qs(u.query).get("filter_name", ["vulpes vulpes"])[0]
        if u.path.endswith(BIRD_IMAGE): name = "uria lomvia"
        node, image_id = (BIRD_NODE, BIRD_IMAGE) if name == "uria lomvia" else (NODE, IMAGE)
        if u.path == "/nodes" and "page" not in parse_qs(u.query):
            data = {"totalItems": 1, "totalPages": 1,
                    "_links": {"firstPage": {"href": "/nodes?" + urlencode({"build": 560, "page": 0, "filter_name": name})}}}
            if name == "unknown test species": data = {"totalItems": 0}
        elif u.path == "/nodes":
            data = {"_embedded": {"items": [{"uuid": node,
                "names": [[{"class": "scientific", "text": name}]],
                "_links": {"primaryImage": {"href": f"/images/{image_id}?build=560"}}}]}}
        else:
            data = {"uuid": image_id, "attribution": "Artist <script>unsafe()</script>", "_links": {
                "specificNode": {"href": f"/nodes/{node}"},
                "license": {"href": "https://creativecommons.org/licenses/by/4.0/"},
                "thumbnailFiles": [{"type": "image/png", "href":
                    f"https://images.phylopic.org/images/{image_id}/thumbnail/128x128.png"}]}}
        route.fulfill(json=data, headers=CORS)
    page.route("https://api.phylopic.org/**", respond)
    def image(route):
        if mode == "image": route.abort()
        else: route.fulfill(content_type="image/png", body=TRANSPARENT_TILE)
    page.route("https://images.phylopic.org/**", image)
    return calls


@pytest.mark.browser
def test_species_portrait_is_lazy_credited_cached_and_does_not_change_review(prepared, tmp_path):
    from playwright.sync_api import sync_playwright, expect
    h = copied(prepared, tmp_path)
    _, app = api_client(h, tmp_path)
    head = h.head
    with _serve(app) as url, sync_playwright() as p:
        browser = _open_browser(p)
        page = _new_page(browser, viewport={"width": 1024, "height": 850})
        errors, writes = [], []
        calls = provider(page)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("request", lambda request: writes.append(request.url)
            if request.method == "POST" and "/auth/" not in request.url else None)
        _login_and_wait(page, url, "910001")
        page.locator('[data-individual-checkbox]').first.wait_for(state="attached", timeout=30000)
        assert not calls
        baseline = page.locator('[data-role="map"]').bounding_box()
        _open_toolbar_menu(page, "species-menu")
        img = page.get_by_role("img", name="Silhouette of Vulpes vulpes")
        expect(img).to_be_visible()
        expect(page.locator(".movement-species-credit")).to_contain_text("Artist <script>unsafe()</script>")
        expect(page.get_by_role("link", name="PhyloPic", exact=True)).to_have_attribute(
            "href", f"https://www.phylopic.org/images/{IMAGE}")
        expect(page.get_by_role("link", name="CC BY 4.0", exact=True)).to_be_visible()
        assert page.locator(".movement-species-credit script").count() == 0
        assert page.locator('[data-role="species-menu"] .movement-species-reference-links a').count() == 3
        assert len(calls) == 3
        assert page.locator('[data-role="map"]').bounding_box() == baseline
        page.keyboard.press("Escape")
        _open_toolbar_menu(page, "species-menu")
        expect(img).to_be_visible()
        assert len(calls) == 3
        page.keyboard.press("Escape")
        page.locator('[data-study-filter-panel] > summary').click()
        expect(page.locator(".movement-filter-row")).to_have_count(3)
        assert not errors and not writes
        assert h.head == head
        browser.close()


@pytest.mark.browser
@pytest.mark.parametrize("mode", ["offline", "malformed", "image", "module", "timeout"])
def test_unavailable_portrait_keeps_review_and_links_usable(prepared, tmp_path, mode):
    from playwright.sync_api import sync_playwright, expect
    h = copied(prepared, tmp_path)
    _, app = api_client(h, tmp_path)
    head = h.head
    with _serve(app) as url, sync_playwright() as p:
        browser = _open_browser(p)
        page = _new_page(browser)
        calls = provider(page, mode=mode)
        if mode == "module": page.route("**/static/species_portraits.js", lambda route: route.abort())
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        _login_and_wait(page, url, "910001")
        page.locator('[data-individual-checkbox]').first.wait_for(state="attached", timeout=30000)
        assert not calls
        _open_toolbar_menu(page, "species-menu")
        if mode != "module":
            page.wait_for_function("window.performance.getEntriesByType('resource').some(e => e.name.includes('api.phylopic.org'))")
        if mode == "image":
            page.wait_for_function("window.performance.getEntriesByType('resource').some(e => e.name.includes('images.phylopic.org'))")
        expect(page.get_by_role("link", name="GBIF", exact=True)).to_be_visible()
        expect(page.locator('[data-role="species-menu"]')).to_contain_text("Vulpes vulpes")
        assert page.locator(".movement-species-silhouette").count() == 0
        page.keyboard.press("Escape")
        page.locator('[data-role="select-all"]').click()
        expect(page.locator('[data-individual-checkbox]').first).to_be_checked()
        assert not errors
        assert h.head == head
        browser.close()


@pytest.mark.browser
def test_mixed_species_decorate_independently_without_expanding_toolbar(prepared, tmp_path):
    from playwright.sync_api import sync_playwright, expect
    h = copied(prepared, tmp_path)
    path = h.path / "metadata.json"
    metadata = json.loads(path.read_text())
    for record, name in zip(metadata["individuals"].values(), ("Vulpes vulpes", "Uria lomvia", "Unknown test species")):
        record["species"] = name
    path.write_text(json.dumps(metadata))
    _, app = api_client(h, tmp_path)
    with _serve(app) as url, sync_playwright() as p:
        browser = _open_browser(p)
        page = _new_page(browser, viewport={"width": 560, "height": 850})
        provider(page)
        _login_and_wait(page, url, "910001")
        page.locator('[data-individual-checkbox]').first.wait_for(state="attached", timeout=30000)
        menu = page.locator('[data-role="species-menu"]')
        expect(menu.locator(":scope > summary")).to_contain_text("3 species")
        before = page.locator(".movement-toolbar").bounding_box()
        _open_toolbar_menu(page, "species-menu")
        expect(menu.locator(".movement-species-silhouette")).to_have_count(2)
        expect(menu.get_by_role("img", name="Silhouette of Uria lomvia")).to_be_visible()
        assert menu.locator('.movement-species-entry[data-species="Unknown test species"] img').count() == 0
        assert page.locator(".movement-toolbar").bounding_box() == before
        panel = menu.locator(".movement-popover-panel").bounding_box()
        assert panel["x"] >= 11 and panel["x"] + panel["width"] <= 549
        browser.close()
