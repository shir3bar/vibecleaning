import pytest

from test_review_readiness_browser import make_app
from test_movement_browser import _serve, _open_browser, _new_page, _login_and_wait

pytestmark = pytest.mark.browser


@pytest.mark.parametrize('source_format', ['csv', 'rds'])
def test_removal_reason_blocks_navigation_and_saves(tmp_path, source_format):
    import playwright.sync_api as pw
    app, _ = make_app(tmp_path, source_format)
    with _serve(app) as url, pw.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser)
        _login_and_wait(page, url, 'readiness')
        page.locator('[data-individual-checkbox]').first.wait_for(state='attached')
        page.locator('[data-role="individual-view-queue"]').click()
        active = page.locator('[data-queue-individual].queue-active')
        active.wait_for()
        individual = active.get_attribute('data-queue-individual')
        active.locator('[data-review-decision="remove"]').click()
        save = page.locator('[data-role="individual-queue-save"]')
        pw.expect(save).to_be_disabled()
        page.locator('[data-queue-nav="next-individual"]').click()
        assert active.get_attribute('data-queue-individual') == individual
        active.locator('[data-removal-reason]').select_option('other')
        pw.expect(save).to_be_disabled()
        active.locator('[data-queue-comment-input]').fill('Confined near rehabilitation site')
        pw.expect(save).to_be_enabled()
        with page.expect_response(lambda r: r.url.endswith('/actions/review-individual')) as saved:
            save.click()
        assert saved.value.status == 200, saved.value.text()
        assert saved.value.json()['step']['parameters']['decision']['removal_reason'] == 'other'
        browser.close()
