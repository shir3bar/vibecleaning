import pytest

from test_review_readiness_browser import make_app
from test_movement_browser import _serve, _open_browser, _new_page, _login_and_wait

pytestmark = pytest.mark.browser


def flag_first_individual(app, source_format):
    from fastapi.testclient import TestClient
    client = TestClient(app)
    client.post('/api/auth/login', json={'username': 'browser-reviewer', 'password': 'test-password-long'})
    family = 'movement_rds' if source_format == 'rds' else 'movement_raw'
    base = f'/api/apps/movement/family/{family}/study/readiness'
    loaded = client.get(base + '/load').json()
    dataset = loaded['dataset_id']
    overview = client.get(base + f'/dataset/{dataset}/overview', params={'logical_name': loaded['logical_name']}).json()
    response = client.post(base + '/actions/annotate-scope', json={
        'dataset_id': dataset, 'expected_current_dataset_id': dataset,
        'expected_review_revision': loaded['edit_profile']['review_revision'],
        'logical_name': loaded['logical_name'], 'scope': {'kind': 'individual', 'individual': overview['individuals'][0]},
        'status': 'suspected', 'origin': 'manual', 'issue_type': 'Test flag', 'comment': 'Inspect all fixes'})
    assert response.status_code == 200, response.text
    return overview['individuals']


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


@pytest.mark.parametrize('source_format', ['csv', 'rds'])
def test_bulk_removal_dialog_covers_unselected_individuals(tmp_path, source_format):
    import playwright.sync_api as pw
    app, _ = make_app(tmp_path, source_format)
    with _serve(app) as url, pw.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser)
        _login_and_wait(page, url, 'readiness')
        page.locator('[data-individual-checkbox]').first.wait_for(state='attached')
        page.locator('[data-role="select-none"]').click()
        page.locator('[data-role="remove-study"]').click()
        modal = page.locator('[data-role="remove-study-modal"]')
        pw.expect(modal).to_be_visible()
        pw.expect(modal).to_contain_text('2 individuals')
        modal.locator('select').select_option('nest')
        with page.expect_response(lambda r: r.url.endswith('/actions/remove-study')) as saved:
            page.locator('[data-role="remove-study-submit"]').click()
        assert saved.value.status == 200, saved.value.text()
        assert saved.value.json()['step']['summary']['reviewed_remove_count'] == 2
        pw.expect(modal).to_be_hidden()
        browser.close()


@pytest.mark.parametrize('source_format', ['csv', 'rds'])
def test_completion_saves_pending_decision_and_groups_completed_studies(tmp_path, source_format):
    import playwright.sync_api as pw
    import shutil
    app, study = make_app(tmp_path, source_format)
    other = study.with_name('zz_unreviewed')
    other.mkdir()
    for source in study.glob('*.' + source_format):
        shutil.copy2(source, other / source.name)
    with _serve(app) as url, pw.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser)
        page.on('dialog', lambda dialog: dialog.accept())
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        _login_and_wait(page, url, 'readiness')
        page.locator('[data-individual-checkbox]').first.wait_for(state='attached')
        page.locator('[data-role="individual-view-queue"]').click()
        active = page.locator('[data-queue-individual].queue-active')
        first = active.get_attribute('data-queue-individual')
        active.locator('[data-review-decision="ok"]').click()
        page.locator('[data-role="individual-queue-save"]').click()
        pw.expect(active).not_to_have_attribute('data-queue-individual', first)
        active.locator('[data-review-decision="ok"]').click()
        complete = page.locator('[data-role="complete-review"]')
        pw.expect(complete).to_be_enabled()
        complete.click()
        pw.expect(page.locator('[data-role="export-progress-text"]')).to_contain_text('Review completed', timeout=30000)
        selection = page.locator('[data-role="study"]')
        pw.expect(selection).to_have_value('readiness')
        pw.expect(selection.locator('option').last).to_have_text('readiness · Completed')
        assert (study / 'scrubdata' / 'cleaned_files' / 'individual_decisions.csv').exists()
        page.reload(wait_until='domcontentloaded')
        page.locator('[data-role="study"] option[value="readiness"]').wait_for(state='attached')
        pw.expect(selection.locator('option').last).to_have_text('readiness · Completed')
        assert not errors
        browser.close()


@pytest.mark.parametrize('source_format', ['csv', 'rds'])
def test_track_lines_hide_during_progressive_loading_preserving_points_and_player(tmp_path, source_format):
    import playwright.sync_api as pw
    from test_movement_browser import _delay_binary_responses
    app, _ = make_app(tmp_path, source_format)
    _delay_binary_responses(app, seconds=1)
    individuals = flag_first_individual(app, source_format)
    with _serve(app) as url, pw.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={'width': 1440, 'height': 1000})
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        _login_and_wait(page, url, 'readiness')
        page.locator('[data-individual-checkbox]').first.wait_for(state='attached')
        lines = page.locator('[data-role="show-track-lines"]')
        pw.expect(lines).to_be_checked()
        lines.uncheck()
        page.locator(f'[data-individual-checkbox="{individuals[0]}"]').check()
        page.wait_for_function('window.__movementDiagnostics.renderedLayerIds.some(id => id.startsWith("movement-overview-preview-"))')
        assert not page.evaluate('window.__movementDiagnostics.visibleLayerIds.some(id => id.includes("preview-") || id.includes("paths-"))')
        page.wait_for_function('window.__movementDiagnostics.visibleLayerIds.some(id => id.startsWith("movement-binary-points-"))')
        before = page.evaluate('window.__movementDiagnostics.visibleLayerIds')
        assert any('suspected' in layer for layer in before)
        assert not any('paths-' in layer or 'burst-casing' in layer or 'flagged-fix-steps' in layer for layer in before)
        lines.check()
        page.wait_for_function('window.__movementDiagnostics.visibleLayerIds.some(id => id.startsWith("movement-binary-paths-"))')
        lines.uncheck()
        assert set(before) == set(page.evaluate('window.__movementDiagnostics.visibleLayerIds'))
        page.locator('[data-role="individual-view-queue"]').click()
        page.wait_for_function('window.__movementDiagnostics.visibleLayerIds.includes("movement-track-player-position")')
        assert not page.evaluate('window.__movementDiagnostics.visibleLayerIds.some(id => id.includes("paths-"))')
        pw.expect(page.locator('[data-role="show-points"]')).to_be_checked()
        pw.expect(page.locator('[data-role="show-bursts"]')).to_be_checked()
        page.reload(wait_until='domcontentloaded')
        page.locator('[data-role="study"] option[value="readiness"]').wait_for(state='attached')
        pw.expect(lines).not_to_be_checked()
        assert not errors
        browser.close()


@pytest.mark.parametrize('source_format', ['csv', 'rds'])
def test_flagged_sort_directions_stable_ties_and_persisted_selection(tmp_path, source_format):
    import playwright.sync_api as pw
    app, study = make_app(tmp_path, source_format)
    if source_format == 'csv':
        with (study / 'movement.csv').open('a') as stream:
            stream.write('g0,gamma,2024-01-01T00:30:00Z,-71.1,41.1,train,False\n')
    individuals = flag_first_individual(app, source_format)
    with _serve(app) as url, pw.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser)
        _login_and_wait(page, url, 'readiness')
        page.locator('[data-individual-checkbox]').first.wait_for(state='attached')
        page.locator('[data-role="individual-view-queue"]').click()
        order = page.locator('[data-role="individual-queue-order"]')
        active = page.locator('[data-queue-individual].queue-active')
        selected = active.get_attribute('data-queue-individual')
        cards = page.locator('[data-queue-individual]')
        def names():
            return cards.evaluate_all('items => items.map(item => item.dataset.queueIndividual)')
        order.select_option('flagged')
        assert names() == individuals
        order.select_option('flagged_asc')
        assert names() == individuals[1:] + individuals[:1]
        assert active.get_attribute('data-queue-individual') == selected
        page.reload(wait_until='domcontentloaded')
        page.locator('[data-role="study"] option[value="readiness"]').wait_for(state='attached')
        page.locator('[data-role="study"]').select_option('readiness')
        page.locator('[data-individual-checkbox]').first.wait_for(state='attached')
        page.locator('[data-role="individual-view-queue"]').click()
        pw.expect(order).to_have_value('flagged_asc')
        assert names() == individuals[1:] + individuals[:1]
        browser.close()
