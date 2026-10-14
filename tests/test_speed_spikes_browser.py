"""Two-stage previews must agree with the map and saved review steps."""

import json
import struct
import numpy as np

import pytest

from examples.movement import speed_spikes
from test_review_readiness_browser import make_app
from test_movement_browser import _serve, _open_browser, _new_page, _login_and_wait

pytestmark = pytest.mark.browser


@pytest.mark.parametrize('source_format', ['csv', 'rds'])
def test_edit_spike_thresholds_preview_save_reset_and_change_study(tmp_path, monkeypatch, source_format):
    import shutil
    import playwright.sync_api as pw
    app, study = make_app(tmp_path, source_format)
    shutil.copytree(study, study.with_name('other_study'))
    cap = {'v_max_m_s': 1., 'vmax_ci_lower': .5, 'scientific_name': 'Test species', 'common_name': 'Test species',
           'body_mass_kg': '1', 'movement_mode': 'running', 'source_file': 'test cap', 'source_sha256': 'test'}
    monkeypatch.setattr(speed_spikes, 'species_cap', lambda study_key, species, **kwargs: dict(cap))
    edits = {'lower_speed_mps': 0, 'upper_speed_mps': 1e6,
             'moderate_min_abs_turn_deg': 100, 'fast_min_abs_turn_deg': 80}
    with _serve(app) as url, pw.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={'width': 1280, 'height': 800})
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        _login_and_wait(page, url, 'readiness')
        page.locator('[data-individual-checkbox]').first.wait_for(state='attached', timeout=30_000)
        page.locator('[data-role="select-all"]').click()
        page.locator('[data-role="fixes-progress"]').wait_for(state='hidden', timeout=30_000)
        status = page.locator('[data-role="speed-spike-status"]')
        for field in ['gps_spike_two_stage', 'gps_spike_physiology']:
            with page.expect_response(lambda r: r.url.endswith('/actions/preview-filter')) as initial:
                page.locator('[data-role="color-by"]').select_option(field)
            assert initial.value.status == 200, initial.value.text()
            baseline = initial.value.json()['resolved_scope']['filter']
            if field == 'gps_spike_physiology':
                assert baseline['species_cap_field'] == 'vmax_ci_lower'
                assert baseline['upper_speed_mps'] == cap['vmax_ci_lower']
                assert baseline['species_cap']['v_max_m_s'] == cap['v_max_m_s']
            pw.expect(status).to_contain_text('candidate fixes')
            page.locator('[data-spike-stage="fast"]').uncheck()
            editor = page.locator('[data-role="spike-editor"]')
            if not editor.evaluate('el => el.open'):
                editor.locator('summary').click()
            dataset_before = page.locator('[data-role="dataset"]').input_value()

            def apply_edits():
                for key, value in edits.items():
                    editor.locator(f'[data-spike-threshold="{key}"]').fill(str(value))
                pw.expect(page.locator('[data-role="mark-suspected"]')).to_be_disabled()
                pw.expect(page.locator('[data-action="check-above-threshold"]')).to_be_disabled()
                assert page.locator('[data-role="dataset"]').input_value() == dataset_before
                with page.expect_response(lambda r: r.url.endswith('/actions/preview-filter')) as preview:
                    editor.locator('[data-action="preview-spike-edits"]').click()
                assert preview.value.status == 200, preview.value.text()
                payload = preview.value.json()
                spec = payload['resolved_scope']['filter']
                assert spec['threshold_overrides'] == edits
                assert spec['distribution'] == baseline['distribution']
                assert all(spec[key] == value for key, value in edits.items())
                pw.expect(status).to_have_text(f"{payload['match_count']} candidate fixes")
                pw.expect(page.locator('[data-spike-stage="fast"]')).not_to_be_checked()
                pw.expect(page.locator('[data-spike-stage="moderate"]')).to_be_checked()
                assert 'Lower–Upper · |turn| ≥100°' in page.locator('[data-role="threshold-pane"]').inner_text()
                for marker, value in [('q3', 0), ('upper', 1e6)]:
                    assert float(page.locator(f'[data-speed-marker="{marker}"]').get_attribute('data-speed-value')) == value
                page.wait_for_function('n => window.__movementDiagnostics.binaryThresholdMatchCount === n', arg=payload['match_count'])
                return payload

            apply_edits()
            with page.expect_response(lambda r: r.url.endswith('/actions/preview-filter')) as reset:
                editor.locator('[data-action="reset-spike-defaults"]').click()
            assert reset.value.status == 200, reset.value.text()
            reset_spec = reset.value.json()['resolved_scope']['filter']
            assert not reset_spec.get('threshold_overrides')
            assert reset_spec['lower_speed_mps'] == baseline['lower_speed_mps']
            assert reset_spec['upper_speed_mps'] == baseline['upper_speed_mps']
            pw.expect(status).to_contain_text('candidate fixes')
            expected = apply_edits()['match_count']
            pane_bounds = page.locator('[data-role="threshold-pane"]').bounding_box()
            map_bounds = page.locator('[data-role="map"]').bounding_box()
            assert pane_bounds['y'] >= map_bounds['y']
            assert pane_bounds['height'] <= map_bounds['height']
            page.locator('[data-role="threshold-pane"]').screenshot(path=tmp_path / f'{field}-editable.png')
            page.locator('[data-role="mark-suspected"]').click()
            with page.expect_response(lambda r: r.url.endswith('/actions/annotate-scope')) as saved:
                page.locator('[data-role="issue-submit"]').click()
            assert saved.value.status == 200, saved.value.text()
            result = saved.value.json()
            recorded = result['step']['parameters']['scope']['filter']
            assert recorded['threshold_overrides'] == edits
            assert recorded['selected_stages'] == ['moderate']
            assert all(recorded[key] == value for key, value in edits.items())
            assert result['step']['summary']['resolved_fix_count'] == expected
            page.locator('[data-role="issue-modal"]').wait_for(state='hidden')
            # The modal closes before the saved dataset finishes loading.
            # Wait for the completed save before switching filters/studies.
            pw.expect(page.locator('[data-role="status"]')).to_have_text(
                f"Flagged {expected:,} fixes as suspicious in {result['dataset']['dataset_id']}."
                if expected else 'Filter saved as a step: 0 flags.')
            page.locator('[data-role="color-by"]').select_option('individual')

        # Custom choices are local to this study, not silently carried to another.
        page.locator('[data-role="study"]').select_option('other_study')
        page.locator('[data-individual-checkbox]').first.wait_for(state='attached', timeout=30_000)
        page.locator('[data-role="select-all"]').click()
        page.locator('[data-role="fixes-progress"]').wait_for(state='hidden', timeout=30_000)
        with page.expect_response(lambda r: r.url.endswith('/actions/preview-filter')) as other:
            page.locator('[data-role="color-by"]').select_option('gps_spike_physiology')
        assert other.value.status == 200, other.value.text()
        assert not other.value.json()['resolved_scope']['filter'].get('threshold_overrides')
        assert not errors, errors
        browser.close()


@pytest.mark.parametrize('source_format', ['csv', 'rds'])
def test_two_stage_preview_colors_scope_and_save(tmp_path, monkeypatch, source_format):
    import playwright.sync_api as pw
    app, study = make_app(tmp_path, source_format)
    if source_format == 'csv':
        (study/'movement.csv').write_text('eventid,individual,timestamp,longitude,latitude\n' + ''.join(
            f'a{i},alpha,2024-01-01T00:{i:02d}:00Z,{x},0\n'
            for i,x in enumerate([0,.001,.1,.002,.003,.004,.005,.006])) + ''.join(
            f'b{i},beta,2024-01-01T00:{i:02d}:00Z,{20+i*.00001},0\n' for i in range(20)))
    # The small RDS fixture's species has no supplied cap; use an explicit test cap.
    cap = {'v_max_m_s':1.,'vmax_ci_lower':0.,'scientific_name':'Test species','common_name':'Test species',
           'body_mass_kg':'1','movement_mode':'running','source_file':'test cap','source_sha256':'test'}
    monkeypatch.setattr(speed_spikes, 'species_cap', lambda study_key, species, **kwargs: dict(cap))
    with _serve(app) as url, pw.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={'width':1440,'height':1000})
        errors=[]
        page.on('pageerror',lambda error: errors.append(str(error)))
        _login_and_wait(page,url,'readiness')
        page.locator('[data-individual-checkbox]').first.wait_for(state='attached',timeout=30_000)
        page.locator('[data-role="select-all"]').click()
        page.locator('[data-role="fixes-progress"]').wait_for(state='hidden',timeout=30_000)
        for field in ['gps_spike_two_stage','gps_spike_physiology']:
            before=page.locator('[data-role="dataset"]').input_value()
            with page.expect_response(lambda r: r.url.endswith('/actions/preview-filter')) as preview:
                page.locator('[data-role="color-by"]').select_option(field)
            assert preview.value.status == 200, preview.value.text()
            payload=preview.value.json()
            scope=payload['resolved_scope']; spec=scope['filter']; expected=payload['match_count']
            if field == 'gps_spike_physiology':
                assert spec['species_cap_field'] == 'vmax_ci_lower'
                assert spec['upper_speed_mps'] == 0
            assert expected > 0
            status=page.locator('[data-role="speed-spike-status"]')
            pw.expect(status).to_have_text(f'{expected} candidate fixes')
            assert float(page.locator('[data-role="spike-q3"]').get_attribute('title')) == spec['lower_speed_mps']
            assert float(page.locator('[data-role="spike-upper"]').get_attribute('title')) == spec['upper_speed_mps']
            histogram = page.locator('[data-role="speed-histogram"]')
            pw.expect(histogram.locator('svg')).to_be_visible()
            assert int(histogram.get_attribute('data-sample-count')) == spec['distribution']['sample_count']
            assert sum(scope['speed_histogram']['linear']['counts']) == spec['distribution']['sample_count']
            for key, value in [('q3', spec['lower_speed_mps']), ('upper', spec['upper_speed_mps'])]:
                marker = histogram.locator(f'[data-speed-marker="{key}"]')
                assert float(marker.get_attribute('data-speed-value')) == value
                assert 12 <= float(marker.locator('line').get_attribute('x1')) <= 310
            pane = page.locator('[data-role="threshold-pane"]')
            assert 'Fast spike' not in pane.inner_text() and 'Sharp spike' not in pane.inner_text()
            assert ('Q3–cap lower CI' if field == 'gps_spike_physiology' else 'Q3–P99.9') in pane.inner_text()
            assert pane.evaluate('(el) => el.scrollWidth <= el.clientWidth')
            pane.screenshot(path=str(tmp_path / f'{source_format}-{field}-histogram.png'))
            # Axis changes explore the same distribution without changing cutoffs,
            # selected candidates, or creating a review step.
            for scale in ['linear', 'log']:
                histogram.locator(f'[data-scale="{scale}"]').click()
                pw.expect(histogram).to_have_attribute('data-scale', scale)
                pw.expect(status).to_have_text(f'{expected} candidate fixes')
                assert float(page.locator('[data-role="spike-q3"]').get_attribute('title')) == spec['lower_speed_mps']
                assert float(page.locator('[data-role="spike-upper"]').get_attribute('title')) == spec['upper_speed_mps']
            page.wait_for_function('n => window.__movementDiagnostics.binaryThresholdMatchCount === n',arg=expected)
            assert page.locator('[data-role="dataset"]').input_value() == before
            pw.expect(page.locator('[data-role="issue-modal"]')).to_be_hidden()
            # The categorical legend and worker receive exactly the preview groups.
            page.locator('[data-action="check-above-threshold"]').click()
            assert page.evaluate('window.__movementDiagnosticsSnapshot().selectedFixCount') == expected
            # Recalculate for just the individual containing a candidate: study quantiles stay fixed.
            individual='MF043' if source_format=='rds' else 'alpha'
            page.locator('[data-role="select-none"]').click()
            with page.expect_response(lambda r: r.url.endswith('/actions/preview-filter')) as selected:
                page.locator(f'[data-individual-checkbox="{individual}"]').check()
            assert selected.value.status == 200, selected.value.text()
            assert selected.value.json()['resolved_scope']['filter']['distribution'] == spec['distribution']
            assert selected.value.json()['resolved_scope']['speed_histogram'] == scope['speed_histogram']
            expected=selected.value.json()['match_count']
            pw.expect(status).to_have_text(f'{expected} candidate fixes')
            stages=['moderate','fast']
            if field == 'gps_spike_two_stage':
                page.locator('[data-spike-stage="moderate"]').uncheck()
                stages=['fast']
                expected=next(g['fix_count'] for g in selected.value.json()['resolved_scope']['spike_groups'] if g['stage']=='fast')
                pw.expect(status).to_have_text(f'{expected} candidate fixes')
                if expected == 0:
                    pw.expect(page.locator('[data-role="mark-suspected"]')).to_have_text('Save filter (0 flags)')
            page.locator('[data-role="mark-suspected"]').click()
            with page.expect_response(lambda r:r.url.endswith('/actions/annotate-scope')) as saved:
                page.locator('[data-role="issue-submit"]').click()
            assert saved.value.status == 200,saved.value.text()
            result=saved.value.json(); recorded=result['step']['parameters']['scope']['filter']
            assert result['step']['summary']['resolved_fix_count'] == expected
            assert recorded['distribution'] == spec['distribution']
            assert recorded['individuals'] == [individual]
            assert recorded['selected_stages'] == stages
            assert recorded['upper_speed_mps'] == spec['upper_speed_mps']
            page.wait_for_function('id => document.querySelector("[data-role=dataset]").value === id',arg=result['dataset']['dataset_id'])
            page.locator('[data-role="issue-modal"]').wait_for(state='hidden')
            page.locator('[data-role="color-by"]').select_option('individual')
            page.locator('[data-role="select-all"]').click()
        # Confirming a spike changes adjacent speeds. Refresh numerical values
        # without replacing retained map geometry, then check the next P99.
        page.evaluate("""() => {
            const original = deck.MapboxOverlay.prototype.setProps;
            deck.MapboxOverlay.prototype.setProps = function(props) {
                if (props.layers) window.__speedTestLayers = props.layers;
                return original.call(this, props);
            };
        }""")
        page.locator('[data-role="individual-view-queue"]').click()
        card=page.locator(f'[data-queue-individual="{individual}"]')
        card.locator('.movement-title').click()
        confirm=card.locator('[data-queue-issue-action="confirm"][data-issue-type="Filter GPS spike (physiology)"]')
        confirm.wait_for(state='visible',timeout=30_000)
        with page.expect_response(lambda r:r.url.endswith('/actions/confirm-issues')) as confirmed:
            confirm.click()
        assert confirmed.value.status == 200,confirmed.value.text()
        confirm.wait_for(state='detached',timeout=30_000)
        page.locator('[data-role="individual-view-browse"]').click()
        page.locator('[data-role="select-none"]').click()
        page.locator(f'[data-individual-checkbox="{individual}"]').check()
        page.wait_for_function('window.__speedTestLayers?.some(l => l.id.startsWith("movement-binary-points-") && l.props.visible)')
        page.evaluate("""() => {
            const layer = window.__speedTestLayers.find(l => l.id.startsWith('movement-binary-points-') && l.props.visible);
            window.__speedTestBinary = layer.props.userData.binaryBlock;
            window.__speedTestView = window.__movementDiagnosticsSnapshot().mapView;
        }""")
        page.locator('[data-role="color-by"]').select_option('gps_spike_speed_turn')
        cutoff=page.locator('[data-action="set-threshold-value"]')
        cutoff.wait_for(state='visible',timeout=30_000)
        family='movement_rds' if source_format=='rds' else 'movement_raw'
        dataset=page.locator('[data-role="dataset"]').input_value()
        refreshed=page.request.get(f'{url}/api/apps/movement/family/{family}/study/readiness/dataset/{dataset}/fixes-binary',params={
            'logical_name':result['step']['parameters']['target_artifact'],'individuals':individual})
        assert refreshed.status == 200
        raw=refreshed.body(); header_len=struct.unpack_from('<I',raw,4)[0]
        header=json.loads(raw[8:8+header_len]); offset=((8+header_len+7)//8)*8
        arrays={k:np.frombuffer(raw,dtype=v['dtype'],count=v['length'],offset=offset+v['offset']) for k,v in header['arrays'].items()}
        speeds=arrays[header['color_columns']['speed_mps']['array']]; speeds=speeds[np.isfinite(speeds)&(arrays['review_status']!=2)]
        cutoff=page.locator('[data-action="set-threshold-value"]')
        cutoff.wait_for(state='visible',timeout=30_000)
        assert float(cutoff.input_value()) == pytest.approx(np.quantile(speeds,.99))
        assert page.evaluate("""() => {
            const layer = window.__speedTestLayers.find(l => l.id.startsWith('movement-binary-points-') && l.props.visible);
            return layer.props.userData.binaryBlock === window.__speedTestBinary;
        }""")
        before=page.evaluate('window.__speedTestView'); after=page.evaluate('window.__movementDiagnosticsSnapshot().mapView')
        assert before['center'] == pytest.approx(after['center'])
        assert before['zoom'] == pytest.approx(after['zoom'])
        assert not errors
        browser.close()
