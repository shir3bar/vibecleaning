"""Audit speed units, tier boundaries, exclusions and CSV/RDS parity."""
from datetime import datetime, timedelta, timezone
import csv

import numpy as np
import pandas as pd
import pytest

from examples.movement import rds_index
from examples.movement.movement_features import compute_track_movement
from examples.movement.review_annotations import resolve_filter_row_ranges
from examples.movement.routes import _validate_filter_scope
from examples.movement.speed_spikes import candidate_stage, resolve_speed_spikes, species_cap, speed_histogram, validate_speed_spike
from examples.movement.stationarity import is_gps_spike_annotation


@pytest.mark.parametrize('incoming,outgoing,angle,stage', [
    (1, 20, 180, None), (0.9, 20, 180, None), (2, 2, 165.99, None),
    (2, 2, 166, 'moderate'), (2, 2, -166, 'moderate'),
    (20, 2, 165, None), (20, 2, 170, 'moderate'),
    (20, 10, 160, None), (20, 10, 166, 'moderate'),
    (20, 20, 157, 'fast'), (20, 20, -157, 'fast'), (20, 20, 156.99, None),
    (None, 20, 180, None), (float('nan'), 20, 180, None), (20, 20, None, None),
])
def test_exact_tier_boundaries_and_both_adjacent_speeds(incoming, outgoing, angle, stage):
    assert candidate_stage(incoming, outgoing, angle,
        {'method': 'study_quantiles', 'lower_speed_mps': 1, 'upper_speed_mps': 10}) == stage


def track(individual, xs, seconds=60):
    return [dict(fix_key=f'row:{i+1}', row_index=i+1, source_artifact=f'{individual}.csv',
        individual=individual, set_name='train', time_ms=i*seconds*1000, lon=x, lat=0)
        for i,x in enumerate(xs)]


@pytest.mark.parametrize('values', [[], [0, 0, 0], [2], [0, 0, .00001, .1, 1, 1, 100000]])
def test_speed_histogram_counts_every_step_and_keeps_zero_on_log_axis(values):
    histogram = speed_histogram(np.asarray(values, dtype=float))
    assert histogram['sample_count'] == len(values)
    assert histogram['zero_count'] == values.count(0)
    assert sum(histogram['linear']['counts']) == len(values)
    assert sum(histogram['log']['counts']) + histogram['zero_count'] == len(values)
    for name in ['linear', 'log']:
        edges = histogram[name]['edges']
        assert all(np.isfinite(edges))
        assert all(np.diff(edges) > 0)
        if edges:
            assert len(edges) == len(histogram[name]['counts']) + 1


def test_speed_is_time_aware_and_exclusions_rebuild_adjacent_steps():
    spec = {'kind': 'gps_spike', 'metric': 'speed', 'method': 'manual',
            'speed_threshold_mps': 1, 'minimum_abs_turn_angle_deg': 166}
    fast = track('alpha', [0, .01, 0])
    slow = track('alpha', [0, .01, 0], seconds=3600)
    scope, count = resolve_speed_spikes(lambda: iter([fast]), spec, [])
    assert count == 1 and scope['source_rows'][0]['row_ranges'] == [[2, 2]]
    assert resolve_speed_spikes(lambda: iter([slow]), spec, [])[1] == 0
    confirmed = {'status': 'confirmed', 'scope': {'kind': 'fix', 'row_ranges': [[2, 2]]}}
    assert resolve_speed_spikes(lambda: iter([fast]), spec, [confirmed])[1] == 0
    # Unresolved spikes are not removed by another spike rule.
    assert resolve_speed_spikes(lambda: iter([fast]), spec, [{**confirmed, 'status': 'suspected', 'issue_type': 'GPS spikes'}])[1] == 1
    duplicates = track('alpha', [0, .01, 0], seconds=0)
    assert resolve_speed_spikes(lambda: iter([duplicates]), spec, [])[1] == 0


def test_study_quantiles_ignore_selection_and_turn_angles_and_replay_freezes_them():
    tracks = [track('alpha', [0, .01, 0, -.0001]), track('beta', [20+i*.00001 for i in range(100)])]
    spec = {'kind': 'gps_spike', 'metric': 'speed', 'method': 'study_quantiles', 'individuals': ['alpha']}
    scope, count = resolve_speed_spikes(lambda: iter(tracks), spec, [])
    all_scope, _ = resolve_speed_spikes(lambda: iter(tracks), {**spec, 'individuals': []}, [])
    assert scope['filter']['distribution'] == all_scope['filter']['distribution']
    assert scope['speed_histogram'] == all_scope['speed_histogram']
    assert sum(scope['speed_histogram']['linear']['counts']) == 102
    assert 'histogram' not in scope['filter']['distribution']
    values = []
    for records in tracks:
        movement, _ = compute_track_movement({(records[0]['individual'], 'train'): records})
        values.extend(v['speed_mps'] for v in movement.values() if v['speed_mps'] is not None)
    assert scope['filter']['distribution']['sample_count'] == len(values) == 102
    assert scope['filter']['lower_speed_mps'] == pytest.approx(np.quantile(values, .75))
    assert scope['filter']['upper_speed_mps'] == pytest.approx(np.quantile(values, .999))
    assert count == 1
    replay, _ = resolve_speed_spikes(lambda: iter([tracks[0]]), scope['filter'], [])
    assert replay['filter']['distribution'] == scope['filter']['distribution']


def test_physiology_retains_q3_floor_and_saved_cap():
    records = track('alpha', [0, .01, 0, .0001])
    spec = {'kind': 'gps_spike', 'metric': 'speed', 'method': 'physiology',
            'distribution': {'sample_count': 10, 'q1': .1, 'q3': 1, 'p999': 100},
            'species_cap': {'v_max_m_s': .5, 'vmax_ci_lower': .2, 'source_sha256': 'saved'}}
    resolved = validate_speed_spike(spec)
    assert resolved['species_cap_field'] == 'v_max_m_s'
    assert resolved['upper_speed_mps'] == .5
    assert candidate_stage(.9, .9, 180, resolved) is None
    assert candidate_stage(2, 2, 157, resolved) == 'fast'
    def must_not_read_table():
        raise AssertionError('Saved cap must be replayed without looking up a new table')
    scope, _ = resolve_speed_spikes(lambda: iter([records]), spec, [], cap_loader=must_not_read_table)
    assert scope['filter']['species_cap'] == spec['species_cap']


@pytest.mark.parametrize('lower_ci', [25, .001, 0])
def test_new_physiology_uses_lower_ci_and_replay_preserves_it(lower_ci):
    records = track('alpha', [0, .01, 0, .0001])
    cap = {'v_max_m_s': 50, 'vmax_ci_lower': lower_ci, 'source_sha256': 'supplied'}
    scope, count = resolve_speed_spikes(lambda: iter([records]),
        {'kind': 'gps_spike', 'metric': 'speed', 'method': 'physiology'}, [],
        cap_loader=lambda: dict(cap))
    spec = scope['filter']
    assert spec['species_cap_field'] == 'vmax_ci_lower'
    assert spec['upper_speed_mps'] == lower_ci
    assert spec['species_cap'] == cap
    q3 = spec['distribution']['q3']
    assert spec['lower_speed_mps'] == q3
    assert candidate_stage(q3, q3, 180, spec) is None
    above_both = max(q3, lower_ci) + 1
    assert candidate_stage(above_both, above_both, 157, spec) == 'fast'
    replay, replay_count = resolve_speed_spikes(lambda: iter([records]), spec, [],
        cap_loader=lambda: pytest.fail('Replay must retain the saved CI'))
    assert replay['filter'] == spec
    assert replay_count == count
    assert replay['source_rows'] == scope['source_rows']


@pytest.mark.parametrize('lower_ci', [None, -1, float('nan'), float('inf')])
def test_physiology_rejects_unusable_lower_ci_without_using_point_estimate(lower_ci):
    with pytest.raises(ValueError):
        resolve_speed_spikes(lambda: iter([track('alpha', [0, .01, 0])]),
            {'kind': 'gps_spike', 'metric': 'speed', 'method': 'physiology'}, [],
            cap_loader=lambda: {'v_max_m_s': 50, 'vmax_ci_lower': lower_ci})


@pytest.mark.parametrize('method', ['study_quantiles', 'physiology'])
def test_custom_thresholds_keep_defaults_and_replay_exact_values(method):
    tracks = [track('alpha', [0, .01, 0, -.0001]), track('beta', [20+i*.00001 for i in range(100)])]
    edits = {'lower_speed_mps': 2, 'upper_speed_mps': 10,
             'moderate_min_abs_turn_deg': 140, 'fast_min_abs_turn_deg': 130}
    spec = {'kind': 'gps_spike', 'metric': 'speed', 'method': method, 'threshold_overrides': edits}
    scope, count = resolve_speed_spikes(lambda: iter(tracks), spec, [],
        cap_loader=lambda: {'v_max_m_s': 50, 'vmax_ci_lower': 25})
    recorded = scope['filter']
    assert all(recorded[key] == value for key, value in edits.items())
    assert recorded['threshold_overrides'] == edits
    assert recorded['lower_cutoff_policy'] == 'custom'
    assert recorded['distribution']['q3'] < 2
    assert recorded['distribution']['p999'] > 10
    if method == 'physiology':
        assert recorded['species_cap']['v_max_m_s'] == 50  # Original table value, not the edit.
        assert recorded['species_cap']['vmax_ci_lower'] == 25
        assert recorded['species_cap_field'] == 'vmax_ci_lower'
    assert candidate_stage(2, 100, 180, recorded) is None
    assert candidate_stage(5, 100, 139.9, recorded) is None
    assert candidate_stage(5, 100, 140, recorded) == 'moderate'
    assert candidate_stage(10, 100, 135, recorded) is None  # Equal upper cutoff stays in lower group.
    assert candidate_stage(11, 100, 129.9, recorded) is None
    assert candidate_stage(11, 100, -130, recorded) == 'fast'
    replay, replay_count = resolve_speed_spikes(lambda: iter([tracks[0]]), recorded, [],
        cap_loader=lambda: pytest.fail('Replay must use the saved cap and overrides'))
    assert replay_count == count
    assert replay['filter']['distribution'] == recorded['distribution']
    assert replay['filter']['threshold_overrides'] == edits
    assert replay['source_rows'] == scope['source_rows']


@pytest.mark.parametrize('overrides', [[], {'unknown': 5}, {'lower_speed_mps': -1},
    {'upper_speed_mps': float('inf')}, {'lower_speed_mps': True},
    {'moderate_min_abs_turn_deg': 181}, {'fast_min_abs_turn_deg': -1}])
def test_reject_invalid_custom_thresholds(overrides):
    with pytest.raises(ValueError):
        _validate_filter_scope({'kind': 'gps_spike', 'metric': 'speed',
            'method': 'study_quantiles', 'threshold_overrides': overrides})


def test_supplied_caps_and_stationarity_recognize_all_new_spike_labels():
    cap = species_cap('BartlamBrooks_c9b4f9b5', {'Equus burchellii'})
    assert cap['v_max_m_s'] == pytest.approx(16.1511733336179)
    assert cap['vmax_ci_lower'] == pytest.approx(6.55065547905739)
    assert species_cap('Bildstein_Car_Talk', {'Cathartes aura'})['v_max_m_s'] == pytest.approx(36.2181051693226)
    assert species_cap('Bildstein_Car_Talk', {'Cathartes aura'})['vmax_ci_lower'] == pytest.approx(16.8978509026709)
    assert species_cap('costa2024_elephant_seals', set())['vmax_ci_lower'] == 0
    with pytest.raises(ValueError, match='No unique'):
        species_cap('unknown', {'unknown'})
    for label in ['GPS spike (speed + turn)', 'GPS spike (two-stage speed)', 'GPS spike (physiology)']:
        assert is_gps_spike_annotation({'issue_type': f'Filter {label}'})


def test_legacy_distance_scope_stays_distance_and_iqr_provenance_is_validated():
    old = _validate_filter_scope({'kind': 'gps_spike', 'step_length_threshold_m': 200,
                                 'minimum_abs_turn_angle_deg': 120})
    assert old['step_length_threshold_m'] == 200 and 'metric' not in old
    preset = {'method': 'q3-plus-100-iqr', 'q1': 1, 'q3': 3, 'sample_count': 20}
    spec = _validate_filter_scope({'kind': 'gps_spike', 'metric': 'speed', 'speed_threshold_mps': 203,
                                  'minimum_abs_turn_angle_deg': 166, 'cutoff_preset': preset})
    assert spec['cutoff_preset']['multiplier'] == 100
    with pytest.raises(ValueError, match='IQR'):
        _validate_filter_scope({'field_key': 'step_length_m', 'field_kind': 'numeric',
                                'threshold_value': 203, 'cutoff_preset': preset})


@pytest.mark.parametrize('overrides', [{}, {'lower_speed_mps': 1e9, 'upper_speed_mps': 1e10,
                                          'moderate_min_abs_turn_deg': 140, 'fast_min_abs_turn_deg': 120}])
def test_csv_rds_matching_and_confirmed_exclusion_parity(tmp_path, monkeypatch, overrides):
    frames, csv_rows = {}, []
    epoch = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for iid, (individual, xs) in enumerate([
        ('alpha', [0, .0001, .0002, .01, .0003, .0004]),
        ('beta', [20+i*.00001 for i in range(100)]),
    ], 1):
        records = track(individual, xs)
        times = [epoch.timestamp()+r['time_ms']/1000 for r in records]
        frame = pd.DataFrame({'x_':xs, 'y_':[0.]*len(xs), 't_':times, 'timestamp':times,
            'geometry':[[x,0.] for x in xs], 'study_id':[1]*len(xs), 'individual_id':[iid]*len(xs),
            'individual_local_identifier':[individual]*len(xs), 'burst_':[1]*len(xs)})
        frame.attrs.update({'class':['sf','move2'], 'sf_column':'geometry', 'time_column':'t_',
            'track_id_column':'individual_local_identifier', 'geometry_crs':{'input':'EPSG:4326'}})
        p=tmp_path/f'1_{iid}.rds'; p.write_bytes(b'fixture reader is mocked'); frames[p]=frame
        csv_rows.extend({'eventid':f'{individual}_{i}', 'individual':individual,
            'timestamp':(epoch+timedelta(seconds=i*60)).isoformat(), 'longitude':x, 'latitude':0}
            for i,x in enumerate(xs))
    monkeypatch.setattr(rds_index, 'read_movement_rds', lambda path: frames[path].copy())
    index=tmp_path/'index.sqlite'; paths=tuple(frames)
    rds_index.build_rds_index(rds_index.RdsBundle(tmp_path,'test',tuple({'logical_name':p.name} for p in paths),paths,'test'),index)
    path=tmp_path/'movement.csv'
    with path.open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(csv_rows[0])); writer.writeheader();writer.writerows(csv_rows)
    spec={'kind':'gps_spike','metric':'speed','method':'study_quantiles','individuals':['alpha'],
          'threshold_overrides': overrides}
    for exclusions in ([],[{'status':'confirmed','scope':{'kind':'fix','source_rows':[{'logical_name':'1_1.rds','row_ranges':[[4,4]]}]}}]):
        rds_scope,count=rds_index.resolve_rds_review_scope(index,{'kind':'filter','filter':spec},annotations=exclusions)
        csv_exclusions=[{'status':'confirmed','scope':{'kind':'fix','row_ranges':[[4,4]]}}] if exclusions else []
        details={}
        rows,csv_count=resolve_filter_row_ranges(path,spec,annotations=csv_exclusions,stationarity_details=details)
        assert csv_count == count == (0 if exclusions or overrides else 2)
        assert details['filter']['distribution'] == pytest.approx(rds_scope['filter']['distribution'])
        assert details['speed_histogram'] == rds_scope['speed_histogram']
        assert rows == ([pair for source in rds_scope['source_rows'] for pair in source['row_ranges']])


def test_csv_direct_save_freezes_physiology_cutoffs_for_bundled_replay(tmp_path):
    from app.state import get_dataset_artifact
    from test_movement_fixes import create_movement_test_client
    import json
    content = 'individual,timestamp,longitude,latitude,species\n' + ''.join(
        f'alpha,2024-01-01T00:{i:02d}:00Z,{x},0,Cathartes aura\n'
        for i,x in enumerate([0, .001, .5, .002, .003, .004, .005, .006, .007, .008]))
    client, dataset = create_movement_test_client(tmp_path, csv_content=content)
    base = '/api/apps/movement/family/movement_clean/study/test_study'
    response = client.post(base + '/actions/annotate-scope', json={
        'dataset_id': dataset, 'expected_current_dataset_id': dataset,
        'logical_name': 'movement.csv', 'status': 'suspected', 'user': 'reviewer',
        'issue_type': 'Filter GPS spike (physiology)', 'origin': 'threshold',
        'comment': 'Check physiology spikes', 'scope': {'kind': 'filter', 'filter': {
            'kind': 'gps_spike', 'metric': 'speed', 'method': 'physiology'}}})
    assert response.status_code == 200, response.text
    saved = response.json()
    recorded = saved['step']['parameters']['scope']['filter']
    assert recorded['species_cap']['scientific_name'] == 'Cathartes aura'
    assert recorded['distribution']['sample_count'] == 9
    assert recorded['species_cap_field'] == 'vmax_ci_lower'
    assert recorded['upper_speed_mps'] == pytest.approx(16.8978509026709)
    assert recorded['species_cap']['v_max_m_s'] == pytest.approx(36.2181051693226)
    study = tmp_path/'data'/'movement_clean'/'test_study'
    _, sidecar = get_dataset_artifact(study, saved['dataset']['dataset_id'], 'movement_review_annotations.json')
    annotation = json.loads(sidecar.read_text())['annotations'][0]
    assert annotation['scope']['filter'] == recorded
    assert annotation['scope']['row_ranges'] == [[3,3]]
    from examples.movement.routes import _movement_graph_payload
    graph_filter = _movement_graph_payload(study)['steps'][0]['label_parameters']['filter']
    assert graph_filter['metric'] == 'speed'
    assert graph_filter['method'] == 'physiology'
    assert graph_filter['species_cap_field'] == 'vmax_ci_lower'
    assert graph_filter['upper_speed_mps'] == recorded['upper_speed_mps']
    assert graph_filter['distribution'] == recorded['distribution']
    assert (study/'movement.csv').read_text() == content


def test_rds_saved_spikes_match_exported_original_rows(tmp_path):
    from test_rds_movement import _client
    from examples.movement.rds_index import read_movement_rds
    client, study = _client(tmp_path)
    originals = {p.name: p.read_bytes() for p in study.glob('*.rds')}
    base = '/api/apps/movement/family/movement_rds/study/268904527'
    loaded = client.get(base+'/load').json()
    dataset, logical = loaded['dataset_id'], loaded['logical_name']
    spec = {'kind':'gps_spike','metric':'speed','method':'study_quantiles'}
    preview = client.post(base+'/actions/preview-filter',json={
        'dataset_id':dataset,'logical_name':logical,'filter':spec})
    assert preview.status_code == 200, preview.text
    scope = preview.json()['resolved_scope']
    assert preview.json()['match_count'] == 1
    response = client.post(base+'/actions/annotate-scope',json={
        'dataset_id':dataset,'logical_name':logical,'expected_current_dataset_id':dataset,
        'expected_review_revision':loaded['edit_profile']['review_revision'],
        'scope':scope,'status':'suspected','origin':'threshold',
        'issue_type':'Filter GPS spike (two-stage speed)','comment':'Compare speed rules'})
    assert response.status_code == 200, response.text
    saved = response.json()
    assert saved['step']['parameters']['scope']['filter'] == scope['filter']
    exported = client.post(base+'/actions/export-reviewed-rds',json={
        'dataset_id':saved['dataset']['dataset_id'],'writer':'python'})
    assert exported.status_code == 200, exported.text
    expected = {s['logical_name']: {row for lo,hi in s['row_ranges'] for row in range(lo,hi+1)} for s in scope['source_rows']}
    for name, original in originals.items():
        frame = read_movement_rds(study/'scrubdata'/'cleaned_files'/name.replace('.rds','_cleaned.rds'))
        flagged = {i+1 for i,value in enumerate(frame['outlier_status']) if str(value) == 'suspected'}
        assert flagged == expected.get(name,set())
        assert set(frame.loc[frame['outlier_status']=='suspected','outlier_issue_type']) <= {'Filter GPS spike (two-stage speed)'}
        assert (study/name).read_bytes() == original


def test_real_vulture_rds_resolves_supplied_cap_without_species_metadata(tmp_path):
    from pathlib import Path
    import shutil
    from fastapi.testclient import TestClient
    from examples.rds_movement.app import create_rds_movement_app
    from test_movement_browser import _auth_manager, STATIC_ROOT, INDEX_PATH
    source=Path(__file__).resolve().parents[1]/'data'/'movement_rds'/'481458_20761565.rds'
    study=tmp_path/'data'/'movement_rds'/'renamed_study'
    study.mkdir(parents=True)
    shutil.copy2(source,study/source.name)
    app=create_rds_movement_app(data_root=tmp_path/'data',cache_root=tmp_path/'cache',
        static_root=STATIC_ROOT,index_path=INDEX_PATH,auth_manager=_auth_manager())
    with TestClient(app) as client:
        # Use the same real login as the browser helpers.
        client.post('/api/auth/login',json={'username':'browser-reviewer','password':'test-password-long'})
        base='/api/apps/movement/family/movement_rds/study/renamed_study'
        loaded=client.get(base+'/load').json()
        assert 'dataset_id' in loaded,loaded
        response=client.post(base+'/actions/preview-filter',json={
            'dataset_id':loaded['dataset_id'],'logical_name':loaded['logical_name'],
            'filter':{'kind':'gps_spike','metric':'speed','method':'physiology'}})
        assert response.status_code == 200,response.text
        spec=response.json()['resolved_scope']['filter']
        assert spec['species_cap']['scientific_name']=='Cathartes aura'
        assert spec['species_cap']['matched_by']=='movebank_study_id'
        assert spec['species_cap_field'] == 'vmax_ci_lower'
        assert spec['upper_speed_mps']==pytest.approx(16.8978509026709)
        assert spec['species_cap']['v_max_m_s']==pytest.approx(36.2181051693226)
        assert response.json()['match_count'] > 0
