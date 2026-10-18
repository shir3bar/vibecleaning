import json
import csv
import io

import pytest
from fastapi.testclient import TestClient

from app.state import get_dataset_artifact
from examples.movement.review_annotations import normalize_annotation
from examples.movement.routes import _validate_individual_review_decision
from test_review_readiness_browser import make_app


def review_client(tmp_path, source_format):
    app, study = make_app(tmp_path, source_format)
    client = TestClient(app)
    client.post('/api/auth/login', json={'username': 'browser-reviewer', 'password': 'test-password-long'})
    family = 'movement_rds' if source_format == 'rds' else 'movement_raw'
    base = f'/api/apps/movement/family/{family}/study/readiness'
    loaded = client.get(base + '/load').json()
    return client, study, base, loaded


def decision_body(client, base, loaded, individual, **decision):
    dataset = loaded['dataset_id']
    profile = client.get(base + '/edit-profile', params={'dataset_id': dataset}).json()
    return dict(dataset_id=dataset, expected_current_dataset_id=dataset,
                expected_review_revision=profile['review_revision'], logical_name=loaded['logical_name'],
                decision={**dict(individual=individual, review_decision='remove', needs_check=False,
                              comment=''), **decision})


def downloaded_decisions(client, base, dataset):
    response = client.get(base + f'/dataset/{dataset}/individual-decisions.csv')
    assert response.status_code == 200, response.text
    return list(csv.DictReader(io.StringIO(response.text)))


@pytest.mark.parametrize('source_format', ['csv', 'rds'])
def test_bulk_removal_csv_conflict_and_undo(tmp_path, source_format):
    from app.edit_locks import undo_guarded
    client, study, base, loaded = review_client(tmp_path, source_format)
    original = {p.name: p.read_bytes() for p in study.glob('*.' + source_format)}
    initial = downloaded_decisions(client, base, loaded['dataset_id'])
    assert len(initial) == 2
    assert {r['review_decision'] for r in initial} == {'undecided'}
    first = initial[0]['individual']
    saved = client.post(base + '/actions/review-individual', json=decision_body(
        client, base, loaded, first, review_decision='ok', needs_check=True, comment='Prior note')).json()
    before = saved['dataset']['dataset_id']
    loaded['dataset_id'] = before
    body = decision_body(client, base, loaded, first)
    body.update(removal_reason='other', comment='Nid déplacé,\n研究')
    removed = client.post(base + '/actions/remove-study', json=body)
    assert removed.status_code == 200, removed.text
    result = removed.json()
    after = result['dataset']['dataset_id']
    rows = downloaded_decisions(client, base, after)
    assert len(rows) == len(initial)
    assert {r['review_decision'] for r in rows} == {'remove'}
    assert {r['comment'] for r in rows} == {'Nid déplacé,\n研究'}
    assert {r['removal_reason'] for r in rows} == {'other'}
    assert {r['reviewer_username'] for r in rows} == {'browser-reviewer'}
    assert len({r['step_id'] for r in rows}) == 1
    assert all(r['review_id'] and r['review_round'] and r['reviewed_at'] for r in rows)
    assert next(r for r in rows if r['individual'] == first)['needs_check'] == 'true'
    _, path = get_dataset_artifact(study, after, 'movement_review_annotations.json')
    annotations = json.loads(path.read_text())['annotations']
    assert annotations[0]['comment'] == 'Prior note'
    assert len(annotations) == 3
    assert client.post(base + '/actions/remove-study', json=body).status_code == 423
    undo_guarded(study, expected_current_dataset_id=after)
    restored = downloaded_decisions(client, base, before)
    assert next(r for r in restored if r['individual'] == first)['review_decision'] == 'ok'
    assert {p.name: p.read_bytes() for p in study.glob('*.' + source_format)} == original


@pytest.mark.parametrize('reason,comment,valid', [
    ('', '', False), ('unknown', '', False), ('other', '  ', False),
    ('other', 'Nest relocated,\ncheck source notes', True),
    *[(reason, '', True) for reason in ['redeployment', 'short_track', 'confined', 'nest']],
])
def test_removal_reason_validation(reason, comment, valid):
    value = dict(individual='alpha', review_decision='remove', needs_check=False,
                 removal_reason=reason, comment=comment)
    if not valid:
        with pytest.raises(ValueError):
            _validate_individual_review_decision(value)
    else:
        assert _validate_individual_review_decision(value)['removal_reason'] == reason


def test_legacy_removal_remains_readable_and_non_removal_clears_reason():
    assert normalize_annotation({'annotation_kind': 'individual_review', 'reviewed': True,
                                 'review_decision': 'remove'})['removal_reason'] == ''
    assert _validate_individual_review_decision(dict(individual='alpha', review_decision='ok',
               needs_check=False, removal_reason='nest'))['removal_reason'] == ''


@pytest.mark.parametrize('source_format', ['csv', 'rds'])
def test_removal_reason_saved_and_projected(tmp_path, source_format):
    client, study, base, loaded = review_client(tmp_path, source_format)
    overview = client.get(base + f"/dataset/{loaded['dataset_id']}/overview",
                          params={'logical_name': loaded['logical_name']}).json()
    individual = overview['individuals'][0]
    body = decision_body(client, base, loaded, individual)
    rejected = client.post(base + '/actions/review-individual', json=body)
    assert rejected.status_code == 400, rejected.text
    # The editor's first mutation may have created its self-assigned review.
    body = decision_body(client, base, loaded, individual, removal_reason='nest')
    response = client.post(base + '/actions/review-individual', json=body)
    assert response.status_code == 200, response.text
    saved = response.json()
    _, path = get_dataset_artifact(study, saved['dataset']['dataset_id'], 'movement_review_annotations.json')
    assert json.loads(path.read_text())['annotations'][-1]['removal_reason'] == 'nest'
    result = client.get(base + f"/dataset/{saved['dataset']['dataset_id']}/overview",
                        params={'logical_name': loaded['logical_name']}).json()
    assert result['individual_reviews'][individual]['removal_reason'] == 'nest'


def remove_all(client, base, loaded):
    body = decision_body(client, base, loaded, 'unused')
    body.update(removal_reason='short_track')
    response = client.post(base + '/actions/remove-study', json=body)
    assert response.status_code == 200, response.text
    loaded['dataset_id'] = response.json()['dataset']['dataset_id']
    return decision_body(client, base, loaded, 'unused')


@pytest.mark.parametrize('source_format', ['csv', 'rds'])
@pytest.mark.parametrize('background', [False, True])
def test_completion_exports_full_inputs_and_decisions_idempotently(tmp_path, source_format, background):
    import hashlib
    from app.reviews import load_review_state
    from app.state import project_paths
    client, study, base, loaded = review_client(tmp_path, source_format)
    original = {p.name: p.read_bytes() for p in study.glob('*.' + source_format)}
    body = remove_all(client, base, loaded)
    body['background'] = background
    response = client.post(base + '/review/complete', json=body)
    assert response.status_code == (202 if background else 200), response.text
    result = response.json()
    if background:
        job = client.get(base + '/analysis-jobs/' + result['job_id']).json()
        assert job['status'] == 'completed', job
        result = job['result']
    assert result['review']['status'] == 'completed'
    output = project_paths(study)['meta'] / 'cleaned_files'
    manifest = json.loads((output / 'writer_manifest.json').read_text())
    assert manifest['dataset_id'] == loaded['dataset_id']
    assert manifest['review_id'] == result['review']['review_id']
    assert sum(item['row_count'] for item in manifest['files']) > 0
    for item in manifest['files'] + manifest['auxiliary_files']:
        assert hashlib.sha256((output / item['output_name']).read_bytes()).hexdigest() == item['output_sha256']
    assert list(csv.DictReader(io.StringIO((output / 'individual_decisions.csv').read_text()))) == downloaded_decisions(client, base, loaded['dataset_id'])
    assert {p.name: p.read_bytes() for p in study.glob('*.' + source_format)} == original
    before = load_review_state(study)
    retry = client.post(base + '/review/complete', json=body)
    assert retry.status_code == 200, retry.text
    assert retry.json()['already_completed']
    assert load_review_state(study) == before
    profile = client.get(base + '/edit-profile', params={'dataset_id': loaded['dataset_id']}).json()
    assert profile['completed'] is True
    studies = client.get(base.rsplit('/study/', 1)[0] + '/studies').json()['studies']
    assert studies[0]['completed'] is True


def test_completion_incomplete_and_failed_publication_preserve_prior_export(tmp_path, monkeypatch):
    from app.reviews import active_review, load_review_state
    from app.state import project_paths
    import examples.movement.completion_publication as publication
    client, study, base, loaded = review_client(tmp_path, 'csv')
    first = downloaded_decisions(client, base, loaded['dataset_id'])[0]['individual']
    saved = client.post(base + '/actions/review-individual', json=decision_body(
        client, base, loaded, first, review_decision='ok')).json()
    loaded['dataset_id'] = saved['dataset']['dataset_id']
    response = client.post(base + '/review/complete', json=decision_body(client, base, loaded, first))
    assert response.status_code == 423
    assert not (project_paths(study)['meta'] / 'cleaned_files').exists()
    body = remove_all(client, base, loaded)
    assert client.post(base + '/review/complete', json=body).status_code == 200
    output = project_paths(study)['meta'] / 'cleaned_files'
    original = {p.name: p.read_bytes() for p in output.iterdir()}
    body = remove_all(client, base, loaded)
    replace = publication.atomic_replace
    def fail_publish(source, target):
        if source.name.startswith('.completion-ready-'):
            raise OSError('disk unavailable')
        return replace(source, target)
    with monkeypatch.context() as patch:
        patch.setattr(publication, 'atomic_replace', fail_publish)
        response = client.post(base + '/review/complete', json=body)
        assert response.status_code == 400, response.text
    assert active_review(load_review_state(study)) is not None
    assert {p.name: p.read_bytes() for p in output.iterdir()} == original
    assert client.post(base + '/review/complete', json=body).status_code == 200


@pytest.mark.parametrize('change', ['dataset', 'assignment', 'permissions'])
def test_completion_rechecks_after_generating_files(tmp_path, monkeypatch, change):
    import examples.movement.routes as routes
    from app.edit_locks import undo_guarded
    from app.reviews import load_review_state, save_review_state, active_review
    from app.state import project_paths
    client, study, base, loaded = review_client(tmp_path, 'csv')
    body = remove_all(client, base, loaded)
    create = routes.create_analysis
    def changed(*args, **kwargs):
        result = create(*args, **kwargs)
        if change == 'dataset':
            undo_guarded(study, expected_current_dataset_id=loaded['dataset_id'])
        elif change == 'assignment':
            state = load_review_state(study)
            state['revision'] += 1
            active_review(state)['reviewer_user_id'] = 'someone-else'
            save_review_state(study, state)
        else:
            monkeypatch.setattr(client.app.state.auth_manager, 'actor_by_id', lambda user_id: None)
        return result
    monkeypatch.setattr(routes, 'create_analysis', changed)
    response = client.post(base + '/review/complete', json=body)
    assert response.status_code == (403 if change == 'permissions' else 409), response.text
    assert active_review(load_review_state(study)) is not None
    assert not (project_paths(study)['meta'] / 'cleaned_files').exists()


def test_simultaneous_completions_record_one_event(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    import examples.movement.routes as routes
    from app.reviews import load_review_state
    client, study, base, loaded = review_client(tmp_path, 'csv')
    body = remove_all(client, base, loaded)
    create = routes.create_analysis
    barrier = Barrier(2)
    def generate(*args, **kwargs):
        result = create(*args, **kwargs)
        barrier.wait(timeout=10)
        return result
    monkeypatch.setattr(routes, 'create_analysis', generate)
    with ThreadPoolExecutor(max_workers=2) as workers:
        responses = list(workers.map(lambda _: client.post(base + '/review/complete', json=body), range(2)))
    assert [response.status_code for response in responses] == [200, 200]
    assert sum(response.json().get('already_completed', False) for response in responses) == 1
    assert sum(event['type'] == 'review_completed' for event in load_review_state(study)['events']) == 1


@pytest.mark.parametrize('had_previous', [False, True])
def test_interrupted_publication_restores_previous_export(tmp_path, had_previous):
    import hashlib
    from app.state import project_paths
    from examples.movement.completion_publication import prepared_publication, recover_publication
    client, study, base, loaded = review_client(tmp_path, 'csv')
    destination = project_paths(study)['meta'] / 'cleaned_files'
    files = [{'output_name': 'individual_decisions.csv', 'output_sha256': hashlib.sha256(b'new').hexdigest()}]
    output = tmp_path / 'export'
    output.mkdir()
    (output / 'individual_decisions.csv').write_bytes(b'new')
    manifest = {'files': files, 'analysis_id': 'interrupted'}
    (output / 'writer_manifest.json').write_text(json.dumps(manifest))
    if had_previous:
        destination.mkdir()
        (destination / 'individual_decisions.csv').write_bytes(b'old')
        (destination / 'writer_manifest.json').write_text(json.dumps({'files': files}))
    class Interrupted(BaseException):
        pass
    def crash():
        raise Interrupted()
    with pytest.raises(Interrupted), prepared_publication(study, output, manifest) as publish:
        publish({}, crash)
    recover_publication(study)
    assert destination.exists() == had_previous
    if had_previous:
        assert (destination / 'individual_decisions.csv').read_bytes() == b'old'


def test_multi_csv_study_covers_all_sources_and_ignores_context_exports(tmp_path):
    app, study = make_app(tmp_path, 'csv')
    (study / 'second.csv').write_text('eventid,individual,timestamp,longitude,latitude\ng1,gamma,2024-01-01T00:00:00Z,-71,41\n')
    (study / 'movement_osm_context.csv').write_text('context,value\nwater,true\n')
    client = TestClient(app)
    client.post('/api/auth/login', json={'username': 'browser-reviewer', 'password': 'test-password-long'})
    base = '/api/apps/movement/family/movement_raw/study/readiness'
    loaded = client.get(base + '/load').json()
    dataset = loaded['dataset_id']
    header = client.head(base + f'/dataset/{dataset}/individual-decisions.csv')
    assert header.status_code == 200, header.text
    assert header.headers['X-Individual-Count'] == '3'
    body = remove_all(client, base, loaded)
    rows = downloaded_decisions(client, base, loaded['dataset_id'])
    assert {row['individual'] for row in rows} == {'alpha', 'beta', 'gamma'}
    assert {row['review_decision'] for row in rows} == {'remove'}
    completed = client.post(base + '/review/complete', json=body)
    assert completed.status_code == 200, completed.text
    assert completed.json()['coverage']['required_count'] == 3
    manifest = json.loads((study / 'scrubdata/cleaned_files/writer_manifest.json').read_text())
    assert {item['logical_name'] for item in manifest['files']} == {'movement.csv', 'second.csv'}


def test_review_permissions_flags_prior_reasons_and_completed_download(tmp_path):
    from app.auth import AuthManager, build_user_record
    from examples.slim_movement.app import create_slim_movement_app
    from test_movement_browser import STATIC_ROOT, INDEX_PATH
    _, study = make_app(tmp_path, 'csv')
    manager = AuthManager([build_user_record(username=name, user_id=name, role=role,
        password='test-password-long') for name, role in [('editor', 'editor'), ('one', 'reviewer'), ('two', 'reviewer')]])
    app = create_slim_movement_app(data_root=tmp_path / 'data', static_root=STATIC_ROOT,
                                  index_path=INDEX_PATH, auth_manager=manager)
    clients = {}
    for name in ['editor', 'one', 'two']:
        clients[name] = TestClient(app)
        clients[name].post('/api/auth/login', json={'username': name, 'password': 'test-password-long'})
    editor, reviewer, other = (clients[name] for name in ['editor', 'one', 'two'])
    base = '/api/apps/movement/family/movement_raw/study/readiness'
    loaded = editor.get(base + '/load').json()
    body = decision_body(editor, base, loaded, 'alpha')
    assigned = editor.post(base + '/review/assign', json={**body, 'reviewer_user_id': 'one'})
    assert assigned.status_code == 200, assigned.text
    body = decision_body(reviewer, base, loaded, 'alpha')
    assert other.post(base + '/actions/remove-study', json={**body, 'removal_reason': 'nest'}).status_code == 403
    assert other.get(base + f"/dataset/{loaded['dataset_id']}/individual-decisions.csv").status_code == 403
    flagged = reviewer.post(base + '/actions/annotate-scope', json={**body,
        'scope': {'kind': 'individual', 'individual': 'alpha'}, 'status': 'suspected',
        'issue_type': 'Existing issue', 'comment': 'Retain this flag', 'origin': 'manual'})
    assert flagged.status_code == 200, flagged.text
    loaded['dataset_id'] = flagged.json()['dataset']['dataset_id']
    body = remove_all(reviewer, base, loaded)
    _, annotations_path = get_dataset_artifact(study, loaded['dataset_id'], 'movement_review_annotations.json')
    annotations = json.loads(annotations_path.read_text())['annotations']
    assert annotations[0]['comment'] == 'Retain this flag'
    assert annotations[0]['status'] == 'suspected'
    assert reviewer.post(base + '/review/complete', json=body).status_code == 200
    assert len(downloaded_decisions(reviewer, base, loaded['dataset_id'])) == 2
    assert reviewer.post(base + '/actions/remove-study', json={**body, 'removal_reason': 'nest'}).status_code in {403, 409, 423}
    body = decision_body(editor, base, loaded, 'alpha')
    assigned = editor.post(base + '/review/assign', json={**body, 'reviewer_user_id': 'two'})
    assert assigned.status_code == 200, assigned.text
    profile = other.get(base + '/edit-profile', params={'dataset_id': loaded['dataset_id']}).json()
    assert profile['completed'] is False
    assert profile['coverage']['prior_decisions_by_individual']['alpha']['removal_reason'] == 'short_track'
    assert {row['review_decision'] for row in downloaded_decisions(other, base, loaded['dataset_id'])} == {'undecided'}
