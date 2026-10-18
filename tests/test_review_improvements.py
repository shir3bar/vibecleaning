import json

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
                decision=dict(individual=individual, review_decision='remove', needs_check=False,
                              comment='', **decision))


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
