import hashlib
import json

import pytest

from app.state import get_dataset_artifact
from examples.movement import routes
from test_movement_fixes import create_movement_test_client
from test_rds_movement import _client as rds_client


CSV = "eventid,individual,timestamp,longitude,latitude,set\n" + "".join(
    f"{individual}{index},{individual},2024-01-01T00:{index:02d}:00Z,-70,40,train\n"
    for individual in ("alpha", "beta") for index in range(8)
)


def post_review(client, base, action, body):
    profile = client.get(f"{base}/edit-profile", params={"dataset_id": body["dataset_id"]}).json()
    return client.post(f"{base}/actions/{action}", json={
        "expected_review_revision": profile.get("review_revision"), **body,
    })


@pytest.fixture(params=["csv", "rds"])
def reviewed_study(tmp_path, request):
    if request.param == "csv":
        client, _ = create_movement_test_client(tmp_path, csv_content=CSV)
        study = tmp_path / "data/movement_clean/test_study"
        base = "/api/apps/movement/family/movement_clean/study/test_study"
    else:
        client, study = rds_client(tmp_path)
        base = "/api/apps/movement/family/movement_rds/study/268904527"
    loaded = client.get(f"{base}/load").json()
    dataset = loaded["dataset_id"]
    logical = loaded["logical_name"]
    fixes = client.get(f"{base}/dataset/{dataset}/fixes", params={"logical_name": logical}).json()["fixes"]
    individuals = sorted({fix["individual"] for fix in fixes})
    alpha = [fix["fix_key"] for fix in fixes if fix["individual"] == individuals[0]][:6]
    beta = [fix["fix_key"] for fix in fixes if fix["individual"] == individuals[1]][:3]
    raw_hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                  for path in study.iterdir() if path.is_file()}
    parents = []
    for issue_type, keys in [("GPS spikes", alpha[:5] + beta), ("GPS spikes", alpha[2:5]),
                             ("Stationarity", alpha[3:6])]:
        response = post_review(client, base, "annotate-scope", {
            "dataset_id": dataset, "expected_current_dataset_id": dataset,
            "logical_name": logical, "scope": {"kind": "fix", "fix_keys": keys},
            "status": "suspected", "origin": "manual", "issue_type": issue_type,
            "comment": "Saved candidate group", "user": "reviewer",
        })
        assert response.status_code == 200, response.text
        result = response.json()
        dataset = result["dataset"]["dataset_id"]
        parents.append(result["step"]["summary"]["annotation_id"])
    return client, study, base, dataset, logical, individuals, alpha, beta, parents, raw_hashes


def test_queue_group_resolves_exact_individual_and_preserves_overlap(reviewed_study, monkeypatch):
    client, study, base, dataset, logical, individuals, alpha, beta, parents, raw_hashes = reviewed_study
    # The normal point request may be capped; groups must still cover every fix.
    monkeypatch.setattr(routes, "DEFAULT_FIX_LIMIT", 1)

    def groups(dataset_id, individual):
        response = client.get(f"{base}/dataset/{dataset_id}/issue-groups",
                              params={"logical_name": logical, "individual": individual})
        assert response.status_code == 200, response.text
        return response.json()

    initial = groups(dataset, individuals[0])
    assert {g["issue_type"]: g["fix_count"] for g in initial["groups"]} == {"GPS spikes": 5, "Stationarity": 3}
    spikes = initial["groups"][0]
    assert set(spikes["fix_keys"]) == set(alpha[:5])
    assert all(fix["individual"] == individuals[0] for fix in initial["fixes"])
    before_beta = groups(dataset, individuals[1])["groups"]
    body = {
        "dataset_id": dataset, "expected_current_dataset_id": dataset,
        "logical_name": logical, "source_bundle_signature": initial["source_signature"],
        "issue_group": {"individual": individuals[0], "issue_type": "GPS spikes", "expected_fix_count": 5},
        "note": "Obvious errors reviewed together", "user": "reviewer",
    }
    wrong_count = {**body, "issue_group": {**body["issue_group"], "expected_fix_count": 4}}
    assert post_review(client, base, "confirm-issues", wrong_count).status_code == 400
    wrong_source = {**body, "source_bundle_signature": "outdated"}
    assert post_review(client, base, "confirm-issues", wrong_source).status_code == 400
    response = post_review(client, base, "confirm-issues", body)
    assert response.status_code == 200, response.text
    result = response.json()
    confirmed = result["dataset"]["dataset_id"]
    assert result["step"]["title"].startswith("Confirm 5 suspected")
    summary = result["step"]["summary"]
    assert summary.get("confirmed_fix_count", summary.get("resolved_fix_count")) == 5
    assert groups(confirmed, individuals[1])["groups"] == before_beta
    remaining = groups(confirmed, individuals[0])
    assert [(g["issue_type"], g["fix_count"]) for g in remaining["groups"]] == [("Stationarity", 3)]
    # An overlap remains resolvable under its other originating issue.
    response = post_review(client, base, "dismiss-issues", {
        **body, "dataset_id": confirmed, "expected_current_dataset_id": confirmed,
        "issue_group": {"individual": individuals[0], "issue_type": "Stationarity", "expected_fix_count": 3},
    })
    assert response.status_code == 200, response.text
    dismissed = response.json()["dataset"]["dataset_id"]
    assert not groups(dismissed, individuals[0])["groups"]
    assert groups(dismissed, individuals[1])["groups"] == before_beta
    # Earlier versions retain their unresolved candidates.
    assert groups(dataset, individuals[0])["groups"] == initial["groups"]
    _, sidecar = get_dataset_artifact(study, dismissed, "movement_review_annotations.json")
    annotations = json.loads(sidecar.read_text())["annotations"]
    confirmations = [item for item in annotations if item["status"] == "confirmed"]
    assert {item["parent_annotation_id"] for item in confirmations} == set(parents[:2])
    assert all(item["comment"] == body["note"] for item in confirmations)
    fixes = client.get(f"{base}/dataset/{dismissed}/fixes", params={
        "logical_name": logical, "review_status": "reviewed", "limit": 1000,
    }).json()["fixes"]
    statuses = {fix["fix_key"]: fix["review"]["status"] for fix in fixes}
    assert all(statuses[key] == "confirmed" for key in alpha[:5])
    assert all(statuses[key] == "suspected" for key in beta)
    assert post_review(client, base, "confirm-issues", body).status_code in {409, 423}
    assert raw_hashes == {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in study.iterdir() if path.is_file()}


def test_queue_buttons_resolve_groups_without_point_selection(reviewed_study):
    from test_movement_browser import _serve, _open_browser, _wait_for_layer, STATIC_ROOT, INDEX_PATH
    from app.auth import AuthManager
    from app.web import create_app
    playwright_api = pytest.importorskip("playwright.sync_api")
    client, study, base, dataset, logical, individuals, *_ = reviewed_study
    browser_app = client.app
    if study.parent.name == "movement_clean":
        browser_app = create_app(data_root=study.parents[1], static_root=STATIC_ROOT,
            index_path=INDEX_PATH, auth_manager=AuthManager.for_testing(
                username="rds-reviewer", password="test-password-long", role="editor"))
        routes.register_movement_routes(browser_app, data_root=study.parents[1])
    with _serve(browser_app) as url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        errors = []
        track_requests = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("request", lambda request: track_requests.append(request.url)
                if any(endpoint in request.url for endpoint in
                       ("/overview?", "/fixes-binary?", "/fixes?")) else None)
        page.goto(url, wait_until="domcontentloaded")
        if page.locator("#login-username").count():
            page.locator("#login-username").fill("rds-reviewer")
            page.locator("#login-password").fill("test-password-long")
            page.locator("#login-submit").click()
        page.locator(".movement-root").wait_for(state="visible", timeout=20_000)
        page.locator(f'[data-role="study"] option[value="{study.name}"]').wait_for(state="attached")
        page.locator('[data-role="study"]').select_option(study.name)
        page.locator('[data-individual-checkbox]').first.wait_for(state="attached")
        page.evaluate("""() => {
          const original = deck.MapboxOverlay.prototype.setProps;
          deck.MapboxOverlay.prototype.setProps = function(props) {
            if (props.layers) window.__testMapLayers = props.layers;
            if (window.__testObserveReview && props.layers && !props.layers.some(layer =>
                layer.id.startsWith('movement-binary-points-') && layer.props.visible)) {
              window.__testReviewBlankFrames++;
            }
            return original.call(this, props);
          };
        }""")
        page.locator('[data-role="individual-view-queue"]').click()
        active = page.locator(f'[data-queue-individual="{individuals[0]}"].queue-active')
        active.wait_for(state="visible")
        spike = '[data-issue-type="GPS spikes"]'
        stationary = '[data-issue-type="Stationarity"]'
        highlight = active.locator(f'{spike}[data-queue-issue-action="highlight"]')
        highlight.wait_for(state="visible", timeout=30_000)
        assert "5 fixes" in highlight.text_content()
        assert page.locator('[data-queue-issues]').count() == 1
        bursts = active.locator('[data-queue-bursts]')
        assert not bursts.evaluate("element => element.open")
        bursts.locator('summary').click()
        bursts.locator('[data-queue-burst-visible]').first.wait_for(state="visible")
        page.wait_for_function("""() => (window.__testMapLayers || []).some(layer =>
          layer.id.startsWith('movement-binary-points-') && layer.props.visible)
        """, timeout=30_000)
        highlight.click()
        _wait_for_layer(page, "movement-queue-issue-highlight")
        assert highlight.get_attribute("aria-pressed") == "true"
        # Re-rendering the card must leave an explicitly opened menu open.
        assert bursts.evaluate("element => element.open")
        assert page.evaluate("""() => {
          const layers = window.__testMapLayers;
          const highlight = layers.at(-1);
          if (highlight.id !== 'movement-queue-issue-highlight'
              || highlight.props.data.length !== 5 || !highlight.props.filled
              || highlight.props.parameters.depthTest !== false
              || String(highlight.props.getFillColor) !== '246,92,110,255') return false;
          return layers.slice(0, -1).every(layer =>
            ['getColor', 'getFillColor', 'getLineColor'].every(name =>
                  layer.props[name] === undefined || (
                !layer.props.data?.attributes?.[name]
                && String(layer.props[name].slice(0, 3)) === '112,122,133'
              )
            )
          ) && layers.some(layer => layer.id.startsWith('movement-binary-points-')
            && layer.props.visible && layer.props.data.attributes.getFilterValue);
        }""")
        # Switching issue types shows the exact new group, including overlaps.
        stationary_highlight = active.locator(f'{stationary}[data-queue-issue-action="highlight"]')
        stationary_highlight.click()
        assert highlight.get_attribute("aria-pressed") == "false"
        assert page.evaluate("() => window.__testMapLayers.at(-1).props.data.length") == 3
        stationary_highlight.click()
        assert page.evaluate("""() => {
          const layers = window.__testMapLayers;
          return !layers.some(layer => layer.id === 'movement-queue-issue-highlight')
            && layers.some(layer => layer.id.startsWith('movement-binary-points-')
              && layer.props.visible && layer.props.data.attributes.getFillColor);
        }""")
        highlight.click()
        bursts.locator('summary').click()
        page.screenshot(path=f"/tmp/vibecleaning-queue-groups-active-{study.parent.name}.png")
        # An unsaved individual decision must survive these separate review steps.
        active.locator('[data-review-decision="fix_keep"]').click()
        is_rds = study.parent.name == "movement_rds"
        if is_rds:
            page.locator('[data-role="slider"]').evaluate("""element => {
              element.value = '3';
              element.dispatchEvent(new Event('input', {bubbles: true}));
            }""")
            page.wait_for_function("() => window.__movementDiagnosticsSnapshot().trackPlayerIndex === 3")
            before_review = page.evaluate("() => window.__movementDiagnosticsSnapshot()")
            before_track_requests = len(track_requests)
            page.evaluate("""() => {
              window.__testReviewBinary = window.__testMapLayers.find(layer =>
                layer.id.startsWith('movement-binary-points-') && layer.props.visible
              ).props.userData.binaryBlock;
              window.__testReviewCanvas = document.querySelector('[data-role=map] canvas');
              window.__testReviewBlankFrames = 0;
              window.__testObserveReview = true;
            }""")

        def assert_retained_track(suspected_count):
            if not is_rds:
                return
            after = page.evaluate("() => window.__movementDiagnosticsSnapshot()")
            assert len(track_requests) == before_track_requests, track_requests[before_track_requests:]
            for key in ("trackPlayerIndex", "trackPlayerTimeMs", "trackPlayerSourceRow",
                        "trackPlayerFixCount", "activeIndividual", "binaryBlockCount"):
                assert after[key] == before_review[key], key
            assert after["mapView"]["center"] == pytest.approx(before_review["mapView"]["center"])
            assert after["mapView"]["zoom"] == pytest.approx(before_review["mapView"]["zoom"])
            assert page.evaluate("""suspected => {
              const layer = window.__testMapLayers.find(layer =>
                layer.id.startsWith('movement-binary-points-') && layer.props.visible);
              const binary = layer?.props.userData.binaryBlock;
              const statuses = Array.from(binary?.arrays.review_status || []);
              return binary === window.__testReviewBinary
                && document.querySelector('[data-role=map] canvas') === window.__testReviewCanvas
                && window.__testReviewBlankFrames === 0
                && statuses.filter(status => status === 2).length === 5
                && statuses.filter(status => status === 1).length === suspected;
            }""", suspected_count)

        with page.expect_response(lambda response: response.url.endswith("/actions/confirm-issues")) as saved:
            active.locator(f'{spike}[data-queue-issue-action="confirm"]').click()
        assert saved.value.status == 200, saved.value.text()
        assert saved.value.json()["step"]["parameters"]["issue_group"]["individual"] == individuals[0]
        active.locator(f'{spike}[data-queue-issue-action="confirm"]').wait_for(state="detached", timeout=30_000)
        stationary_button = active.locator(f'{stationary}[data-queue-issue-action="unflag"]')
        stationary_button.wait_for(state="visible", timeout=30_000)
        assert active.locator('[data-review-decision="fix_keep"]').get_attribute("class") == "is-selected"
        assert "unsaved" in active.locator('.movement-review-state').text_content()
        assert page.locator('[data-role="confirm-modal"]').is_hidden()
        assert_retained_track(1)
        with page.expect_response(lambda response: response.url.endswith("/actions/dismiss-issues")) as dismissed:
            stationary_button.click()
        assert dismissed.value.status == 200, dismissed.value.text()
        active.get_by_text("No unresolved flags.", exact=True).wait_for(timeout=30_000)
        assert_retained_track(0)
        page.evaluate("() => { window.__testObserveReview = false; }")
        page.screenshot(path=f"/tmp/vibecleaning-queue-groups-{study.parent.name}.png")
        # Existing Save decision still saves the individual decision, independently.
        with page.expect_response(lambda response: response.url.endswith("/actions/review-individual")) as decision:
            page.locator('[data-role="individual-queue-save"]').click()
        assert decision.value.status == 200, decision.value.text()
        other = page.locator(f'[data-queue-individual="{individuals[1]}"].queue-active')
        other.locator(f'{spike}[data-queue-issue-action="highlight"]').wait_for(state="visible", timeout=30_000)
        assert "3 fixes" in other.locator(f'{spike}[data-queue-issue-action="highlight"]').text_content()
        if is_rds:
            # Returning to cached detail must not revive dismissed flags or lose
            # confirmations in the table while the binary map remains correct.
            page.locator(f'[data-queue-individual="{individuals[0]}"]').click()
            active.get_by_text("No unresolved flags.", exact=True).wait_for(timeout=30_000)
            active.locator('[data-queue-table]').click()
            rows = page.locator('[data-role="table-wrap"] tr[data-fix-key]')
            rows.first.wait_for(state="visible")
            statuses = rows.evaluate_all("rows => rows.map(row => row.cells[3].textContent)")
            assert statuses.count("confirmed") == 5, statuses
            assert "suspected" not in statuses
        assert not errors, errors
        browser.close()


def test_group_confirmation_includes_more_than_five_thousand_fixes(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone
    count = 5300
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    content = "eventid,individual,timestamp,longitude,latitude,set\n" + "".join(
        f"a{index},alpha,{(start + timedelta(minutes=index)).isoformat()},-70,40,train\n"
        for index in range(count)
    )
    client, dataset = create_movement_test_client(tmp_path, csv_content=content)
    base = "/api/apps/movement/family/movement_clean/study/test_study"
    flagged = post_review(client, base, "annotate-scope", {
        "dataset_id": dataset, "logical_name": "movement.csv",
        "scope": {"kind": "individual", "individual": "alpha"},
        "status": "suspected", "origin": "manual", "issue_type": "Stationarity",
        "comment": "Review stationary tail", "user": "reviewer",
    })
    assert flagged.status_code == 200, flagged.text
    dataset = flagged.json()["dataset"]["dataset_id"]
    monkeypatch.setattr(routes, "DEFAULT_FIX_LIMIT", 150)
    payload = client.get(f"{base}/dataset/{dataset}/issue-groups", params={
        "logical_name": "movement.csv", "individual": "alpha",
    }).json()
    assert payload["groups"][0]["fix_count"] == count
    assert len(payload["fixes"]) == count
    confirmed = post_review(client, base, "confirm-issues", {
        "dataset_id": dataset, "expected_current_dataset_id": dataset,
        "logical_name": "movement.csv", "user": "reviewer",
        "issue_group": {"individual": "alpha", "issue_type": "Stationarity", "expected_fix_count": count},
    })
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["step"]["summary"]["confirmed_fix_count"] == count


def test_rds_source_flags_can_be_confirmed_as_a_group(tmp_path):
    from examples.movement.rds_export import write_reviewed_rds_python
    from examples.movement.rds_index import RDS_REVIEW_COLUMNS, read_movement_rds
    client, study = rds_client(tmp_path)
    source = sorted(study.glob("*.rds"))[0]
    count = len(read_movement_rds(source))
    columns = {column: [None] * count for column in RDS_REVIEW_COLUMNS}
    columns["outlier_status"][:2] = ["suspected"] * 2
    columns["outlier_issue_type"][:2] = ["Owner flag"] * 2
    replacement = tmp_path / "reviewed.rds"
    write_reviewed_rds_python(source, replacement, columns)
    replacement.replace(source)
    base = "/api/apps/movement/family/movement_rds/study/268904527"
    loaded = client.get(f"{base}/load").json()
    dataset, logical = loaded["dataset_id"], loaded["logical_name"]
    fixes = client.get(f"{base}/dataset/{dataset}/fixes", params={
        "logical_name": logical, "review_status": "suspected",
    }).json()["fixes"]
    individual = fixes[0]["individual"]
    group = client.get(f"{base}/dataset/{dataset}/issue-groups", params={
        "logical_name": logical, "individual": individual,
    }).json()["groups"][0]
    assert group["fix_count"] == 2
    response = post_review(client, base, "confirm-issues", {
        "dataset_id": dataset, "expected_current_dataset_id": dataset,
        "logical_name": logical, "user": "rds-reviewer",
        "issue_group": {"individual": individual, "issue_type": "Owner flag", "expected_fix_count": 2},
    })
    assert response.status_code == 200, response.text
    assert response.json()["step"]["summary"]["resolved_fix_count"] == 2
    confirmed = response.json()["dataset"]["dataset_id"]
    assert client.get(f"{base}/dataset/{confirmed}/issue-groups", params={
        "logical_name": logical, "individual": individual,
    }).json()["groups"] == []
