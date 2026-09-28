"""Changed stationarity inputs require an explicit, reversible update."""

import hashlib

import pytest

from app.state import get_dataset_artifact, load_project_state
from examples.movement.review_annotations import load_review_annotations
from examples.movement.stationarity import stationary_fix_keys, stationarity_skips
from test_stationarity import SPEC, records
from test_movement_fixes import create_movement_test_client
from test_movement_issue_groups import CSV, post_review
from test_rds_movement import _client as rds_client


def test_skipped_spikes_bridge_only_allowed_gaps_and_keep_hard_boundaries():
    points = records([0, 0, 1, 0, 0])
    points[2]["excluded"] = True
    spec = {**SPEC, "maximum_gap_s": 120, "minimum_duration_s": 240}
    assert stationary_fix_keys(points, spec) == ["1", "2", "4", "5"]
    assert stationary_fix_keys(points, {**spec, "maximum_gap_s": 119}) == []
    for algorithm in ("anchor-radius-v1", "anchor-radius-v2"):
        assert stationary_fix_keys(points, {**spec, "algorithm": algorithm}) == []
    points[2]["segment"] = "different tag"
    assert stationary_fix_keys(points, spec) == []
    points[2].pop("segment")
    points[2]["invalid"] = True
    assert stationary_fix_keys(points, spec) == []
    # Excluded ends do not hide a stationary tail in the retained observations.
    points = records([1, 0, 0, 0, 1])
    points[0]["excluded"] = points[-1]["excluded"] = True
    assert stationary_fix_keys(points, SPEC) == ["2", "3", "4"]


def test_skip_mask_uses_effective_gps_allegations_and_other_exclusions():
    points = [{**p, "source_artifact": "track.csv", "individual": "alpha",
               "set_name": "train", "row_index": i + 1}
              for i, p in enumerate(records([0] * 5))]
    gps = {"annotation_id": "gps", "status": "suspected", "issue_type": "GPS spikes",
           "source_artifact": "track.csv", "scope": {"kind": "fix", "row_ranges": [[2, 3]]}}
    overlap = {**gps, "annotation_id": "overlap", "scope": {"kind": "fix", "row_ranges": [[3, 4]]}}
    dismissal = {**gps, "annotation_id": "dismiss", "parent_annotation_id": "gps", "status": "dismissed"}
    assert stationarity_skips(points, [gps, overlap, dismissal]) == {2, 3}
    assert stationarity_skips(points, [gps, {**dismissal, "status": "confirmed"}]) == {1, 2}
    assert stationarity_skips(points, [{**gps, "issue_type": "Stationarity"}]) == set()
    assert stationarity_skips(points, [{**gps, "issue_type": "Filter GPS spike (step + turn); Stationarity"}]) == {1, 2}
    assert stationarity_skips(points, [{**gps, "issue_type": "owner outlier"}]) == set()
    assert stationarity_skips(points, [{**gps, "source_artifact": "other.csv"}]) == set()
    assert stationarity_skips(points, [{**gps, "status": "confirmed", "issue_type": "Other"}]) == {1, 2}
    points[1]["source_annotation"] = {**gps, "annotation_id": "source:2",
                                      "scope": {"kind": "fix", "row_ranges": [[2, 2]]}}
    assert stationarity_skips(points, []) == {1}
    assert stationarity_skips(points, [{**dismissal, "parent_annotation_id": "source:2"}]) == set()


def test_unchanged_inputs_reuse_distance_scan_and_input_checks_do_not_scan(monkeypatch):
    from examples.movement import stationarity
    points = [{**p, "source_artifact": "cache-test.csv", "individual": "alpha",
               "set_name": "train", "row_index": i + 1}
              for i, p in enumerate(records([0] * 5))]
    gps = {"annotation_id": "gps", "status": "suspected", "issue_type": "GPS spikes",
           "scope": {"kind": "fix", "row_ranges": [[3, 3]]}}
    stationarity._SCAN_CACHE.clear()
    calls = []
    original = stationarity.stationary_fix_keys

    def counted(track, spec):
        calls.append(len(track))
        return original(track, spec)

    monkeypatch.setattr(stationarity, "stationary_fix_keys", counted)
    spec = {**SPEC, "maximum_gap_s": 120}
    first = stationarity.evaluate_stationarity(points, spec, [gps])
    assert len(calls) == 1
    confirmation = {**gps, "annotation_id": "confirmed", "parent_annotation_id": "gps", "status": "confirmed"}
    assert stationarity.evaluate_stationarity(points, spec, [gps, confirmation]) == first
    assert len(calls) == 1
    stationarity.evaluate_stationarity(points, spec, [], calculate=False)
    assert len(calls) == 1
    assert len(stationarity.evaluate_stationarity(points, spec, [])[0]) == 5
    assert len(calls) == 2
    points[-1]["lon"] = 1  # a change in source coordinates invalidates the cache too
    stationarity.evaluate_stationarity(points, spec, [])
    assert len(calls) == 3


def test_notice_ignores_edits_outside_examined_time_source_and_set(tmp_path):
    from examples.movement.stationarity import evaluate_stationarity, validate_stationarity_filter
    from examples.movement.stationarity_runs import run_contexts
    from test_stationarity import csv_content
    path = tmp_path / "track.csv"
    path.write_text(csv_content(), encoding="utf-8")
    from examples.movement.review_annotations import csv_stationarity_records
    points = csv_stationarity_records(path, SPEC)[:3]
    spec = {**SPEC, **validate_stationarity_filter(SPEC)}
    _, windows = evaluate_stationarity(points, spec, [])
    run = {"annotation_id": "run", "source_id": "unchanged", "source_artifact": path.name,
           "scope": {"filter": spec, "stationarity_inputs": windows}, "status": "suspected"}

    def stale(annotation):
        return run_contexts(tmp_path, [run, annotation], path=path, rds=False,
            logical_name=path.name, individual="alpha", source_signature="unchanged")[0]["stale"]

    gps = {"annotation_id": "gps", "status": "suspected", "issue_type": "GPS spikes",
           "scope": {"kind": "fix", "row_ranges": [[2, 2]]}}
    assert stale(gps)
    assert not stale({**gps, "scope": {"kind": "fix", "row_ranges": [[5, 5]]}})
    assert not stale({**gps, "source_artifact": "other.csv"})
    assert not stale({**gps, "scope": {"kind": "individual", "individual": "alpha", "set_name": "test"}})
    assert not stale({**gps, "scope": {"kind": "individual", "individual": "beta"}})


class Study:
    def __init__(self, client, study, base):
        self.client, self.study, self.base = client, study, base
        loaded = client.get(base + "/load").json()
        self.dataset, self.logical = loaded["dataset_id"], loaded["logical_name"]
        self.fixes = client.get(f"{base}/dataset/{self.dataset}/fixes", params={"logical_name": self.logical}).json()["fixes"]
        self.individuals = sorted({fix["individual"] for fix in self.fixes})
        self.keys = [[fix["fix_key"] for fix in self.fixes if fix["individual"] == individual]
                     for individual in self.individuals]

    def body(self, **extra):
        return {"dataset_id": self.dataset, "expected_current_dataset_id": self.dataset,
                "logical_name": self.logical, "user": "reviewer", "comment": "Check stationary periods", **extra}

    def mutate(self, action, **extra):
        response = post_review(self.client, self.base, action, self.body(**extra))
        assert response.status_code == 200, response.text
        result = response.json()
        self.dataset = result["dataset"]["dataset_id"]
        return result["step"]["summary"].get("annotation_id")

    def flag(self, keys, issue="GPS spikes"):
        return self.mutate("annotate-scope", scope={"kind": "fix", "fix_keys": keys},
                           status="suspected", origin="manual", issue_type=issue)

    def resolve(self, parent, keys, confirm=False):
        action, field = ("confirm-issues", "confirmations") if confirm else ("dismiss-issues", "dismissals")
        return self.mutate(action, **{field: [{"parent_annotation_id": parent, "fix_keys": keys}]})

    def save_run(self, **settings):
        spec = {**SPEC, "radius_m": 1e8, "minimum_duration_s": 1, "maximum_gap_s": 1e10,
                "position": "anywhere", "individuals": [], **settings}
        preview = self.client.post(self.base + "/actions/preview-filter", json=self.body(filter=spec))
        assert preview.status_code == 200, preview.text
        return self.mutate("annotate-scope", scope=preview.json()["resolved_scope"],
                           status="suspected", origin="threshold", issue_type="Stationarity")

    def groups(self, individual=None):
        response = self.client.get(f"{self.base}/dataset/{self.dataset}/issue-groups",
            params={"logical_name": self.logical, "individual": individual or self.individuals[0]})
        assert response.status_code == 200, response.text
        return response.json()

    def rerun(self, **extra):
        groups = self.groups()
        return post_review(self.client, self.base, "rerun-stationarity", self.body(
            individual=self.individuals[0], run_id=groups["stationarity_runs"][0]["run_id"],
            source_bundle_signature=groups["source_signature"], **extra))

    def apply(self):
        response = self.rerun()
        assert response.status_code == 200, response.text
        preview = response.json()
        result = self.rerun(apply=True, preview_token=preview["preview_token"])
        assert result.status_code == 200, result.text
        self.dataset = result.json()["dataset"]["dataset_id"]
        assert not self.groups()["stationarity_runs"][0]["stale"]
        return preview


@pytest.fixture(params=["csv", "rds"])
def study(tmp_path, request):
    if request.param == "csv":
        client, _ = create_movement_test_client(tmp_path, csv_content=CSV)
        path = tmp_path / "data/movement_clean/test_study"
        base = "/api/apps/movement/family/movement_clean/study/test_study"
    else:
        client, path = rds_client(tmp_path)
        base = "/api/apps/movement/family/movement_rds/study/268904527"
    return Study(client, path, base)


def test_rerun_checks_effective_inputs_per_individual_and_preserves_decisions(study):
    s = study
    raw = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in s.study.iterdir() if p.is_file()}
    gps = s.flag(s.keys[0][:1])
    overlap = s.flag(s.keys[0][:1])
    run = s.save_run()
    original_dataset = s.dataset
    assert not s.groups()["stationarity_runs"][0]["stale"]
    s.resolve(gps, s.keys[0][:1])
    assert not s.groups()["stationarity_runs"][0]["stale"]  # other allegation still skips it
    s.resolve(overlap, s.keys[0][:1], confirm=True)
    assert not s.groups()["stationarity_runs"][0]["stale"]  # already skipped
    s.resolve(run, s.keys[0][1:2], confirm=True)
    s.resolve(run, s.keys[0][2:3])
    assert not s.groups()["stationarity_runs"][0]["stale"]  # own human decisions do not invalidate
    new_gps = s.flag(s.keys[0][3:4])
    assert s.groups()["stationarity_runs"][0]["stale"]
    assert not s.groups(s.individuals[1])["stationarity_runs"][0]["stale"]
    head = load_project_state(s.study)["current_dataset_id"]
    preview = s.rerun().json()
    assert (preview["added_count"], preview["removed_count"]) == (0, 1)
    assert load_project_state(s.study)["current_dataset_id"] == head  # preview writes nothing
    assert s.rerun(apply=True, preview_token="old").status_code == 400
    s.apply()
    # Restoring the GPS point makes it a candidate again, including when its
    # previous stationarity flag was superseded automatically, not by a reviewer.
    s.resolve(new_gps, s.keys[0][3:4])
    assert s.groups()["stationarity_runs"][0]["stale"]
    preview = s.apply()
    assert (preview["added_count"], preview["removed_count"]) == (1, 0)
    assert preview["reviewed_difference_count"] >= 1  # explicitly unflagged stationarity stays unflagged
    _, original_sidecar = get_dataset_artifact(s.study, original_dataset, "movement_review_annotations.json")
    _, current_sidecar = get_dataset_artifact(s.study, s.dataset, "movement_review_annotations.json")
    earlier = load_review_annotations(original_sidecar)
    current = load_review_annotations(current_sidecar)
    assert current[:len(earlier)] == earlier
    assert len([item for item in current if item.get("parent_annotation_id") == run
                and item["status"] == "confirmed"]) == 1
    assert raw == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in s.study.iterdir() if p.is_file()}


def test_zero_candidate_run_records_examined_inputs_and_can_be_rerun(study):
    s = study
    gps = s.flag(s.keys[0])
    s.save_run(individuals=[s.individuals[0]])
    assert not s.groups()["groups"][-1]["issue_type"] == "Stationarity"
    s.resolve(gps, s.keys[0])
    assert s.groups()["stationarity_runs"][0]["stale"]
    preview = s.apply()
    assert preview["added_count"] == len(s.keys[0])
    gps = s.flag(s.keys[0])
    preview = s.apply()
    assert preview["match_count"] == 0 and preview["removed_count"] == len(s.keys[0])
    s.resolve(gps, s.keys[0])
    assert s.apply()["added_count"] == len(s.keys[0])


def test_saved_legacy_run_is_not_rewritten_and_explicit_rerun_records_new_rule(study):
    s = study
    s.flag(s.keys[0][:1])
    s.save_run(algorithm="anchor-radius-v2", individuals=[s.individuals[0]])
    old_dataset = s.dataset
    _, path = get_dataset_artifact(s.study, old_dataset, "movement_review_annotations.json")
    original = path.read_bytes()
    run = s.groups()["stationarity_runs"][0]
    assert run["stale"] and run["previous_algorithm"] == "anchor-radius-v2"
    assert s.apply()["removed_count"] == 1
    assert path.read_bytes() == original
    assert s.groups()["stationarity_runs"][0]["previous_algorithm"] == "anchor-radius-v3"


def test_rerun_apply_rejects_stale_head_and_keeps_current_history(study):
    s = study
    s.save_run()
    s.flag(s.keys[0][:1])
    preview = s.rerun().json()
    groups = s.groups()
    body = s.body(individual=s.individuals[0], run_id=preview["run_id"],
                  source_bundle_signature=groups["source_signature"],
                  apply=True, preview_token=preview["preview_token"])
    s.apply()
    current = load_project_state(s.study)["current_dataset_id"]
    response = post_review(s.client, s.base, "rerun-stationarity", body)
    assert response.status_code in {409, 423}, response.text
    assert load_project_state(s.study)["current_dataset_id"] == current


@pytest.mark.parametrize("study", ["rds"], indirect=True)
def test_rds_stationarity_uses_scoped_records_instead_of_whole_study_review_array(study, monkeypatch):
    from examples.movement import rds_index

    def unexpected(*args, **kwargs):
        raise AssertionError("Stationarity must not construct a whole-study review status array")

    monkeypatch.setattr(rds_index, "_index_review_status", unexpected)
    study.save_run(individuals=[study.individuals[0]])


@pytest.mark.browser
def test_rerun_notice_preview_cancel_and_update_in_browser(study):
    from test_movement_browser import _serve, _open_browser, _new_page, STATIC_ROOT, INDEX_PATH
    from app.auth import AuthManager
    from app.web import create_app
    from examples.movement import routes
    import playwright.sync_api as playwright_api
    s = study
    s.save_run()
    s.flag(s.keys[0][3:4])
    browser_app = s.client.app
    if s.study.parent.name == "movement_clean":
        browser_app = create_app(data_root=s.study.parents[1], static_root=STATIC_ROOT,
            index_path=INDEX_PATH, auth_manager=AuthManager.for_testing(
                username="rds-reviewer", password="test-password-long", role="editor"))
        routes.register_movement_routes(browser_app, data_root=s.study.parents[1])
    with _serve(browser_app) as url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 1000})
        errors, requests = [], []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("request", lambda request: requests.append(request.url)
                if any(endpoint in request.url for endpoint in ("/overview?", "/fixes-binary?", "/fixes?")) else None)
        page.goto(url, wait_until="domcontentloaded")
        page.locator("#login-username").fill("rds-reviewer")
        page.locator("#login-password").fill("test-password-long")
        page.locator("#login-submit").click()
        page.locator(f'[data-role="study"] option[value="{s.study.name}"]').wait_for(state="attached")
        page.locator('[data-role="study"]').select_option(s.study.name)
        page.locator('[data-individual-checkbox]').first.wait_for(state="attached")
        page.locator('[data-role="individual-view-queue"]').click()
        active = page.locator(f'[data-queue-individual="{s.individuals[0]}"].queue-active')
        notice = active.locator('[data-stationarity-rerun]')
        notice.wait_for(state="visible", timeout=30_000)
        original = load_project_state(s.study)["current_dataset_id"]
        for cancel in (True, False):
            notice.locator('[data-stationarity-run-action="preview"]').click()
            update = notice.locator('[data-stationarity-run-action="apply"]')
            update.wait_for(state="visible", timeout=30_000)
            assert "0 new flags, 1 obsolete flags" in notice.text_content()
            assert load_project_state(s.study)["current_dataset_id"] == original
            if cancel:
                notice.locator('[data-stationarity-run-action="cancel"]').click()
        requests.clear()
        update.click()
        notice.wait_for(state="detached", timeout=30_000)
        group = active.locator('[data-queue-issue-action="highlight"][data-issue-type="Stationarity"]')
        group.wait_for(state="visible", timeout=30_000)
        assert f"{len(s.keys[0]) - 1} fixes" in group.text_content()
        assert load_project_state(s.study)["current_dataset_id"] != original
        assert not requests, requests  # annotation-only updates retain the track
        assert not errors
        browser.close()
