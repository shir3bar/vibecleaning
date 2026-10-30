"""Study-wide, explicitly applied filter runs and portable native-AMT demos."""
import hashlib
import json
from pathlib import Path
import shutil

import pytest
from fastapi.testclient import TestClient

from app.auth import AuthManager
from app.edit_locks import create_guarded_step
from app.state import load_project_state, load_dataset, list_history
from examples.movement.study_filters import StudyFilters, STAGES, stage_of, scope, step_payload, gps_candidate
from examples.movement import study_filters
from examples.movement.routes import RDS_REVIEW_STEP_SCRIPT
from examples.movement.review_annotations import individual_review_decisions
from examples.rds_movement.app import create_rds_movement_app
from scripts.generate_filter_pipeline_demo import generate
from scripts.prefilter_movement_rds import prefilter_folder

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("incoming,outgoing,angle,expected", [
    (11, 11, 166, False), (11, 11, -166, False), (10, 11, 180, False),
    (11, 10, 180, False), (11, 11, 166.001, True), (11, 11, -180, True),
    (None, 11, 180, False), (11, None, 180, False), (11, 11, None, False),
    (float("inf"), 11, 180, False)])
def test_gps_requires_both_strict_speeds_and_strict_angle(incoming, outgoing, angle, expected):
    assert gps_candidate(incoming, outgoing, angle, 10) == expected


def test_interval_masks_preserve_overlaps_and_partial_unflags():
    from examples.movement.study_filters import merge, subtract
    original = {"a": [[1, 4], [7, 12]], "b": [[2, 2]]}
    assert subtract(original, {"a": [[2, 3], [8, 10]]}) == {"a": [[1, 1], [4, 4], [7, 7], [11, 12]], "b": [[2, 2]]}
    assert merge(original, {"a": [[3, 8]]}) == {"a": [[1, 12]], "b": [[2, 2]]}


@pytest.fixture(scope="module")
def prepared(tmp_path_factory):
    root = tmp_path_factory.mktemp("study-filters") / "demo"
    generate(root)
    return root


class Harness:
    def __init__(self, path):
        self.path = path

    @property
    def head(self):
        return load_project_state(self.path)["current_dataset_id"]

    def context(self):
        return StudyFilters(self.path, self.head, cache_root=self.path.parent.parent / "filter-test-cache")

    def append(self, records):
        context = self.context()
        preview = context.preview("speed")
        payload = step_payload(preview, user="test-reviewer", script=RDS_REVIEW_STEP_SCRIPT,
            source_artifacts=[a["logical_name"] for a in load_dataset(self.path, self.head)["artifacts"]
                              if a["logical_name"].endswith(".rds") or a["logical_name"] == "movement_review_annotations.json"])
        payload["parameters"] = {"records": records, "user": "test-reviewer", "dataset_id": self.head}
        payload["output_artifacts"] = ["movement_review_annotations.json"]
        payload["title"] = "Review decisions"
        result = create_guarded_step(self.path, payload, selected_dataset_id=self.head, expected_current_dataset_id=self.head)
        return result

    def apply(self, stage):
        context = self.context()
        preview = context.preview(stage)
        payload = step_payload(preview, user="test-reviewer", script=RDS_REVIEW_STEP_SCRIPT,
            source_artifacts=[a["logical_name"] for a in load_dataset(self.path, self.head)["artifacts"]
                              if a["logical_name"].endswith(".rds") or a["logical_name"] == "movement_review_annotations.json"])
        create_guarded_step(self.path, payload, selected_dataset_id=self.head, expected_current_dataset_id=self.head)
        return preview

    def unflag(self, stage):
        parents = [a for a in self.context().annotations if stage_of(a) == stage and a["status"] == "suspected"]
        self.append([{"annotation_kind": "dismissal", "status": "dismissed", "parent_annotation_id": a["annotation_id"],
                      "scope": scope(self.context().mapping(a)), "comment": "Human unflag"} for a in parents])

    def statuses(self):
        return {s["stage"]: s["status"] for s in self.context().status()["stages"]}


def copied(prepared, tmp_path, ident="910001"):
    path = tmp_path / "data" / "movement_rds" / ident
    shutil.copytree(prepared / "prefiltered" / "movement_rds" / ident, path)
    return Harness(path)


def test_baseline_protocol_and_source_checksums(prepared):
    expected = json.loads((prepared / "expected.json").read_text())
    for ident in ("910001", "910002", "910003"):
        h = Harness(prepared / "prefiltered" / "movement_rds" / ident)
        assert set(h.statuses().values()) == {"current"}
        assert len(list_history(h.path)["steps"]) == 3
        for source in h.path.glob("*.rds"):
            assert hashlib.sha256(source.read_bytes()).hexdigest() == expected["source_checksums"][f"{ident}/{source.name}"]
    a = Harness(prepared / "prefiltered/movement_rds/910001").context()
    assert a.saved("stationarity")["candidate_count"] == 80
    assert a.saved("speed")["statistics"]["threshold_mps"] == pytest.approx(152.75)
    b = Harness(prepared / "prefiltered/movement_rds/910002").context()
    assert b.saved("gps_spike")["statistics"]["threshold_mps"] == pytest.approx(78.46)


def test_stationarity_unflag_changes_only_explicitly_recalculated_filter(prepared, tmp_path):
    h = copied(prepared, tmp_path)
    gps_before = h.context().saved("gps_spike")
    h.unflag("stationarity")
    assert h.statuses() == {"stationarity": "current", "speed": "outdated", "gps_spike": "outdated"}
    head = h.head
    preview = h.context().preview("speed")
    assert h.head == head
    assert preview["removed_count"] == 1 and preview["added_count"] == 0
    assert preview["snapshot"]["statistics"]["threshold_mps"] == pytest.approx(202)
    nsteps = len(list_history(h.path)["steps"])
    h.apply("speed")
    assert len(list_history(h.path)["steps"]) == nsteps + 1
    assert h.context().saved("gps_spike") == gps_before
    assert h.statuses()["gps_spike"] == "outdated"
    assert h.apply("gps_spike")["added_count"] == 1
    assert h.context().preview("stationarity")["added_count"] == 0
    assert set(h.statuses().values()) == {"current"}


def test_speed_unflag_changes_study_gps_population_and_preserves_decisions(prepared, tmp_path):
    h = copied(prepared, tmp_path, "910002")
    h.unflag("speed")
    assert h.statuses() == {"stationarity": "current", "speed": "current", "gps_spike": "outdated"}
    preview = h.context().preview("gps_spike")
    assert preview["snapshot"]["statistics"]["threshold_mps"] == pytest.approx(972.4)
    assert (preview["added_count"], preview["removed_count"]) == (1, 1)
    assert h.context().preview("speed")["added_count"] == 0
    h.append([{"annotation_kind": "individual_review", "reviewed": True, "review_decision": "ok",
               "scope": {"kind": "individual", "individual": "Extreme_speed"}}])
    h.apply("gps_spike")
    decisions = individual_review_decisions(h.context().annotations, source_artifact="")
    assert decisions["Extreme_speed"]["review_decision"] == "ok"
    assert decisions["Extreme_speed"]["filter_review_pending"] is True
    h.unflag("gps_spike")
    assert not individual_review_decisions(h.context().annotations, source_artifact="")["Extreme_speed"]["needs_check"]


def test_manual_flags_overlap_and_net_changes(prepared, tmp_path):
    h = copied(prepared, tmp_path)
    mapping = {"910001_2.rds": [[10, 10]]}
    h.append([{"status": "suspected", "origin": "manual", "issue_type": "Manual inspection", "scope": scope(mapping)} for _ in range(2)])
    assert set(h.statuses().values()) == {"outdated"}
    parents = h.context().annotations[-2:]
    baseline = [s["changed_fix_count"] for s in h.context().status()["stages"]]
    h.append([{"status": "dismissed", "parent_annotation_id": parents[0]["annotation_id"], "scope": scope(mapping)}])
    assert baseline == [s["changed_fix_count"] for s in h.context().status()["stages"]]
    h.append([{"status": "dismissed", "parent_annotation_id": parents[1]["annotation_id"], "scope": scope(mapping)}])
    assert set(h.statuses().values()) == {"current"}


def test_relocation_resume_dry_run_and_cache_rebuild(prepared, tmp_path):
    h = copied(prepared, tmp_path / "Moved café folder", "910002")
    nsteps = len(list_history(h.path)["steps"])
    report = prefilter_folder(h.path.parent, user="test", refresh=True)
    assert not report["failures"]
    assert all(s["action"] == "skipped" for s in report["studies"][0]["filters"])
    assert len(list_history(h.path)["steps"]) == nsteps
    path = h.context().path
    path.unlink()
    assert set(h.statuses().values()) == {"current"}
    raw = tmp_path / "raw" / "movement_rds"
    shutil.copytree(prepared / "raw" / "movement_rds", raw)
    prefilter_folder(raw, dry_run=True)
    assert not list(raw.glob("*/scrubdata"))


def test_boundary_periods_invalid_barriers_and_empty_populations(prepared, tmp_path):
    h = copied(prepared, tmp_path, "910003")
    names = {s["logical_name"] for s in h.context().saved("stationarity")["candidate_scope"]["source_rows"]}
    assert names == {"910003_2.rds", "910003_3.rds"}
    # Keep only the all-invalid and one-fix tracks; there can be no speed.
    empty = tmp_path / "empty" / "movement_rds" / "910003"
    empty.mkdir(parents=True)
    for name in ("910003_6.rds", "910003_7.rds"):
        shutil.copyfile(h.path / name, empty / name)
    report = prefilter_folder(empty.parent)
    assert not report["failures"]
    assert Harness(empty).context().saved("speed")["statistics"]["threshold_mps"] is None
    assert Harness(empty).context().saved("gps_spike")["candidate_count"] == 0


def api_client(h, tmp_path):
    app = create_rds_movement_app(data_root=h.path.parent.parent, cache_root=tmp_path / "cache",
        static_root=ROOT / "examples/movement/static", index_path=ROOT / "examples/movement/static/index.html",
        auth_manager=AuthManager.for_testing(username="browser-reviewer", password="test-password-long", role="editor"))
    client = TestClient(app)
    assert client.post("/api/auth/login", json={"username": "browser-reviewer", "password": "test-password-long"}).status_code == 200
    return client, app


def test_api_preview_apply_no_automatic_cascade_and_stale_token(prepared, tmp_path):
    from test_movement_issue_groups import post_review
    h = copied(prepared, tmp_path)
    h.unflag("stationarity")
    client, _ = api_client(h, tmp_path)
    base = "/api/apps/movement/family/movement_rds/study/910001"
    body = {"dataset_id": h.head, "logical_name": "910001_1.rds", "stage": "speed", "background": False}
    head = h.head
    response = client.post(base + "/actions/preview-study-filter", json=body)
    assert response.status_code == 200, response.text
    assert h.head == head
    preview = response.json()
    body.update(analysis_id=preview["analysis_id"], preview_token="wrong", expected_current_dataset_id=head)
    assert post_review(client, base, "apply-study-filter", body).status_code == 400
    body["preview_token"] = preview["preview_token"]
    response = post_review(client, base, "apply-study-filter", body)
    assert response.status_code == 200, response.text
    assert h.head != head
    assert h.statuses()["gps_spike"] == "outdated"
    assert post_review(client, base, "apply-study-filter", body).status_code in {400, 409, 423}


def test_numeric_rds_filter_uses_recomputed_neighbors(prepared, tmp_path):
    from examples.movement.rds_index import resolve_rds_review_scope
    h = copied(prepared, tmp_path, "910002")
    context = h.context()
    spec = {"kind": "filter", "filter": {"field_key": "speed_mps", "field_kind": "numeric",
        "threshold_value": 1, "operator": "gt", "individuals": ["Extreme_speed"]}}
    assert resolve_rds_review_scope(context.path, spec)[1] == 1
    annotation = {"status": "confirmed", "scope": scope({"910002_3.rds": [[2, 2]]})}
    assert resolve_rds_review_scope(context.path, spec, annotations=[annotation])[1] == 0


def test_old_outgoing_speed_run_requires_optional_filter_specific_update(prepared, tmp_path, monkeypatch):
    """An upgrade preserves saved flags until Speed is explicitly reapplied."""
    h = copied(prepared, tmp_path, "910002")
    # Simulate a saved run of the former code in this temporary study.
    with monkeypatch.context() as old:
        old.setitem(study_filters.RULES, "speed", {"method": "Q3 + 100 * IQR",
            "comparison": "gt", "ownership": "outbound-start"})
        old.setitem(study_filters.STAGE_IMPLEMENTATION_VERSIONS, "speed", study_filters.IMPLEMENTATION_VERSION)
        old.setattr(study_filters, "speed_threshold_matches",
            lambda incoming, outgoing, threshold: outgoing is not None and outgoing > threshold)
        h.apply("speed")
        h.apply("gps_spike")
    head = h.head
    annotations = h.context().annotations
    assert h.statuses() == {"stationarity": "current", "speed": "outdated", "gps_spike": "current"}
    assert h.head == head and h.context().annotations == annotations
    preview = h.context().preview("speed")
    assert preview["removed_count"] == 1
    assert h.head == head and h.context().annotations == annotations
    h.apply("speed")
    assert h.head != head
    assert h.context().saved("speed")["rules"]["speed_basis"] == "both-adjacent"
    assert h.context().saved("speed")["candidate_count"] == 1
    assert h.statuses()["stationarity"] == "current"


def test_cached_metrics_reuse_and_corruption_recovery(prepared, tmp_path, monkeypatch):
    from examples.movement import study_filters
    h = copied(prepared, tmp_path, "910002")
    context = h.context()
    excluded = context.excluded("gps_spike")
    baseline = context.calculate("gps_spike", excluded)
    original = study_filters.compute_track_movement
    def unexpected(*args, **kwargs):
        raise AssertionError("Unchanged metrics should come from disposable cache")
    monkeypatch.setattr(study_filters, "compute_track_movement", unexpected)
    assert context.calculate("gps_spike", excluded) == baseline
    monkeypatch.setattr(study_filters, "compute_track_movement", original)
    for path in (context.path.parent / "study-filter-metrics").glob("*.npz"):
        path.write_bytes(b"broken cache")
    assert context.calculate("gps_spike", excluded) == baseline


def test_csv_protocol_matches_rds_study_quantiles(prepared, tmp_path):
    import csv
    from datetime import datetime, timezone
    from app.state import ensure_project_state
    from examples.movement.rds_index import read_movement_rds
    study = tmp_path / "csv_study"
    study.mkdir()
    source = study / "movement.csv"
    with source.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["eventid", "individual", "timestamp", "longitude", "latitude"])
        writer.writeheader()
        for path in sorted((prepared / "raw/movement_rds/910002").glob("*.rds")):
            for i, row in enumerate(read_movement_rds(path).to_dict("records")):
                writer.writerow({"eventid": f"{path.stem}-{i}", "individual": row["individual_local_identifier"],
                    "timestamp": datetime.fromtimestamp(row["t_"], timezone.utc).isoformat(), "longitude": row["x_"], "latitude": row["y_"]})
    state = ensure_project_state(study)
    for stage in STAGES:
        dataset_id = state["current_dataset_id"]
        context = StudyFilters(study, dataset_id, rds=False, logical_name="movement.csv")
        preview = context.preview(stage)
        names = ["movement.csv"] + (["movement_review_annotations.json"] if context.annotations else [])
        payload = step_payload(preview, user="csv-reviewer", script=RDS_REVIEW_STEP_SCRIPT, source_artifacts=names, logical_name="movement.csv")
        result = create_guarded_step(study, payload, selected_dataset_id=dataset_id, expected_current_dataset_id=dataset_id)
        state = {"current_dataset_id": result["dataset"]["dataset_id"]}
        if stage == "speed":
            assert preview["added_count"] == 1
            assert preview["snapshot"]["statistics"]["threshold_mps"] == pytest.approx(103)
        if stage == "gps_spike":
            assert preview["added_count"] == 1
            assert preview["snapshot"]["statistics"]["threshold_mps"] == pytest.approx(78.46)


def test_report_scope_remaps_filter_review_provenance(prepared, tmp_path):
    from examples.movement.rds_index import build_rds_report_inputs
    from examples.movement.review_annotations import normalize_annotation
    h = copied(prepared, tmp_path, "910002")
    h.unflag("speed")
    h.append([{"annotation_kind": "individual_review", "reviewed": True, "review_decision": "ok",
               "scope": {"kind": "individual", "individual": "Extreme_speed"}}])
    h.apply("gps_spike")
    h.unflag("gps_spike")
    context = h.context()
    _, annotation_bytes, _, _ = build_rds_report_inputs(context.path, annotations=context.annotations,
        fix_keys=[], issue_ids=[], individuals=["Extreme_speed"], snapshot_individuals=[], target_artifact="report.csv")
    converted = [normalize_annotation(a) for a in json.loads(annotation_bytes)["annotations"]]
    decision = individual_review_decisions(converted, source_artifact="report.csv")["Extreme_speed"]
    assert decision["needs_check"] is False


def test_csv_stationarity_endpoints_span_track_sets_and_invalid_positions_break_metrics(tmp_path):
    from app.state import ensure_project_state
    from datetime import datetime, timezone
    study = tmp_path / "csv"
    study.mkdir()
    path = study / "movement.csv"
    text = "eventid,individual,timestamp,longitude,latitude,set\n"
    values = [(0, .1, "train"), (60, .2, "train"), (120, .3, "train"),
              (10000, 0, "train"), (40000, 0, "train"), (60000, 0, "train"),
              (70000, .4, "test"), (80000, .5, "test"), (90000, .6, "test")]
    for i, (seconds, lon, group) in enumerate(values):
        text += f"{i},alpha,{datetime.fromtimestamp(1704067200 + seconds, timezone.utc).isoformat()},{lon},0,{group}\n"
    path.write_text(text, encoding="utf-8")
    state = ensure_project_state(study)
    context = StudyFilters(study, state["current_dataset_id"], rds=False, logical_name=path.name)
    assert context.preview("stationarity")["snapshot"]["candidate_count"] == 0
    # The CSV reader represents an invalid row with placeholder coordinates;
    # it must nevertheless be a barrier to canonical spatial metrics.
    context.csv_records[1]["invalid"] = True
    context.source_checksums[path.name] = "invalid-case-distinct-cache"
    rows, movement = next(context.metrics({}))
    assert movement[rows[0]["fix_key"]]["speed_mps"] is None
    assert movement[rows[1]["fix_key"]]["speed_mps"] is None


@pytest.mark.browser
def test_optional_filter_preview_cancel_apply_in_browser(prepared, tmp_path):
    from playwright.sync_api import sync_playwright, expect
    from test_movement_browser import _serve, _open_browser, _new_page
    h = copied(prepared, tmp_path)
    h.unflag("stationarity")
    _, app = api_client(h, tmp_path)
    with _serve(app) as url, sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser)
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto(url)
        page.locator("#login-username").fill("browser-reviewer")
        page.locator("#login-password").fill("test-password-long")
        page.locator("#login-submit").click()
        page.locator('[data-role="study"] option[value="910001"]').wait_for(state="attached", timeout=60000)
        page.locator('[data-role="study"]').select_option("910001")
        panel = page.locator('[data-study-filter-panel]')
        panel.locator(":scope > summary").click()
        speed = page.locator('[data-study-filter="speed"]')
        expect(speed).to_contain_text("Out of date", timeout=30000)
        head = h.head
        speed.get_by_role("button", name="Recalculate Speed", exact=True).click()
        expect(speed).to_contain_text("1 obsolete flags", timeout=60000)
        assert h.head == head
        speed.get_by_role("button", name="Cancel", exact=True).click()
        assert h.head == head
        speed.get_by_role("button", name="Recalculate Speed", exact=True).click()
        expect(speed).to_contain_text("1 obsolete flags", timeout=60000)
        speed.get_by_role("button", name="Apply Speed as new graph step", exact=True).click()
        expect(speed).to_contain_text("Up to date", timeout=60000)
        expect(page.locator('[data-study-filter="gps_spike"]')).to_contain_text("Out of date")
        assert h.head != head and not errors
        browser.close()
