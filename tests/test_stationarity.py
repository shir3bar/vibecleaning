"""Stationarity is a reproducible review candidate, not an error classification."""

import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import sqlite3
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.state import get_dataset_artifact
from examples.movement.review_annotations import resolve_filter_row_ranges
from examples.movement.rds_index import resolve_rds_review_scope
from examples.movement.stationarity import ALGORITHM, evaluate_stationarity, stationary_fix_keys, validate_stationarity_filter
from test_movement_fixes import create_movement_test_client
from test_rds_movement import _client


SPEC = {
    "kind": "stationarity", "radius_m": 10, "minimum_duration_s": 120,
    "maximum_gap_s": 60, "minimum_fixes": 3, "position": "ends",
    "individuals": ["alpha"], "set_names": [],
}


def records(longitudes, times=None):
    return [{"fix_key": str(i + 1), "time_ms": t * 1000, "lon": lon, "lat": 0}
            for i, (lon, t) in enumerate(zip(longitudes, times or range(0, len(longitudes) * 60, 60)))]


def csv_content():
    points = records([0, 0, 0, .01, .02, .02, .02, .03, .04, .04, .04])
    epoch = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return "eventid,individual,timestamp,longitude,latitude,set\n" + "".join(
        f"fix_{p['fix_key']},alpha,{(epoch + timedelta(milliseconds=p['time_ms'])).isoformat()},{p['lon']},0,train\n"
        for p in points
    )


def test_stationarity_distinguishes_ends_and_interior_and_uses_elapsed_time():
    points = records([0, 0, 0, .01, .02, .02, .02, .03, .04, .04, .04])
    assert stationary_fix_keys(points, SPEC) == ["1", "2", "3", "9", "10", "11"]
    assert stationary_fix_keys(points, {**SPEC, "position": "anywhere"}) == ["1", "2", "3", "5", "6", "7", "9", "10", "11"]
    assert stationary_fix_keys(points, {**SPEC, "position": "middle"}) == ["5", "6", "7"]
    assert stationary_fix_keys(points, {**SPEC, "minimum_duration_s": 121}) == []
    assert stationary_fix_keys(records([0, 0], [0, 120]), {**SPEC, "maximum_gap_s": 120}) == []


def test_stationarity_labels_whole_period_touching_third_retained_endpoint():
    points = records([.1, .2, *([0] * 5), .3, .4, *([1] * 4), .5, .6, *([2] * 5), .7, .8])
    ends = [str(i) for i in [*range(3, 8), *range(16, 21)]]
    middle = [str(i) for i in range(10, 14)]
    assert stationary_fix_keys(points, SPEC) == ends
    assert stationary_fix_keys(points, {**SPEC, "position": "middle"}) == middle
    assert set(stationary_fix_keys(points, {**SPEC, "position": "anywhere"})) == set(ends + middle)
    # Explicit historical filters keep their original endpoint definition.
    for algorithm in ("anchor-radius-v1", "anchor-radius-v2", "anchor-radius-v3"):
        assert stationary_fix_keys(points, {**SPEC, "algorithm": algorithm}) == []


def test_stationarity_period_after_third_fix_and_before_last_three_is_middle():
    points = records([.1, .2, .3, *([0] * 6), .4, .5, .6])
    assert stationary_fix_keys(points, SPEC) == []
    assert stationary_fix_keys(points, {**SPEC, "position": "middle"}) == [str(i) for i in range(4, 10)]


def test_stationarity_endpoints_follow_remaining_fixes_after_gps_and_confirmations():
    points = records([.1, .2, .3, .4, .5, .6, *([0] * 5), .7, .8, .9])
    for i, point in enumerate(points):
        point.update(source_artifact="track.csv", individual="alpha", set_name="train", row_index=i + 1)
    assert evaluate_stationarity(points, SPEC, [])[0] == []
    gps = {"annotation_id": "gps", "status": "suspected", "issue_type": "GPS spikes",
           "scope": {"kind": "fix", "row_ranges": [[1, 4]]}}
    assert evaluate_stationarity(points, SPEC, [gps])[0] == [str(i) for i in range(7, 12)]
    assert evaluate_stationarity(points, {**SPEC, "position": "middle"}, [gps])[0] == []
    assert evaluate_stationarity(points, SPEC, [{**gps, "status": "confirmed", "issue_type": "Other"}])[0] == [str(i) for i in range(7, 12)]
    # A dismissed GPS flag brings the original first fixes back into the count.
    dismissed = {**gps, "annotation_id": "dismissal", "parent_annotation_id": "gps", "status": "dismissed"}
    assert evaluate_stationarity(points, SPEC, [gps, dismissed])[0] == []


def test_stationarity_endpoints_belong_to_individual_not_each_set():
    points = records([.1, .2, .3, *([0] * 4), .4, .5, .6])
    for i, point in enumerate(points):
        point.update(source_artifact="track.csv", individual="alpha", row_index=i + 1,
                     set_name="test" if 3 <= i < 7 else "train")
    assert evaluate_stationarity(points, SPEC, [])[0] == []
    assert evaluate_stationarity(points, {**SPEC, "position": "middle"}, [])[0] == ["4", "5", "6", "7"]
    # Endpoint changes in another set must invalidate the cached classification.
    gps = {"annotation_id": "gps", "status": "suspected", "issue_type": "GPS spikes",
           "scope": {"kind": "fix", "row_ranges": [[1, 1]]}}
    assert evaluate_stationarity(points, SPEC, [gps])[0] == ["4", "5", "6", "7"]


@pytest.mark.parametrize("barrier", ["gap", "duplicate", "segment", "excluded"])
def test_stationarity_never_joins_across_barriers(barrier):
    points = records([0] * 5)
    if barrier == "gap":
        for point in points[2:]:
            point["time_ms"] += 600_000
    elif barrier == "duplicate":
        points[2]["time_ms"] = points[1]["time_ms"]
    elif barrier == "segment":
        for point in points[2:]:
            point["segment"] = 2
    else:
        points[2]["excluded"] = True
    assert stationary_fix_keys(points, {**SPEC, "minimum_duration_s": 240}) == []


def test_stationarity_gap_setting_crosses_short_source_bursts_but_preserves_v1():
    # A daily recording schedule can create many short bursts at one location.
    points = records([0] * 7, [i * 24 * 3600 for i in range(7)])
    for i, point in enumerate(points):
        point["burst"] = i
    spec = {**SPEC, "minimum_duration_s": 48 * 3600, "maximum_gap_s": 100 * 3600}
    assert stationary_fix_keys(points, spec) == [str(i) for i in range(1, 8)]
    assert stationary_fix_keys(points, {**spec, "maximum_gap_s": 23 * 3600}) == []
    assert stationary_fix_keys(points, {**spec, "algorithm": "anchor-radius-v1"}) == []
    assert validate_stationarity_filter(spec)["algorithm"] == ALGORITHM
    assert validate_stationarity_filter({**spec, "algorithm": "anchor-radius-v1"})["algorithm"] == "anchor-radius-v1"


def test_csv_stationarity_respects_tag_changes_and_unparseable_times(tmp_path):
    path = tmp_path / "stationary.csv"
    header = "individual,timestamp,longitude,latitude,burst_,tag-local-identifier\n"
    rows = [f"alpha,2024-01-{day:02d}T00:00:00Z,1,1,{day},tag1\n" for day in range(1, 8)]
    spec = {**SPEC, "minimum_duration_s": 48 * 3600, "maximum_gap_s": 100 * 3600}
    path.write_text(header + "".join(rows), encoding="utf-8")
    assert resolve_filter_row_ranges(path, spec) == ([[1, 7]], 7)
    assert resolve_filter_row_ranges(path, {**spec, "algorithm": "anchor-radius-v1"}) == ([], 0)
    path.write_text(header + "".join(rows[:2] + [row.replace("tag1", "tag2") for row in rows[2:]]), encoding="utf-8")
    assert resolve_filter_row_ranges(path, spec) == ([[3, 7]], 5)
    # Missing time remains a barrier even though source burst IDs are ignored.
    path.write_text(header + "".join(rows[:2]) + "alpha,unknown,1,1,2,tag1\n" + "".join(rows[2:]), encoding="utf-8")
    assert resolve_filter_row_ranges(path, spec) == ([[4, 8]], 5)


def test_stationarity_bounds_total_extent_not_just_short_steps():
    # Each step is ~6 m; the animal traverses 30 m. A 10 m radius cannot
    # turn the full sequence into a five-minute stationary stay.
    assert stationary_fix_keys(records([i * .000054 for i in range(6)]), SPEC) == []
    # Geodesic distance remains local across the date line.
    assert stationary_fix_keys(records([179.99999, -179.99999, 179.99999]), SPEC) == ["1", "2", "3"]


def test_gap_edges_are_not_track_ends():
    points = records([.01, .02, .03, 0, 0, 0, .04, .05, .06], [0, 60, 120, 600, 660, 720, 1300, 1360, 1420])
    assert stationary_fix_keys(points, SPEC) == []
    assert stationary_fix_keys(points, {**SPEC, "position": "anywhere"}) == ["4", "5", "6"]


def test_csv_filter_tracks_source_rows_scopes_exclusions_and_unsorted_input(tmp_path):
    path = tmp_path / "track.csv"
    content = csv_content().splitlines()
    # Deliberately nonchronological source order; scope refers to original rows.
    path.write_text("\n".join([content[0], *reversed(content[1:])]) + "\n", encoding="utf-8")
    assert resolve_filter_row_ranges(path, SPEC) == ([[1, 3], [9, 11]], 6)
    assert resolve_filter_row_ranges(path, {**SPEC, "individuals": ["beta"]}) == ([], 0)
    assert resolve_filter_row_ranges(path, {**SPEC, "set_names": ["test"]}) == ([], 0)
    assert resolve_filter_row_ranges(path, SPEC, confirmed_fix_keys={"id:fix_2#row:10"}) == ([[1, 3]], 3)
    assert resolve_filter_row_ranges(path, SPEC, confirmed_individual_tracks={("alpha", "train")}) == ([], 0)


@pytest.mark.parametrize("key,value", [("radius_m", 0), ("maximum_gap_s", True), ("minimum_duration_s", float("nan")), ("minimum_fixes", 2), ("position", "nowhere"), ("algorithm", "future")])
def test_stationarity_rejects_invalid_settings(key, value):
    with pytest.raises(ValueError, match="[Ss]tationarity"):
        validate_stationarity_filter({**SPEC, key: value})


def test_csv_preview_and_saved_decision_agree_and_preserve_raw_data(tmp_path):
    content = csv_content()
    client, dataset = create_movement_test_client(tmp_path, csv_content=content)
    base = "/api/apps/movement/family/movement_clean/study/test_study"
    body = {"dataset_id": dataset, "logical_name": "movement.csv", "filter": SPEC}
    preview = client.post(base + "/actions/preview-filter", json=body)
    assert preview.status_code == 200, preview.text
    preview = preview.json()
    assert preview["match_count"] == 6
    scope = preview["resolved_scope"]
    assert scope["row_ranges"] == [[1, 3], [9, 11]]
    response = client.post(base + "/actions/annotate-scope", json={
        **body, "expected_current_dataset_id": dataset, "scope": scope,
        "status": "suspected", "issue_type": "Stationarity", "origin": "threshold",
        "comment": "Check deployment timing; could also be resting.", "user": "reviewer",
    })
    assert response.status_code == 200, response.text
    saved = response.json()
    study = tmp_path / "data" / "movement_clean" / "test_study"
    _, sidecar = get_dataset_artifact(study, saved["dataset"]["dataset_id"], "movement_review_annotations.json")
    annotation = json.loads(sidecar.read_text(encoding="utf-8"))["annotations"][0]
    assert annotation["scope"]["row_ranges"] == scope["row_ranges"]
    assert annotation["scope"]["filter"]["algorithm"] == ALGORITHM
    assert len(annotation["scope"]["filter"]["implementation_sha256"]) == 64
    assert annotation["status"] == "suspected"
    assert (study / "movement.csv").read_text(encoding="utf-8") == content
    # Trying a different threshold does not change the saved scope.
    client.post(base + "/actions/preview-filter", json={**body, "filter": {**SPEC, "minimum_duration_s": 900}})
    assert json.loads(sidecar.read_text(encoding="utf-8"))["annotations"][0] == annotation


def test_rds_preview_matches_csv_and_saved_source_scopes(tmp_path):
    client, study = _client(tmp_path)
    base = "/api/apps/movement/family/movement_rds/study/268904527"
    loaded = client.get(base + "/load").json()
    dataset, logical = loaded["dataset_id"], loaded["logical_name"]
    overview = client.get(base + f"/dataset/{dataset}/overview", params={"logical_name": logical}).json()
    individual = overview["individuals"][0]
    spec = {**SPEC, "radius_m": 500_000, "minimum_duration_s": 120,
            "maximum_gap_s": 7200, "position": "anywhere", "individuals": [individual]}
    body = {"dataset_id": dataset, "logical_name": logical, "filter": spec,
            "source_bundle_signature": overview["source_bundle_signature"]}
    response = client.post(base + "/actions/preview-filter", json=body)
    assert response.status_code == 200, response.text
    preview = response.json()
    assert preview["match_count"] > 0
    scope = preview["resolved_scope"]
    # Compare equivalent CSVs with the actual RDS cache's original row order,
    # identifiers and source burst boundaries, not a mocked evaluator.
    indexes = list((tmp_path / "cache").rglob("*.sqlite*"))
    index = next(path for path in indexes if path.is_file() and path.suffix in {".sqlite", ".sqlite3"})
    connection = sqlite3.connect(index)
    connection.row_factory = sqlite3.Row
    try:
        for source in scope["source_rows"]:
            rows = connection.execute(
                "SELECT f.*, i.identifier FROM fixes f JOIN artifacts a ON a.artifact_id=f.artifact_id JOIN individuals i ON i.individual_key=f.individual_key WHERE a.logical_name=? ORDER BY f.source_row",
                (source["logical_name"],),
            ).fetchall()
            path = tmp_path / "equivalent.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["individual", "timestamp", "longitude", "latitude", "burst_"])
                for row in rows:
                    writer.writerow([row["identifier"], datetime.fromtimestamp(row["time_ms"] / 1000, tz=timezone.utc).isoformat(), row["lon"], row["lat"], row["burst_value"]])
            ranges, count = resolve_filter_row_ranges(path, spec)
            assert ranges == source["row_ranges"]
    finally:
        connection.close()
    profile = client.get(base + "/edit-profile", params={"dataset_id": dataset}).json()
    response = client.post(base + "/actions/annotate-scope", json={
        **body, "expected_current_dataset_id": dataset, "expected_review_revision": profile["review_revision"],
        "scope": scope, "status": "suspected", "origin": "threshold", "issue_type": "Stationarity",
        "comment": "Inspect stationary periods.",
    })
    assert response.status_code == 200, response.text
    saved = response.json()
    assert saved["step"]["summary"]["resolved_fix_count"] == preview["match_count"]
    _, sidecar = get_dataset_artifact(study, saved["dataset"]["dataset_id"], "movement_review_annotations.json")
    annotation = json.loads(sidecar.read_text(encoding="utf-8"))["annotations"][0]
    assert annotation["scope"]["source_rows"] == scope["source_rows"]
    assert annotation["scope"]["filter"] == scope["filter"]
    excluded_scope, count = resolve_rds_review_scope(index, scope, annotations=[{**annotation, "status": "confirmed"}])
    assert count == 0
    assert excluded_scope["source_rows"] == []


@pytest.mark.browser
@pytest.mark.parametrize("source_format,select_all", [
    ("csv", False), ("rds", False),
    ("csv", True),
    ("car_talk", False),
])
def test_stationarity_color_column_settings_scope_and_save(tmp_path, source_format, select_all):
    import playwright.sync_api as playwright_api
    from test_movement_browser import _serve, _open_browser, _new_page, _login_and_wait, _auth_manager, STATIC_ROOT, INDEX_PATH
    from examples.slim_movement.app import create_slim_movement_app
    from examples.rds_movement.app import create_rds_movement_app

    is_rds = source_format in {"rds", "car_talk"}
    study = tmp_path / "data" / ("movement_rds" if is_rds else "movement_raw") / "stationary"
    study.mkdir(parents=True)
    duration = "48" if source_format == "car_talk" else "0.0333333333"
    if is_rds:
        filename = "481458_20761565.rds" if source_format == "car_talk" else "268904527_269302973.rds"
        source = STATIC_ROOT.parents[2] / "data" / "movement_rds" / filename
        if not source.exists():
            pytest.fail("RDS sample unavailable")
        shutil.copy2(source, study / source.name)
        app = create_rds_movement_app(data_root=tmp_path / "data", cache_root=tmp_path / "cache", static_root=STATIC_ROOT, index_path=INDEX_PATH, auth_manager=_auth_manager())
        individual, radius, gap, expected = "MF043", "500000", "2", 9
        if source_format == "car_talk":
            individual, radius, gap, expected = "Car Talk", "100", "100", 5285
    else:
        other_individual = csv_content().split("\n", 1)[1].replace(",alpha,", ",beta,").replace("fix_", "beta_")
        (study / "movement.csv").write_text(csv_content() + other_individual, encoding="utf-8")
        app = create_slim_movement_app(data_root=tmp_path / "data", static_root=STATIC_ROOT, index_path=INDEX_PATH, auth_manager=_auth_manager())
        individual, radius, gap, expected = "alpha", "10", "0.0166666667", 6
    with _serve(app) as url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 1000})
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        _login_and_wait(page, url, "stationary")
        alpha = page.locator(f'[data-individual-checkbox="{individual}"]')
        alpha.wait_for(state="attached", timeout=30_000)
        alpha.check()
        assert page.locator(".movement-stationarity").count() == 0
        page.locator('[data-role="color-by"]').select_option("stationarity")

        def set_setting(name, value):
            control = page.locator(f'[data-role="stationarity-{name}"]')
            control.fill(value)
            control.press("Tab")

        def wait_for_matches(count=expected):
            try:
                page.wait_for_function(
                    "expected => document.querySelector('[data-role=stationarity-status]')?.textContent.replaceAll(',', '').includes(`${expected} stationary candidate fixes`)",
                    arg=count, timeout=30_000,
                )
            except playwright_api.TimeoutError:
                pytest.fail(f"Stationarity did not reach {count} matches: " + str(page.locator('.movement-threshold').text_content()) + f"; errors: {errors}")

        set_setting("radius", radius)
        set_setting("duration", duration)
        set_setting("gap", gap)
        if source_format == "car_talk":
            page.locator('[data-role="stationarity-position"]').select_option("anywhere")
        wait_for_matches()
        # Filter scope follows the individual list, with no separate selector.
        assert page.locator('[data-action="set-threshold-flag-scope"]').count() == 0
        page.locator('[data-action="check-above-threshold"]').click()
        assert page.locator('[data-role="mark-suspected"]').is_enabled()
        # Changing a setting invalidates previously checked matches immediately.
        set_setting("duration", "100000")
        wait_for_matches(0)
        assert page.locator('[data-role="mark-suspected"]').is_enabled()
        assert page.locator('[data-role="mark-suspected"]').text_content() == "Save filter (0 flags)"
        assert page.locator('[data-action="check-above-threshold"]').is_disabled()
        set_setting("duration", duration)
        wait_for_matches()
        # Colour selection can be switched without retaining a stale match column.
        page.locator('[data-role="color-by"]').select_option("individual")
        assert page.locator('[data-role="stationarity-radius"]').count() == 0
        page.locator('[data-role="color-by"]').select_option("stationarity")
        wait_for_matches()
        # The same colour column also calculates when the review queue hides
        # the normal threshold controls.
        page.locator('[data-role="color-by"]').select_option("individual")
        page.locator('[data-role="individual-view-queue"]').click()
        page.wait_for_function("() => window.__movementDiagnosticsSnapshot().activeIndividual !== ''")
        with page.expect_response(lambda response: response.url.endswith("/actions/preview-filter")) as queue_preview:
            page.locator('[data-role="color-by"]').select_option("stationarity")
        assert queue_preview.value.status == 200
        assert queue_preview.value.json()["match_count"] == expected
        page.locator('[data-role="individual-view-browse"]').click()
        wait_for_matches()
        alpha.uncheck()
        assert page.locator('[data-role="mark-suspected"]').is_disabled()
        alpha.check()
        wait_for_matches()
        expected_flag_count = expected
        if select_all:
            page.locator('[data-role="select-all"]').click()
            expected_flag_count *= 2
            wait_for_matches(expected_flag_count)
        page.locator('[data-action="check-above-threshold"]').click()
        assert page.locator('.movement-threshold').evaluate("pane => pane.scrollWidth <= pane.clientWidth")
        page.screenshot(path=tmp_path / "stationarity.png")
        page.locator('[data-role="mark-suspected"]').click()
        page.locator('[data-role="issue-modal"]').wait_for(state="visible", timeout=30_000)
        expected_label = "Filter Stationarity" if source_format == "car_talk" else "Filter Stationarity (start/end)"
        assert page.locator('[data-role="issue-type"]').input_value() == expected_label
        assert f"Exact fixes to flag: {expected_flag_count}" in page.locator('[data-role="issue-meta"]').text_content().replace(",", "")
        with page.expect_response(lambda response: response.url.endswith("/actions/annotate-scope"), timeout=30_000) as saved:
            page.locator('[data-role="issue-submit"]').click()
        assert saved.value.status == 200, saved.value.text()
        payload = saved.value.json()
        assert payload["step"]["summary"]["resolved_fix_count"] == expected_flag_count
        saved_filter = payload["step"]["parameters"]["scope"]["filter"]
        assert saved_filter["kind"] == "stationarity"
        assert saved_filter["algorithm"] == ALGORITHM
        assert saved_filter["endpoint_rule"] == "any-first-or-last-3-retained-fixes-per-individual"
        assert saved_filter["individuals"] == (["alpha", "beta"] if select_all else [individual])
        assert saved_filter["radius_m"] == float(radius)
        assert saved_filter["minimum_duration_s"] == pytest.approx(float(duration) * 3600)
        assert saved_filter["maximum_gap_s"] == pytest.approx(float(gap) * 3600)
        page.locator('[data-role="issue-modal"]').wait_for(state="hidden", timeout=30_000)
        if source_format != "car_talk":
            # Save middle periods separately; suspected end flags must remain
            # independent, with their own label and original source-row scope.
            page.wait_for_function("document.querySelector('[data-role=dataset]').value === "
                                   + json.dumps(payload["dataset"]["dataset_id"]))
            page.locator('[data-role="stationarity-position"]').select_option("middle")
            middle_count = 0 if is_rds else 3 * (2 if select_all else 1)
            wait_for_matches(middle_count)
            page.locator('[data-role="mark-suspected"]').click()
            page.locator('[data-role="issue-modal"]').wait_for(state="visible")
            assert page.locator('[data-role="issue-type"]').input_value() == "Filter Stationarity (middle)"
            with page.expect_response(lambda response: response.url.endswith("/actions/annotate-scope")) as middle_saved:
                page.locator('[data-role="issue-submit"]').click()
            assert middle_saved.value.status == 200, middle_saved.value.text()
            middle_payload = middle_saved.value.json()
            assert middle_payload["step"]["summary"]["resolved_fix_count"] == middle_count
            assert middle_payload["step"]["parameters"]["scope"]["filter"]["position"] == "middle"
            _, sidecar = get_dataset_artifact(study, middle_payload["dataset"]["dataset_id"],
                                             "movement_review_annotations.json")
            annotations = json.loads(sidecar.read_text(encoding="utf-8"))["annotations"]
            assert [item["issue_type"] for item in annotations] == [
                "Filter Stationarity (start/end)", "Filter Stationarity (middle)"]
            if not is_rds:
                assert annotations[0]["scope"]["row_ranges"] == (
                    [[1, 3], [9, 14], [20, 22]] if select_all else [[1, 3], [9, 11]])
                assert annotations[1]["scope"]["row_ranges"] == (
                    [[5, 7], [16, 18]] if select_all else [[5, 7]])
            page.locator('[data-role="issue-modal"]').wait_for(state="hidden", timeout=30_000)
        assert not errors, errors
        browser.close()


@pytest.mark.browser
@pytest.mark.parametrize("source_format", ["csv", "rds"])
@pytest.mark.parametrize("overlap_column_update", [False, True])
def test_stationarity_highlights_survive_all_individuals_view(tmp_path, source_format, overlap_column_update):
    import asyncio
    import playwright.sync_api as playwright_api
    from test_movement_browser import _serve, _open_browser, _new_page, _login_and_wait, _auth_manager, STATIC_ROOT, INDEX_PATH
    from examples.slim_movement.app import create_slim_movement_app
    from examples.rds_movement.app import create_rds_movement_app

    study = tmp_path / "data" / ("movement_rds" if source_format == "rds" else "movement_raw") / "stationary"
    study.mkdir(parents=True)
    if source_format == "rds":
        for filename in ["268904527_269302973.rds", "268904527_269302895.rds"]:
            source = STATIC_ROOT.parents[2] / "data" / "movement_rds" / filename
            if not source.exists():
                pytest.fail("RDS samples unavailable")
            shutil.copy2(source, study / filename)
        app = create_rds_movement_app(data_root=tmp_path / "data", cache_root=tmp_path / "cache", static_root=STATIC_ROOT, index_path=INDEX_PATH, auth_manager=_auth_manager())
        individual, radius, gap, single_count = "MF043", "500000", "2", 9
    else:
        other = csv_content().split("\n", 1)[1].replace(",alpha,", ",beta,").replace("fix_", "beta_")
        (study / "movement.csv").write_text(csv_content() + other, encoding="utf-8")
        app = create_slim_movement_app(data_root=tmp_path / "data", static_root=STATIC_ROOT, index_path=INDEX_PATH, auth_manager=_auth_manager())
        individual, radius, gap, single_count = "alpha", "10", "0.0166666667", 6

    @app.middleware("http")
    async def delay_full_map(request, call_next):
        if request.url.path.endswith("/fixes-binary") and not request.query_params.getlist("individuals"):
            # Stationarity can finish before the larger all-individuals map arrives.
            await asyncio.sleep(0.8)
        return await call_next(request)

    with _serve(app) as url, playwright_api.sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser, viewport={"width": 1440, "height": 1000})
        page.add_init_script("""(() => {
          const original = Worker.prototype.addEventListener;
          Worker.prototype.addEventListener = function(type, listener, options) {
            if (type !== 'message') return original.call(this, type, listener, options);
            return original.call(this, type, function(event) {
              if (window.__delayStationarityAck && event.data?.type === 'stationarity') {
                setTimeout(() => listener.call(this, event), 1000);
              } else listener.call(this, event);
            }, options);
          };
        })()""")
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        _login_and_wait(page, url, "stationary")
        page.wait_for_function("() => window.__movementDiagnosticsSnapshot().mapReady")
        # Inspect the colour buffers actually handed to Deck, not just the
        # filter count or the underlying boolean column.
        page.evaluate("""() => {
          const original = deck.MapboxOverlay.prototype.setProps;
          deck.MapboxOverlay.prototype.setProps = function(props) {
            if (props.layers) window.__testMapLayers = props.layers;
            return original.call(this, props);
          };
          window.__testRedFixCount = () => {
            let count = 0;
            for (const layer of window.__testMapLayers || []) {
              if (!layer.id.startsWith('movement-binary-points-') || layer.props.visible === false) continue;
              const attributes = layer.props.data.attributes;
              const colors = attributes.getFillColor?.value || [];
              const visible = attributes.getFilterValue?.value || [];
              for (let i = 0; i < visible.length; i++) {
                if (visible[i] && colors[i*4] === 246 && colors[i*4+1] === 92 && colors[i*4+2] === 110) count++;
              }
            }
            return count;
          };
        }""")
        page.locator(f'[data-individual-checkbox="{individual}"]').check()
        page.locator('[data-role="color-by"]').select_option("stationarity")
        for name, value in [("radius", radius), ("duration", "0.0333333333"), ("gap", gap)]:
            control = page.locator(f'[data-role="stationarity-{name}"]')
            control.fill(value)
            control.press("Tab")
        try:
            page.wait_for_function("n => window.__testRedFixCount() === n", arg=single_count, timeout=30_000)
        except Exception:
            print(page.evaluate("""() => ({
              status: document.querySelector('[data-role=stationarity-status]')?.textContent,
              settings: [...document.querySelectorAll('[data-stationarity-setting]')].map(e => [e.dataset.stationaritySetting, e.value]),
              redCount: window.__testRedFixCount(), diagnostics: window.__movementDiagnosticsSnapshot(),
            })"""))
            print(errors)
            raise
        if source_format == "csv" and overlap_column_update:
            # Finish a background preview while an unchanged field is focused.
            # It must not replace that input between focus and the first key.
            held = []
            page.route("**/actions/preview-filter", lambda route: held.append(route))
            with page.expect_request(lambda request: request.url.endswith("/actions/preview-filter")):
                control = page.locator('[data-role="stationarity-radius"]')
                control.fill(str(float(radius) + 1))
                control.press("Tab")
            duration_control = page.locator('[data-role="stationarity-duration"]')
            duration_control.focus()
            duration_control.evaluate("element => window.__focusedStationarityInput = element")
            assert held
            held.pop().continue_()
            page.wait_for_function("n => window.__testRedFixCount() === n", arg=single_count, timeout=30_000)
            assert duration_control.evaluate("element => element === window.__focusedStationarityInput && element === document.activeElement")
            duration_control.press("Tab")
            page.unroute("**/actions/preview-filter")
        page.evaluate("value => window.__delayStationarityAck = value", overlap_column_update)
        with page.expect_response(lambda response: response.url.endswith("/actions/preview-filter")) as preview:
            page.locator('[data-role="select-all"]').click()
        all_count = preview.value.json()["match_count"]
        assert all_count > single_count
        page.wait_for_function("() => window.__movementDiagnostics.renderedLayerIds.includes('movement-binary-points-full')", timeout=30_000)
        try:
            page.wait_for_function("n => document.querySelector('[data-role=stationarity-status]').textContent.replaceAll(',', '').includes(`${n} stationary candidate fixes`)", arg=all_count, timeout=30_000)
            assert page.locator('[data-action="toggle-threshold-level"][data-level="True"]').is_checked()
            page.wait_for_function("n => window.__testRedFixCount() === n", arg=all_count, timeout=10_000)
        except playwright_api.TimeoutError:
            details = page.evaluate("""() => ({
              status: document.querySelector('[data-role=stationarity-status]').textContent,
              red: window.__testRedFixCount(),
              blocks: (window.__testMapLayers || []).filter(l => l.id.startsWith('movement-binary-points-')).map(l => {
                const b = l.props.userData.binaryBlock;
                return {id:l.id,visible:l.props.visible,revision:b.stationarityRevision,
                  trueValues:[...(b.arrays.stationarity || [])].filter(v=>v===1).length,
                  lastKey:b.lastRenderCacheKey,thresholdCount:b.lastRenderAttributes?.thresholdCount};
              })
            })""")
            pytest.fail(f"Expected {all_count} red fixes; {details}; errors={errors}")
        # Reusing the full map after selecting a subset must update its colours too.
        page.locator('[data-role="select-none"]').click()
        page.locator(f'[data-individual-checkbox="{individual}"]').check()
        page.wait_for_function("n => window.__testRedFixCount() === n", arg=single_count, timeout=30_000)
        page.locator('[data-role="select-all"]').click()
        page.wait_for_function("n => window.__testRedFixCount() === n", arg=all_count, timeout=30_000)
        assert not errors, errors
        browser.close()
