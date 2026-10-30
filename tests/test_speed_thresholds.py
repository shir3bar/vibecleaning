"""A Speed threshold selects the central fix, independently of turn angle."""
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

import numpy as np
import pandas as pd
import pytest
import rdata
from fastapi.testclient import TestClient

from app.auth import AuthManager
from app.state import get_dataset_artifact, list_history, load_project_state
from examples.movement.movement_features import speed_threshold_matches
from examples.movement.review_annotations import resolve_filter_row_ranges
from examples.movement.routes import _validate_filter_scope
from examples.movement import study_filters
from examples.rds_movement.app import create_rds_movement_app
from examples.slim_movement.app import create_slim_movement_app
from scripts.generate_filter_pipeline_demo import amt_constructor, crs_constructor, DemoCrs, GEOD, EPOCH

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("incoming,outgoing,expected", [
    (11, 11, True), (10, 11, False), (11, 10, False), (1, 20, False),
    (20, 1, False), (None, 20, False), (20, None, False),
    (float("nan"), 20, False), (20, float("inf"), False), (0, 0, False),
])
def test_both_speeds_must_strictly_exceed_the_cutoff(incoming, outgoing, expected):
    assert speed_threshold_matches(incoming, outgoing, 10) is expected


@pytest.mark.parametrize("angle", [0, 180, None, float("nan")])
def test_study_speed_candidates_and_quantiles_do_not_use_angles(angle):
    speeds = [1., 2., 2., 3.] * 30 + [180., 180., None]
    rows = [{"fix_key": str(i), "row_index": i + 1, "source_artifact": "track.rds"}
            for i in range(len(speeds))]
    movement = {row["fix_key"]: {"speed_mps": speed, "turn_angle_deg": angle}
                for row, speed in zip(rows, speeds)}
    context = object.__new__(study_filters.StudyFilters)
    context.metrics = lambda excluded: iter([(rows, movement)])
    matches, stats = context.calculate("speed", {})
    assert matches == {"track.rds": [[122, 122]]}
    assert stats["sample_count"] == 122
    assert stats["threshold_mps"] == pytest.approx(103)


@pytest.fixture(params=["csv", "rds"])
def speed_study(tmp_path, request):
    source_format = request.param
    family = "movement_rds" if source_format == "rds" else "movement_raw"
    study = tmp_path / "data" / family / "910004"
    study.mkdir(parents=True)
    cases = [
        ("Departure", [0, 6, 606], [0, 60, 120]),
        ("Arrival", [0, 600, 606], [0, 60, 120]),
        ("Straight", [0, 600, 1200], [0, 60, 120]),
        ("Reversal", [0, 600, 0], [0, 60, 120]),
        ("Two_fixes", [0, 600], [0, 60]),
        ("Invalid", [0, 600, None, 1800, 2400], [0, 60, 120, 180, 240]),
        ("Zero_lag", [0, 600, 1200], [0, 0, 60]),
        ("Single", [0], [0]),
    ]
    csv_rows, expected = [], []
    for individual_id, (name, xs, times) in enumerate(cases, 1):
        lons = [GEOD.fwd(0, 0, 90, x)[0] if x is not None else 200. for x in xs]
        filename = f"910004_{individual_id}.rds"
        frame = pd.DataFrame({"x_": lons, "y_": np.zeros(len(xs)), "t_": EPOCH + np.array(times),
            "timestamp": EPOCH + np.array(times), "study_id": [910004] * len(xs),
            "individual_id": [individual_id] * len(xs), "individual_local_identifier": [name] * len(xs),
            "tag_local_identifier": ["tag"] * len(xs), "burst_": np.ones(len(xs))})
        if source_format == "rds":
            rdata.write_rds(study / filename, frame,
                constructor_dict={**rdata.conversion.to_r.DEFAULT_CLASS_MAP,
                                  pd.DataFrame: amt_constructor, DemoCrs: crs_constructor})
        for i, (lon, seconds) in enumerate(zip(lons, times), 1):
            csv_rows.append({"eventid": f"{name}_{i}", "individual": name,
                "timestamp": datetime.fromtimestamp(EPOCH + seconds, timezone.utc).isoformat(),
                "longitude": lon, "latitude": 0})
            if name in {"Straight", "Reversal"} and i == 2:
                expected.append(f"file:{filename}#row:2" if source_format == "rds"
                                else f"id:{name}_{i}#row:{len(csv_rows)}")
    if source_format == "csv":
        with (study / "movement.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(csv_rows[0]))
            writer.writeheader()
            writer.writerows(csv_rows)
    create_app = create_rds_movement_app if source_format == "rds" else create_slim_movement_app
    options = {"cache_root": tmp_path / "cache"} if source_format == "rds" else {}
    app = create_app(data_root=tmp_path / "data", static_root=ROOT / "examples/movement/static",
        index_path=ROOT / "examples/movement/static/index.html",
        auth_manager=AuthManager.for_testing(username="browser-reviewer", password="test-password-long", role="editor"),
        **options)
    return source_format, app, study, family, expected


def test_threshold_preview_apply_and_reload_select_same_central_fixes(speed_study):
    from test_movement_issue_groups import post_review
    source_format, app, study, family, expected = speed_study
    originals = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in study.iterdir()}
    with TestClient(app) as client:
        client.post("/api/auth/login", json={"username": "browser-reviewer", "password": "test-password-long"})
        base = f"/api/apps/movement/family/{family}/study/910004"
        loaded = client.get(base + "/load").json()
        dataset, logical = loaded["dataset_id"], loaded["logical_name"]
        spec = {"field_key": "speed_mps", "field_kind": "numeric", "threshold_value": 5, "operator": "gt"}
        preview = client.post(base + "/actions/preview-filter", json={
            "dataset_id": dataset, "logical_name": logical, "filter": spec})
        assert preview.status_code == 200, preview.text
        assert preview.json()["match_count"] == 2
        assert load_project_state(study)["current_dataset_id"] == dataset
        saved = post_review(client, base, "annotate-scope", {
            "dataset_id": dataset, "logical_name": logical, "expected_current_dataset_id": dataset,
            "scope": {"kind": "filter", "filter": spec}, "status": "suspected",
            "issue_type": "Speed", "issue_field": "speed_mps", "user": "Reviewer",
            "comment": "Both adjacent speeds exceed 5 m/s."})
        assert saved.status_code == 200, saved.text
        result = saved.json()
        assert result["step"]["summary"]["resolved_fix_count"] == 2
        assert result["step"]["parameters"]["scope"]["filter"]["speed_basis"] == "both-adjacent"
        new_dataset = result["dataset"]["dataset_id"]
        _, sidecar = get_dataset_artifact(study, new_dataset, "movement_review_annotations.json")
        annotation = json.loads(sidecar.read_text())["annotations"][-1]
        scope = annotation["scope"]
        if source_format == "rds":
            actual = [f"file:{s['logical_name']}#row:{i}" for s in scope["source_rows"]
                      for start, end in s["row_ranges"] for i in range(start, end + 1)]
        else:
            with (study / "movement.csv").open() as stream:
                rows = list(csv.DictReader(stream))
            actual = [f"id:{rows[i - 1]['eventid']}#row:{i}" for start, end in scope["row_ranges"]
                      for i in range(start, end + 1)]
        assert set(actual) == set(expected)
        assert client.get(base + "/load").json()["dataset_id"] == new_dataset
    assert originals == {name: hashlib.sha256((study / name).read_bytes()).hexdigest() for name in originals}


def test_csv_speed_threshold_sorts_tracks_respects_sets_and_rebuilds_neighbors(tmp_path):
    path = tmp_path / "tracks.csv"
    path.write_text("eventid,individual,timestamp,longitude,latitude,set\n"
        "c,A,2024-01-01T00:02:00Z,0,0,train\n"
        "a,A,2024-01-01T00:00:00Z,0,0,train\n"
        "b,A,2024-01-01T00:01:00Z,0.01,0,train\n"
        "d,B,2024-01-01T00:00:00Z,0,0,train\n"
        "e,B,2024-01-01T00:01:00Z,0.01,0,test\n"
        "f,B,2024-01-01T00:02:00Z,0,0,test\n")
    spec = _validate_filter_scope({"field_key": "speed_mps", "field_kind": "numeric",
                                   "operator": "gt", "threshold_value": 5})
    assert resolve_filter_row_ranges(path, spec) == ([[3, 3]], 1)
    assert resolve_filter_row_ranges(path, spec, confirmed_fix_keys={"id:b#row:3"}) == ([], 0)


def test_frontend_speed_preview_and_worker_parity(node_runtime):
    result = subprocess.run([node_runtime, str(ROOT / "tests/frontend/speed_thresholds.cjs"),
                             str(ROOT / "examples/movement/static")], capture_output=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.browser
def test_speed_threshold_browser_preview_matches_saved_flags(speed_study):
    from playwright.sync_api import sync_playwright, expect
    from test_movement_browser import _serve, _open_browser, _new_page, _login_and_wait
    _, app, study, _, expected = speed_study
    with _serve(app) as url, sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser)
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        _login_and_wait(page, url, "910004")
        page.locator("[data-individual-checkbox]").first.wait_for(state="attached", timeout=30000)
        page.locator('[data-role="select-all"]').click()
        page.locator('[data-role="fixes-progress"]').wait_for(state="hidden", timeout=30000)
        page.locator('[data-role="color-by"]').select_option("speed_mps")
        cutoff = page.locator('[data-action="set-threshold-value"]')
        cutoff.fill("5")
        cutoff.press("Tab")
        page.locator('[data-action="check-above-threshold"]').click()
        cards = page.locator('[data-role="selected-fixes"] .movement-title')
        assert {text.split(" • ")[0] for text in cards.all_text_contents()} == {"Straight", "Reversal"}
        expect(cards).to_have_count(2)
        with page.expect_response(lambda response: response.url.endswith("/actions/preview-filter")) as preview:
            page.locator('[data-role="mark-suspected"]').click()
        assert preview.value.json()["match_count"] == len(expected)
        page.locator('[data-role="issue-modal"]').wait_for(state="visible")
        with page.expect_response(lambda response: response.url.endswith("/actions/annotate-scope")) as saved:
            page.locator('[data-role="issue-submit"]').click()
        assert saved.value.status == 200, saved.value.text()
        assert saved.value.json()["step"]["summary"]["resolved_fix_count"] == len(expected)
        assert len(list_history(study)["steps"]) == 1
        assert not errors
        browser.close()
