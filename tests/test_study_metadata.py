"""Native AMT, portable study metadata, and reviewable invalid coordinates."""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import struct
import subprocess

from fastapi.testclient import TestClient
import numpy as np
import pandas as pd
import pytest

from app.auth import AuthManager
from app.state import ensure_project_state
from examples.movement import rds_export
from examples.movement.anomaly_ranking import score_bursts
from examples.movement.rds_index import (
    RDS_REVIEW_COLUMNS, build_rds_binary_columns, build_rds_overview,
    ensure_rds_index, read_movement_rds, resolve_rds_review_scope,
    rds_burst_feature_rows, rds_stationarity_records, validate_movement_rds,
)
from examples.movement.sources import RDS_SOURCE, source_overview
from examples.movement.stationarity import evaluate_stationarity
from examples.movement.study_metadata import (
    metadata_catalog_entry, read_study_metadata, resolve_index_metadata,
)
from examples.rds_movement.app import create_rds_movement_app
from scripts.export_study_metadata import export_metadata


# Base-R-generated track_xyt with integer64 IDs, unused factor levels, UTC
# timestamps, custom row names/attributes, and an invalid point between valid runs.
AMT_FIXTURE = "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"
STUDY = "9000000000"
INDIVIDUAL = "8000000000"
FILENAME = f"{STUDY}_{INDIVIDUAL}.rds"
ROOT = Path(__file__).resolve().parents[1]


def metadata_document():
    return {"schema_version": 1, "study_id": STUDY, "source": {"file_name": "metadata.rds", "sha256": "master"},
        "units": {"animal_mass": "g"}, "individuals": {INDIVIDUAL: {
            "individual_id": INDIVIDUAL, "source_file": FILENAME,
            "individual_local_identifier": "animal-A", "species": "Uria lomvia", "sex": "f",
            "deployments": [{"deployment_id": "01", "animal_mass": 1110, "animal_life_stage": None},
                            {"deployment_id": "02", "animal_mass": 1050, "animal_life_stage": "Adult"}],
        }}}


@pytest.fixture
def amt_study(tmp_path):
    study = tmp_path / "data" / "movement_rds" / STUDY
    study.mkdir(parents=True)
    (study / FILENAME).write_bytes(base64.b64decode(AMT_FIXTURE))
    (study / "metadata.json").write_text(json.dumps(metadata_document()), encoding="utf-8")
    return study


def index_for(study, tmp_path):
    state = ensure_project_state(study)
    return ensure_rds_index(study, state["current_dataset_id"], cache_root=tmp_path / "cache")


def binary_arrays(content):
    length = struct.unpack("<I", content[4:8])[0]
    header = json.loads(content[8:8+length])
    offset = ((8+length+7)//8)*8
    return header, {name: np.frombuffer(content, dtype=item["dtype"],
            count=item["length"], offset=offset+item["offset"])
        for name, item in header["arrays"].items()}


def test_native_amt_index_retains_invalid_rows_and_breaks_geometry(amt_study, tmp_path):
    source = amt_study / FILENAME
    original = source.read_bytes()
    frame = read_movement_rds(source)
    info = validate_movement_rds(source, frame)
    assert info["study_id"] == STUDY and info["individual_id"] == INDIVIDUAL
    assert str(frame.individual_id.dtype) == "Int64"
    assert frame.individual_local_identifier.cat.categories.tolist() == ["animal-A", "unused"]
    bundle, index = index_for(amt_study, tmp_path)
    historical_signature = hashlib.sha256(b"rds-index-schema:8\n")
    historical_signature.update(FILENAME.encode()+b"\0"+hashlib.sha256(original).hexdigest().encode()+b"\n")
    assert bundle.signature == historical_signature.hexdigest()
    overview = build_rds_overview(index, overview_fix_limit=100)
    assert overview["total_rows"] == 5 and overview["invalid_coordinate_count"] == 1
    assert overview["stats"]["animal-A"]["invalid_coordinate_count"] == 1
    assert abs(overview["initial_view"]["longitude"]) <= 180
    assert overview["auto_bursts"][0]["path_length_m"] is None
    assert overview["auto_bursts"][0]["spatial_summary_unavailable"]
    assert overview["fixes"][2]["coordinate_valid"] is False
    header, arrays = binary_arrays(build_rds_binary_columns(index, bundle_signature=bundle.signature))
    assert header["row_count"] == 5
    assert arrays["coordinate_valid"].tolist() == [1, 1, 0, 1, 1]
    assert list(zip(arrays["line_source_indexes"], arrays["line_target_indexes"])) == [(0, 1), (3, 4)]
    assert np.isnan(arrays["step_length_m"][[1, 2, 4]]).all()
    features = rds_burst_feature_rows(index)
    assert features[0]["invalid_coordinate_count"] == 1
    assert any("invalid positions" in warning for warning in score_bursts(features)["warnings"])
    records = rds_stationarity_records(index, {})
    assert records[2]["invalid"]
    periods = []
    evaluate_stationarity(records, {"radius_m": 1000, "minimum_duration_s": 1,
        "maximum_gap_s": 100000, "minimum_fixes": 2, "position": "all"}, [], periods=periods)
    assert len(periods) == 2 and all(len(period) == 2 for period in periods)
    _scope, count = resolve_rds_review_scope(index, {"kind": "roi", "polygon": [[190,190],[210,190],[210,210],[190,210]]})
    assert count == 0
    assert source.read_bytes() == original


def test_metadata_refresh_is_independent_of_fix_cache_and_reimport(amt_study, tmp_path, monkeypatch):
    bundle, index = index_for(amt_study, tmp_path)
    before = index.stat().st_mtime_ns
    monkeypatch.setattr("examples.movement.sources.ensure_rds_index", lambda *a: (bundle, index))
    payload, _signature = source_overview(RDS_SOURCE, amt_study, bundle.dataset_id, FILENAME, overview_fix_limit=0)
    assert payload["species_by_individual"] == {"animal-A": "Uria lomvia"}
    resolved = resolve_index_metadata(amt_study, index)
    assert len(resolved["metadata_by_individual"]["animal-A"]["deployments"]) == 2
    document = metadata_document()
    document["individuals"][INDIVIDUAL]["species"] = "Species updated"
    (amt_study / "metadata.json").write_text(json.dumps(document))
    changed = resolve_index_metadata(amt_study, index)
    assert changed["species_by_individual"]["animal-A"] == "Species updated"
    assert changed["metadata_signature"] != resolved["metadata_signature"]
    bundle2, index2 = index_for(amt_study, tmp_path)
    assert index2 == index and bundle2.signature == bundle.signature and index.stat().st_mtime_ns == before
    assert metadata_catalog_entry(amt_study)["species"] == ["Species updated"]
    connection = sqlite3.connect(index)
    connection.execute("UPDATE artifacts SET logical_name=?", (FILENAME.replace(".rds", "_cleaned.rds"),))
    connection.commit(); connection.close()
    assert resolve_index_metadata(amt_study, index)["species_by_individual"]["animal-A"] == "Species updated"


@pytest.mark.parametrize("problem", ["absent", "malformed", "nonfinite", "wrong_study", "wrong_file", "wrong_deployments"])
def test_bad_or_missing_metadata_does_not_block_tracks(amt_study, tmp_path, problem):
    path = amt_study / "metadata.json"
    if problem == "absent": path.unlink()
    elif problem == "malformed": path.write_text("{")
    elif problem == "nonfinite": path.write_text(path.read_text().replace('"animal_mass": 1110', '"animal_mass": NaN'))
    else:
        document = metadata_document()
        if problem == "wrong_study": document["study_id"] = "1"
        if problem == "wrong_file": document["individuals"][INDIVIDUAL]["source_file"] = "../file.rds"
        if problem == "wrong_deployments": document["individuals"][INDIVIDUAL]["deployments"] = "bad"
        path.write_text(json.dumps(document))
    bundle, index = index_for(amt_study, tmp_path)
    payload = resolve_index_metadata(amt_study, index)
    assert payload["metadata_by_individual"] == {}
    assert bool(payload["metadata_warnings"]) is (problem != "absent")
    assert build_rds_overview(index)["total_rows"] == 5


def test_mixed_species_resolve_per_numeric_individual_id(amt_study, tmp_path):
    _bundle, index = index_for(amt_study, tmp_path)
    document = metadata_document()
    second_id = "8000000001"
    document["individuals"][second_id] = {
        "individual_id": second_id, "source_file": f"{STUDY}_{second_id}.rds",
        "individual_local_identifier": "animal-B", "species": "Uria aalge", "sex": None, "deployments": [],
    }
    (amt_study / "metadata.json").write_text(json.dumps(document))
    with sqlite3.connect(index) as connection:
        connection.execute("INSERT INTO artifacts VALUES(2,?, 'test', 0, ?, 'animal-B', ?, '{}')",
                           (f"{STUDY}_{second_id}.rds", STUDY, second_id))
    resolved = resolve_index_metadata(amt_study, index)
    assert resolved["species_by_individual"] == {"animal-A": "Uria lomvia", "animal-B": "Uria aalge"}
    assert resolved["study_metadata"]["species"] == ["Uria aalge", "Uria lomvia"]
    assert metadata_catalog_entry(amt_study)["species"] == ["Uria aalge", "Uria lomvia"]


@pytest.mark.parametrize("engine", ["python", "r"])
def test_amt_export_preserves_r_types_attributes_and_metadata(amt_study, tmp_path, engine):
    if engine == "r" and not shutil.which("Rscript"):
        pytest.skip("Rscript unavailable")
    source = amt_study / FILENAME
    original = source.read_bytes()
    bundle, index = index_for(amt_study, tmp_path)
    rows = [{"fix_key": f"file:{FILENAME}#row:{i}", "source_row": i,
             "logical_name": FILENAME, "identifier": "animal-A"} for i in range(1, 6)]
    annotations = [{"annotation_id": "flag", "status": "suspected", "issue_type": "Invalid position",
                    "scope": {"kind": "fix", "source_rows": [{"logical_name": FILENAME, "row_ranges": [[3,3]]}]}}]
    result = rds_export.export_reviewed_rds_bundle(sources=[(FILENAME, source)],
        rows_by_artifact={FILENAME: rows}, annotations=annotations,
        output_dir=tmp_path / "output", cleaned_dir=None, writer=engine,
        metadata_path=amt_study / "metadata.json", decisions=[])
    output = tmp_path / "output" / FILENAME.replace(".rds", "_cleaned.rds")
    frame = read_movement_rds(output)
    assert len(frame) == 5 and frame.outlier_status.iloc[2] == "suspected"
    assert frame.x_.iloc[2] == 200 and "geometry" not in frame
    assert read_study_metadata(output.parent)[0]["individuals"][INDIVIDUAL]["species"] == "Uria lomvia"
    manifest = json.loads((output.parent / "writer_manifest.json").read_text())
    metadata_entry = next(row for row in manifest["auxiliary_files"] if row["output_name"] == "metadata.json")
    assert metadata_entry["output_sha256"] == hashlib.sha256((output.parent / "metadata.json").read_bytes()).hexdigest()
    assert result["row_count"] == 5 and source.read_bytes() == original
    if shutil.which("Rscript"):
        subprocess.run(["Rscript", "-e", '''a<-commandArgs(TRUE);x<-readRDS(a[1]);y<-readRDS(a[2]);
            stopifnot(identical(class(x),class(y)));ax<-attributes(x);ay<-attributes(y);
            ax$names<-NULL;ay$names<-NULL;stopifnot(identical(ax,ay));
            for(n in names(x))stopifnot(identical(x[[n]],y[[n]]))''', str(source), str(output)], check=True)


def test_all_invalid_track_still_has_table_rows_and_safe_bounds(amt_study, tmp_path):
    bundle, index = index_for(amt_study, tmp_path)
    with sqlite3.connect(index) as connection:
        connection.execute("UPDATE fixes SET lon=200,lat=200,coordinate_valid=0")
    overview = build_rds_overview(index)
    assert overview["total_rows"] == overview["invalid_coordinate_count"] == 5
    assert overview["initial_view"] == {"longitude": 0, "latitude": 0, "zoom": overview["initial_view"]["zoom"]}
    header, _arrays = binary_arrays(build_rds_binary_columns(index, bundle_signature=bundle.signature))
    assert header["line_count"] == 0 and header["row_count"] == 5


def test_exporter_preserves_deployments_units_ids_and_missing_values(tmp_path, monkeypatch):
    source = tmp_path / "master.rds"; source.write_bytes(b"master")
    study = tmp_path / "collection" / "anne" / "movement_rds" / STUDY
    study.mkdir(parents=True); (study / FILENAME).write_bytes(b"track")
    frame = pd.DataFrame([{"fileName": FILENAME, "MBid": STUDY, "individual_id": int(INDIVIDUAL),
        "individual_local_identifier": "animal-A", "species": "Uria lomvia", "sex": None,
        "tracking_start_date": 1373847600., "animal_mass": np.nan, "deployment_id": str(i),
        "excluded": "yes_duplicated", "rowID": f"r{i}"} for i in range(2)])
    monkeypatch.setattr("scripts.export_study_metadata.rdata.read_rds", lambda *a, **k: frame)
    result = export_metadata(source, tmp_path / "collection")
    assert (result["studies"], result["individuals"], result["metadata_rows"]) == (1,1,2)
    document = json.loads((study / "metadata.json").read_text())
    record = document["individuals"][INDIVIDUAL]
    assert record["deployments"][0]["animal_mass"] is None
    assert record["deployments"][0]["tracking_start_date"] == "2013-07-15T00:20:00Z"
    assert record["deployments"][0]["excluded"] == "yes_duplicated"
    assert export_metadata(source, tmp_path / "collection") == result


def client_for(study, tmp_path):
    app = create_rds_movement_app(data_root=study.parents[1], cache_root=tmp_path / "api-cache",
        static_root=ROOT / "examples/movement/static", index_path=ROOT / "examples/movement/static/index.html",
        auth_manager=AuthManager.for_testing(username="editor", password="test-password-long", role="editor"))
    client = TestClient(app)
    assert client.post("/api/auth/login", json={"username": "editor", "password": "test-password-long"}).status_code == 200
    return client, app


def test_metadata_api_report_and_export_snapshot(amt_study, tmp_path):
    client, _app = client_for(amt_study, tmp_path)
    prefix = f"/api/apps/movement/family/movement_rds/study/{STUDY}"
    listing = client.get("/api/apps/movement/family/movement_rds/studies").json()["studies"]
    assert listing[0]["species"] == ["Uria lomvia"]
    loaded = client.get(prefix+"/load").json(); dataset_id=loaded["dataset_id"]
    for endpoint in ("overview", "summary"):
        response = client.get(prefix+f"/dataset/{dataset_id}/{endpoint}", params={"logical_name": FILENAME})
        assert response.status_code == 200, response.text
        assert response.json()["metadata_by_individual"]["animal-A"]["species"] == "Uria lomvia"
    report = client.post(prefix+"/actions/generate-report", json={"dataset_id": dataset_id,
        "logical_name": FILENAME, "report_type": "individual_profile", "individuals": ["animal-A"]})
    assert report.status_code == 200, report.text
    analysis_id = report.json()["analysis"]["analysis_id"]
    html = client.get(prefix+f"/analysis/{analysis_id}/artifact/movement_individual_reports.html")
    assert html.status_code == 200 and "Uria lomvia" in html.text
    response = client.post(prefix+"/actions/export-reviewed-rds", json={"dataset_id": dataset_id})
    assert response.status_code == 200, response.text
    assert (amt_study / "scrubdata/cleaned_files/metadata.json").exists()
    assert len(read_movement_rds(amt_study / "scrubdata/cleaned_files" / FILENAME.replace(".rds", "_cleaned.rds"))) == 5


def test_completion_exports_metadata_and_preserves_remove_decision_rows(amt_study, tmp_path):
    client, _app = client_for(amt_study, tmp_path)
    prefix = f"/api/apps/movement/family/movement_rds/study/{STUDY}"
    loaded = client.get(prefix+"/load").json()
    dataset_id = loaded["dataset_id"]
    reviewer = client.get("/api/apps/movement/reviewers").json()["reviewers"][0]
    assigned = client.post(prefix+"/review/assign", json={"reviewer_user_id": reviewer["user_id"],
        "logical_name": FILENAME, "expected_current_dataset_id": dataset_id,
        "expected_review_revision": loaded["edit_profile"]["review_revision"]})
    assert assigned.status_code == 200, assigned.text
    decision = client.post(prefix+"/actions/review-individual", json={"dataset_id": dataset_id,
        "expected_current_dataset_id": dataset_id, "logical_name": FILENAME,
        "expected_review_revision": assigned.json()["state"]["revision"],
        "decision": {"individual": "animal-A", "review_decision": "remove", "needs_check": False,
                     "removal_reason": "other", "comment": "Retain data in annotated export"}})
    assert decision.status_code == 200, decision.text
    dataset_id = decision.json()["dataset"]["dataset_id"]
    profile = client.get(prefix+"/edit-profile", params={"dataset_id": dataset_id}).json()
    completed = client.post(prefix+"/review/complete", json={"expected_current_dataset_id": dataset_id,
        "expected_review_revision": profile["review_revision"]})
    assert completed.status_code == 200, completed.text
    folder = amt_study / "scrubdata/cleaned_files"
    assert (folder / "metadata.json").exists()
    assert "remove" in (folder / "individual_decisions.csv").read_text()
    output = folder / FILENAME.replace(".rds", "_cleaned.rds")
    assert len(read_movement_rds(output)) == 5
    reimport = tmp_path / "reimport/movement_rds" / STUDY
    reimport.mkdir(parents=True)
    shutil.copy2(output, reimport / output.name)
    shutil.copy2(folder / "metadata.json", reimport / "metadata.json")
    bundle, index = index_for(reimport, tmp_path / "reimport")
    assert build_rds_overview(index)["invalid_coordinate_count"] == 1
    assert resolve_index_metadata(reimport, index)["species_by_individual"] == {"animal-A": "Uria lomvia"}


@pytest.mark.browser
def test_metadata_and_invalid_position_navigation_in_browser(amt_study, tmp_path):
    from playwright.sync_api import sync_playwright, expect
    from urllib.parse import parse_qs, urlsplit
    from test_movement_browser import _serve, _open_browser, _new_page
    _client, app = client_for(amt_study, tmp_path)
    with _serve(app) as url, sync_playwright() as playwright:
        browser = _open_browser(playwright)
        page = _new_page(browser)
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(url)
        page.locator("#login-username").fill("editor")
        page.locator("#login-password").fill("test-password-long")
        page.locator("#login-submit").click()
        option = page.locator(f'[data-role="study"] option[value="{STUDY}"]')
        expect(option).to_contain_text("Uria lomvia", timeout=60000)
        page.locator('[data-role="study"]').select_option(STUDY)
        expect(page.locator('[data-role="study-metadata"]')).to_contain_text("Uria lomvia", timeout=60000)
        expect(page.locator('[data-role="study-metadata"] a')).to_have_count(3)
        # Follow the generated links as a reviewer would. Stub external pages so
        # this regression checks query preservation without depending on uptime.
        def species_search(route):
            name = parse_qs(urlsplit(route.request.url).query).get("q", [""])[0]
            route.fulfill(content_type="text/plain", body=f"Species search: {name}")
        page.context.route("https://www.gbif.org/taxon/search?*", species_search)
        page.context.route("https://eol.org/search?*", species_search)
        page.locator('[data-role="species-menu"] > summary').click()
        for label in ("GBIF", "EOL"):
            with page.expect_popup() as opened:
                page.locator('[data-role="study-metadata"]').get_by_role("link", name=label, exact=True).click()
            popup = opened.value
            expect(popup.locator("body")).to_have_text("Species search: Uria lomvia")
            popup.close()
        details = page.locator('[data-individual-metadata]').first
        details.locator("summary").click()
        expect(details).to_contain_text("1110")
        expect(details).to_contain_text("1050")
        page.locator('[data-invalid-individual="animal-A"]').click()
        expect(page.locator('[data-role="table-invalid-only"]')).to_be_checked()
        row = page.locator('[data-role="table-wrap"] tr[data-fix-key]')
        expect(row).to_have_count(1, timeout=60000)
        expect(row).to_contain_text("Invalid position")
        expect(row).to_contain_text("200")
        page.locator('[data-role="table-invalid-only"]').uncheck()
        expect(page.locator('[data-role="table-wrap"] tr[data-fix-key]')).to_have_count(5)
        page.locator('[data-role="table-mode"]').select_option("auto_bursts")
        expect(page.locator('[data-role="table-wrap"]')).to_contain_text("Burst contains invalid positions")
        assert errors == []
        browser.close()
