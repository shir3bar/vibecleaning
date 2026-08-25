from __future__ import annotations

import io
import json
from pathlib import Path
import shutil
import struct
import sys
import zipfile

from fastapi.testclient import TestClient
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.auth import AuthManager
from app.execution import create_analysis
from app.state import ensure_project_state, get_dataset_artifact, load_dataset
from examples.movement.analysis_history import review_exclusion_signature
from examples.movement.rds_export import export_reviewed_rds_bundle
from examples.movement.routes import _ranking_definition_matches
from examples.movement.rds_index import (
    build_rds_fixes,
    ensure_rds_index,
    rds_burst_feature_rows,
    read_movement_rds,
    resolve_rds_review_scope,
    source_outlier_ranking,
    validate_movement_rds,
)
from examples.movement.summary import build_movement_fixes
from examples.rds_movement.app import create_rds_movement_app


SAMPLE_ROOT = REPO_ROOT / "data" / "movement_rds"
MOVEMENT_STATIC_ROOT = REPO_ROOT / "examples" / "movement" / "static"
MOVEMENT_INDEX = MOVEMENT_STATIC_ROOT / "index.html"


def _sample_files() -> list[Path]:
    paths = sorted(SAMPLE_ROOT.glob("268904527_*.rds"), key=lambda path: path.stat().st_size)
    if len(paths) < 2:
        pytest.skip("RDS movement sample files are unavailable")
    return paths[:2]


def _client(tmp_path: Path) -> tuple[TestClient, Path]:
    study_dir = tmp_path / "data" / "movement_rds" / "268904527"
    study_dir.mkdir(parents=True)
    for source in _sample_files():
        shutil.copy2(source, study_dir / source.name)
    app = create_rds_movement_app(
        data_root=tmp_path / "data",
        static_root=MOVEMENT_STATIC_ROOT,
        index_path=MOVEMENT_INDEX,
        auth_manager=AuthManager.for_testing(
            username="rds-reviewer",
            password="test-password-long",
            role="editor",
        ),
    )
    client = TestClient(app)
    login = client.post(
        "/api/auth/login",
        json={"username": "rds-reviewer", "password": "test-password-long"},
    )
    assert login.status_code == 200
    return client, study_dir


def _binary_header(content: bytes) -> dict:
    assert content[:4] == b"VCM1"
    length = struct.unpack("<I", content[4:8])[0]
    return json.loads(content[8 : 8 + length])


def _create_saved_rds_source_ranking(
    study_dir: Path,
    *,
    dataset_id: str,
    logical_name: str,
    bundle_signature: str,
) -> dict:
    dataset = load_dataset(study_dir, dataset_id)
    script = '''import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text())
output = next(
    item for item in spec["output_artifacts"]
    if item["logical_name"] == "burst_anomaly_ranking.json"
)
Path(output["path"]).write_text(json.dumps({
    "run_status": "completed",
    "ranking_method": "source_is_outlier",
    "ranked_individuals": [],
    "scored_bursts": [],
}))
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text(
    json.dumps({"run_status": "completed"})
)
'''
    return create_analysis(
        study_dir,
        {
            "user": "rds-reviewer",
            "title": "Saved RDS source ranking",
            "kind": "python",
            "script": script,
            "dataset_id": dataset_id,
            "input_artifacts": [
                str(item.get("logical_name") or "")
                for item in dataset.get("artifacts") or []
                if str(item.get("logical_name") or "").lower().endswith(".rds")
            ],
            "output_artifacts": ["burst_anomaly_ranking.json"],
            "parameters": {
                "app": "movement",
                "action": "run_burst_anomaly_ranking",
                "target_artifact": logical_name,
                "ranking_method": "source_is_outlier",
                "ranking_provider": "source_is_outlier",
                "ranking_schema_version": 1,
                "ranking_definition_signature": "source_is_outlier:sum_fix_counts:v1",
                "burst_gap_mode": "manual",
                "burst_gap_seconds": 3600,
                "burst_gap_quantile": 0.999,
                "feature_set": "movement_only",
                "source_bundle_signature": bundle_signature,
                "burst_definition_signature": "source:burst_:v1",
                "review_exclusion_signature": review_exclusion_signature(
                    study_dir, dataset_id, logical_name
                ),
            },
        },
    )


def test_sample_rds_preserves_lossless_identifiers_and_source_schema():
    source = SAMPLE_ROOT / "481458_6898572515.rds"
    if not source.exists():
        pytest.skip("large-identifier RDS sample is unavailable")
    frame = read_movement_rds(source)
    info = validate_movement_rds(source, frame)

    assert info["study_id"] == "481458"
    assert info["individual_id"] == "6898572515"
    assert {"x_", "y_", "t_", "burst_", "is_outlier", "geometry"}.issubset(frame.columns)


def test_shared_frontend_selects_rds_artifacts_in_rds_mode():
    source = (MOVEMENT_STATIC_ROOT / "app.js").read_text(encoding="utf-8")
    assert 'MOVEMENT_APP_CONFIG.rdsSource ? ".rds" : ".csv"' in source
    ranking_sheet = source[
        source.index('<div class="movement-side-sheet ranking hidden"'):
        source.index('<div class="movement-side-sheet feature-space hidden"')
    ]
    assert '<label data-role="ranking-method-control">View ranking' in ranking_sheet
    assert '<option value="isolation_forest">Isolation forest — worst burst</option>' in source
    assert '<option value="isolation_forest_decision_margin">Isolation forest — total decision margin</option>' in source
    assert '<option value="source_is_outlier">Source is_outlier — total flagged fixes</option>' in source
    assert 'this.refs.rankingMethod.addEventListener("change"' in source
    ranking_handler_start = source.index("  handleRankingMethodChange() {")
    ranking_handler_end = source.index("\n  hasOsmContextFeatures() {", ranking_handler_start)
    assert "this.anomalyRankings.get(method)" in source[ranking_handler_start:ranking_handler_end]
    assert "rankingMethod: this.getRankingMethod()" in source
    assert "retainedBinaryDeckLayers(" in source
    assert "movement-overview-preview-" in source
    assert "requestMovementBinaryAttributes(" in source


def test_rds_frontend_loads_full_binary_only_after_selecting_all():
    source = (MOVEMENT_STATIC_ROOT / "app.js").read_text(encoding="utf-8")
    assert "cancelBinaryRequests(" in source
    assert "wholeStudySelected" in source
    assert "if (data.overviewTruncated) {\n    return [];" in source
    assert source.count("await this.loadBinaryMovement({") == 1


def test_pre_sum_source_ranking_is_not_treated_as_a_source_total_ranking():
    assert _ranking_definition_matches("source_is_outlier", "") is False
    assert _ranking_definition_matches(
        "source_is_outlier",
        "source_is_outlier:sum_fix_counts:v1",
    ) is True
    assert _ranking_definition_matches("isolation_forest", "") is True


def test_rds_ranking_survives_individual_review_decision_steps(tmp_path):
    client, study_dir = _client(tmp_path)
    loaded = client.get(
        "/api/apps/movement/family/movement_rds/study/268904527/load"
    ).json()
    dataset_id = loaded["dataset_id"]
    logical_name = loaded["logical_name"]
    overview = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/dataset/{dataset_id}/overview",
        params={"logical_name": logical_name},
    ).json()
    saved = _create_saved_rds_source_ranking(
        study_dir,
        dataset_id=dataset_id,
        logical_name=logical_name,
        bundle_signature=overview["source_bundle_signature"],
    )
    analysis_id = saved["analysis"]["analysis_id"]
    individuals = list(overview["individuals"])
    assert individuals

    for index, review_decision in enumerate(("ok", "fix_keep", "remove")):
        profile = client.get(
            "/api/apps/movement/family/movement_rds/study/268904527/edit-profile",
            params={"dataset_id": dataset_id},
        ).json()
        response = client.post(
            "/api/apps/movement/family/movement_rds/study/268904527/actions/review-individual",
            json={
                "dataset_id": dataset_id,
                "expected_current_dataset_id": dataset_id,
                "expected_review_revision": profile["review_revision"],
                "logical_name": logical_name,
                "source_bundle_signature": overview["source_bundle_signature"],
                "decision": {
                    "individual": individuals[index % len(individuals)],
                    "review_decision": review_decision,
                    "needs_check": False,
                    "comment": "",
                },
            },
        )
        assert response.status_code == 200, response.text
        dataset_id = response.json()["dataset"]["dataset_id"]
        history = client.get(
            "/api/apps/movement/family/movement_rds/study/268904527/analyses",
            params={
                "dataset_id": dataset_id,
                "logical_name": logical_name,
                "burst_gap_mode": "manual",
                "burst_gap_seconds": "3600",
                "burst_gap_quantile": "0.999",
                "feature_set": "movement_only",
                "ranking_method": "source_is_outlier",
            },
        )
        assert history.status_code == 200, history.text
        payload = history.json()
        assert payload["latest_compatible_by_action"][
            "run_burst_anomaly_ranking"
        ] == analysis_id
        inherited = next(
            item for item in payload["items"] if item["analysis_id"] == analysis_id
        )
        assert inherited["dataset_id"] != dataset_id
        assert inherited["compatible"] is True


def test_rds_binary_renderer_reuses_attributes_and_omits_empty_overlays():
    source = (MOVEMENT_STATIC_ROOT / "app.js").read_text(encoding="utf-8")
    worker_source = (MOVEMENT_STATIC_ROOT / "movement_binary_worker.js").read_text(
        encoding="utf-8"
    )
    binary_layers = source[
        source.index("  retainedBinaryDeckLayers(") : source.index(
            "\n  renderLayers(", source.index("  retainedBinaryDeckLayers(")
        )
    ]
    assert "binary.renderCaches?.has(cacheKey)" in source
    assert "binary.lastRenderCacheKey = cacheKey" in source
    assert "attributeCacheKey = binary.lastRenderCacheKey" in binary_layers
    assert "this.binaryFilterExtension = new deck.DataFilterExtension" in binary_layers
    assert "attributes.thresholdCount" in binary_layers
    assert "attributes.suspectedCount" in binary_layers
    assert "attributes.confirmedCount" in binary_layers
    assert 'const GPS_SPIKE_FIELD_KEY = "gps_spike_step_turn"' in worker_source
    assert '"gps_spike_candidate"' not in worker_source
    assert "Number.isFinite(requestedMax)" in worker_source


def test_binary_color_changes_do_not_cache_stale_attributes_under_the_new_state():
    source = (MOVEMENT_STATIC_ROOT / "app.js").read_text(encoding="utf-8")
    renderer = source[
        source.index("  retainedBinaryDeckLayers(") : source.index(
            "\n  renderLayers(", source.index("  retainedBinaryDeckLayers(")
        )
    ]
    loader = source[
        source.index("  async loadBinaryMovement(") : source.index(
            "\n  binaryFixAt(", source.index("  async loadBinaryMovement(")
        )
    ]

    assert "let attributeCacheKey = cacheKey" in renderer
    assert 'attributeCacheKey = binary.lastRenderCacheKey || "pending"' in renderer
    assert "attributes,\n        attributeCacheKey," in renderer
    assert "field.key === GPS_SPIKE_COLOR_FIELD_KEY" in loader
    assert '? "step_length_m"' in loader
    assert "the checked-fix preview is limited" in source
    assert "Flagging resolves the full threshold filter across the scope below." in source


def test_rds_adapter_matches_existing_csv_movement_model(tmp_path):
    source = _sample_files()[0]
    frame = read_movement_rds(source)
    study_dir = tmp_path / "study"
    study_dir.mkdir()
    shutil.copy2(source, study_dir / source.name)
    state = ensure_project_state(study_dir)
    _bundle, index_path = ensure_rds_index(study_dir, state["current_dataset_id"])
    rds_payload = build_rds_fixes(index_path)

    csv_path = tmp_path / "equivalent.csv"
    pd.DataFrame({
        "event-id": [f"fix_{index}" for index in range(1, len(frame) + 1)],
        "individual-local-identifier": frame["individual_local_identifier"].astype(str),
        "timestamp": pd.to_datetime(frame["t_"], unit="s", utc=True).astype(str),
        "location-long": frame["x_"],
        "location-lat": frame["y_"],
        "set": "train",
    }).to_csv(csv_path, index=False)
    csv_payload = build_movement_fixes(csv_path, limit=None)

    assert len(rds_payload["fixes"]) == len(csv_payload["fixes"])
    for rds_fix, csv_fix in zip(rds_payload["fixes"], csv_payload["fixes"], strict=True):
        assert rds_fix["individual"] == csv_fix["individual"]
        assert rds_fix["time_ms"] == csv_fix["time_ms"]
        assert rds_fix["lon"] == pytest.approx(csv_fix["lon"])
        assert rds_fix["lat"] == pytest.approx(csv_fix["lat"])
        for field in (
            "step_length_m", "speed_mps", "time_delta_s", "turn_angle_deg"
        ):
            csv_value = (csv_fix.get("attributes") or {}).get(field)
            if rds_fix["attributes"][field] is None:
                assert csv_value is None
            else:
                assert rds_fix["attributes"][field] == pytest.approx(
                    csv_value
                )


def test_rds_numeric_filter_scope_keeps_selected_individuals(tmp_path):
    study_dir = tmp_path / "study"
    study_dir.mkdir()
    for source in _sample_files():
        shutil.copy2(source, study_dir / source.name)
    state = ensure_project_state(study_dir)
    _bundle, index_path = ensure_rds_index(study_dir, state["current_dataset_id"])
    payload = build_rds_fixes(index_path)
    selected = payload["fixes"][0]["individual"]
    selected_fix_count = sum(
        fix["individual"] == selected for fix in payload["fixes"]
    )

    scope, count = resolve_rds_review_scope(index_path, {
        "kind": "filter",
        "filter": {
            "field_key": "time_delta_s",
            "field_kind": "numeric",
            "operator": "gt",
            "threshold_value": -1,
            "individuals": [selected],
            "set_names": ["train"],
        },
    })

    assert count == selected_fix_count - 1
    assert {item["logical_name"] for item in scope["source_rows"]} == {
        next(
            fix["source_artifact"]
            for fix in payload["fixes"]
            if fix["individual"] == selected
        )
    }


def test_rds_wrapper_serves_shared_ui_and_full_binary_columns(tmp_path):
    client, study_dir = _client(tmp_path)
    root = client.get("/")
    families = client.get("/api/apps/movement/families").json()
    studies = client.get("/api/apps/movement/family/movement_rds/studies").json()
    loaded = client.get(
        "/api/apps/movement/family/movement_rds/study/268904527/load"
    ).json()

    assert root.status_code == 200
    assert '<meta name="vibecleaning-movement-mode" content="rds_movement">' in root.text
    assert [item["name"] for item in families["families"]] == ["movement_rds"]
    assert [item["name"] for item in studies["studies"]] == ["268904527"]
    assert loaded["source_format"] == "rds"

    dataset_id = loaded["dataset_id"]
    logical_name = loaded["logical_name"]
    overview = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/dataset/{dataset_id}/overview",
        params={"logical_name": logical_name},
    ).json()
    assert overview["burst_source"] == "burst_"
    assert any(field["key"] == "is_outlier" for field in overview["color_fields"])
    response = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/dataset/{dataset_id}/fixes-binary"
    )
    assert response.status_code == 200
    header = _binary_header(response.content)
    assert header["row_count"] == overview["total_rows"]
    assert header["line_count"] == header["row_count"] - len(header["individuals"])
    assert set(header["artifacts"]) == {path.name for path in _sample_files()}
    assert set(header["arrays"]) >= {
        "positions", "time_ms", "source_rows", "burst_values", "is_outlier",
        "line_source_indexes", "line_target_indexes",
    }

    cache_path = next((study_dir / ".vibecleaning" / "cache" / "movement").glob("*.sqlite"))
    first_mtime = cache_path.stat().st_mtime_ns
    repeated = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/dataset/{dataset_id}/fixes-binary"
    )
    assert repeated.status_code == 200
    assert cache_path.stat().st_mtime_ns == first_mtime
    cache_path.unlink()
    rebuilt = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/dataset/{dataset_id}/fixes-binary"
    )
    assert rebuilt.status_code == 200
    assert cache_path.exists()

    feature_rows = rds_burst_feature_rows(cache_path)
    source_ranking = source_outlier_ranking(cache_path)
    assert feature_rows
    assert {row["burst_id"] for row in feature_rows} == {
        row["burst_id"] for row in source_ranking["scored_bursts"]
    }
    assert all(row["anomaly_score"] == row["is_outlier_count"] for row in source_ranking["scored_bursts"])

    summaries = {}
    for ranking_method in (
        "source_is_outlier",
        "isolation_forest",
        "isolation_forest_decision_margin",
    ):
        started = client.post(
            "/api/apps/movement/family/movement_rds/study/268904527/actions/run-burst-anomaly-ranking",
            json={
                "dataset_id": dataset_id,
                "logical_name": logical_name,
                "ranking_method": ranking_method,
                "feature_set": "movement_only",
            },
        )
        assert started.status_code == 202
        job_id = started.json()["job_id"]
        job = client.get(
            f"/api/apps/movement/family/movement_rds/study/268904527/analysis-jobs/{job_id}"
        )
        assert job.status_code == 200
        assert job.json()["status"] == "completed", job.json().get("error")
        summary = job.json()["result"]["summary"]
        summaries[ranking_method] = summary
        expected_provider = (
            "source_is_outlier"
            if ranking_method == "source_is_outlier"
            else "isolation_forest"
        )
        assert summary["ranking_method"] == expected_provider
        assert job.json()["result"]["analysis"]["parameters"][
            "ranking_definition_signature"
        ]

    source_summary = summaries["source_is_outlier"]
    assert source_summary["individual_ranking_summary"]["aggregation"] == "sum_anomaly_score"
    expected_source_totals = {}
    for burst in source_ranking["scored_bursts"]:
        expected_source_totals[burst["individual"]] = (
            expected_source_totals.get(burst["individual"], 0)
            + burst["anomaly_score"]
        )
    for individual_row in source_summary["ranked_individuals"]:
        assert individual_row["individual_score"] == expected_source_totals[
            individual_row["individual"]
        ]
        assert len(individual_row["ranked_burst_refs"]) == min(
            3, individual_row["scored_burst_count"]
        )
    isolation_summary = summaries["isolation_forest"]
    legacy_margin_request_summary = summaries["isolation_forest_decision_margin"]
    for isolation_result in (isolation_summary, legacy_margin_request_summary):
        assert isolation_result["ranking_schema_version"] == 2
        assert isolation_result["individual_rankings"]["isolation_forest"][
            "aggregation"
        ] == "maximum_anomaly_score"
        assert isolation_result["individual_rankings"][
            "isolation_forest_decision_margin"
        ]["aggregation"] == "sum_outlier_margin"

    report_fix_key = f'file:{header["artifacts"][0]}#row:1'
    report = client.post(
        "/api/apps/movement/family/movement_rds/study/268904527/actions/generate-report",
        json={
            "dataset_id": dataset_id,
            "logical_name": logical_name,
            "report_type": "issue_first",
            "fix_keys": [report_fix_key],
            "source_bundle_signature": overview["source_bundle_signature"],
        },
    )
    assert report.status_code == 200, report.text
    analysis_id = report.json()["analysis"]["analysis_id"]
    appendix = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/analysis/{analysis_id}/artifact/movement_outlier_fixes.csv"
    )
    assert appendix.status_code == 200
    assert header["artifacts"][0] in appendix.text

    annotated = client.post(
        "/api/apps/movement/family/movement_rds/study/268904527/actions/annotate-scope",
        json={
            "dataset_id": dataset_id,
            "expected_current_dataset_id": dataset_id,
            "expected_review_revision": loaded["edit_profile"]["review_revision"],
            "logical_name": logical_name,
            "source_bundle_signature": overview["source_bundle_signature"],
            "scope": {
                "kind": "fix",
                "fix_keys": [
                    f'file:{header["artifacts"][0]}#row:1',
                    f'file:{header["artifacts"][1]}#row:1',
                ],
            },
            "status": "suspected",
            "origin": "manual",
            "issue_type": "cross-file test",
            "comment": "Verify portable RDS bundle scope",
        },
    )
    assert annotated.status_code == 200, annotated.text
    reviewed_dataset_id = annotated.json()["dataset"]["dataset_id"]
    _, sidecar_path = get_dataset_artifact(
        study_dir, reviewed_dataset_id, "movement_review_annotations.json"
    )
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    annotation = sidecar["annotations"][-1]
    assert sidecar["schema_version"] == 6
    assert annotation["source_id"] == overview["source_bundle_signature"]
    assert annotation["scope"]["source_rows"] == [
        {"logical_name": name, "row_ranges": [[1, 1]]}
        for name in sorted(header["artifacts"][:2])
    ]
    projection_params = [("logical_name", logical_name)] + [
        ("individuals", individual) for individual in overview["individuals"]
    ]
    review_projection = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/dataset/{reviewed_dataset_id}/review-projection",
        params=projection_params,
    )
    assert review_projection.status_code == 200, review_projection.text
    review_projection_payload = review_projection.json()
    assert review_projection_payload["review_counts"] == {
        "suspected": 2,
        "confirmed": 0,
    }
    assert sum(
        end - start + 1
        for item in review_projection_payload["review_status_ranges"]
        if item["status"] == "suspected"
        for start, end in item["row_ranges"]
    ) == 2
    suspicious = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/dataset/{reviewed_dataset_id}/fixes",
        params={
            "logical_name": logical_name,
            "review_status": "suspected",
            "limit": 1,
        },
    )
    assert suspicious.status_code == 200, suspicious.text
    suspicious_payload = suspicious.json()
    assert suspicious_payload["matching_fix_count"] == 2
    assert suspicious_payload["returned_fix_count"] == 1
    assert suspicious_payload["truncated"] is True
    assert suspicious_payload["fixes"][0]["review"]["status"] == "suspected"

    refreshed = client.get(
        "/api/apps/movement/family/movement_rds/study/268904527/load"
    ).json()
    confirmed = client.post(
        "/api/apps/movement/family/movement_rds/study/268904527/actions/confirm-issues",
        json={
            "dataset_id": reviewed_dataset_id,
            "expected_current_dataset_id": reviewed_dataset_id,
            "expected_review_revision": refreshed["edit_profile"]["review_revision"],
            "logical_name": logical_name,
            "source_bundle_signature": overview["source_bundle_signature"],
            "confirmations": [{
                "parent_annotation_id": annotation["annotation_id"],
                "fix_keys": [f'file:{header["artifacts"][0]}#row:1'],
            }],
        },
    )
    assert confirmed.status_code == 200, confirmed.text
    reviewed_dataset_id = confirmed.json()["dataset"]["dataset_id"]
    projected = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/dataset/{reviewed_dataset_id}/fixes-binary"
    )
    assert projected.status_code == 200
    projected_header = _binary_header(projected.content)
    assert projected_header["row_count"] == header["row_count"]
    assert projected_header["line_count"] == header["line_count"] - 1

    exported = client.post(
        "/api/apps/movement/family/movement_rds/study/268904527/actions/export-reviewed-rds",
        json={"dataset_id": reviewed_dataset_id, "writer": "python"},
    )
    assert exported.status_code == 200, exported.text
    export_id = exported.json()["analysis"]["analysis_id"]
    archive = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/analysis/{export_id}/artifact/movement_reviewed_rds.zip"
    )
    assert archive.status_code == 200
    with zipfile.ZipFile(io.BytesIO(archive.content)) as bundle_zip:
        assert "writer_manifest.json" in bundle_zip.namelist()
        assert set(header["artifacts"]).issubset(bundle_zip.namelist())


def test_rds_binary_subset_contains_only_requested_individual(tmp_path):
    client, _study_dir = _client(tmp_path)
    loaded = client.get(
        "/api/apps/movement/family/movement_rds/study/268904527/load"
    ).json()
    dataset_id = loaded["dataset_id"]
    logical_name = loaded["logical_name"]
    overview = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/dataset/{dataset_id}/overview",
        params={"logical_name": logical_name},
    ).json()
    individual = overview["individuals"][0]

    response = client.get(
        f"/api/apps/movement/family/movement_rds/study/268904527/dataset/{dataset_id}/fixes-binary",
        params=[("logical_name", logical_name), ("individuals", individual)],
    )

    assert response.status_code == 200, response.text
    header = _binary_header(response.content)
    assert header["version"] == 2
    assert header["loaded_individuals"] == [individual]
    assert list(header["individual_point_ranges"]) == [individual]
    assert header["row_count"] == overview["stats"][individual]["row_count"]
    assert header["line_count"] == max(0, header["row_count"] - 1)


def test_python_reviewed_rds_export_adds_only_permitted_columns(tmp_path):
    source = _sample_files()[0]
    frame = read_movement_rds(source)
    logical_name = source.name
    annotations = [{
        "annotation_kind": "issue",
        "status": "suspected",
        "issue_type": "test issue",
        "comment": "test comment",
        "step_id": "step_test",
        "scope": {
            "source_rows": [{"logical_name": logical_name, "row_ranges": [[1, 1]]}],
        },
    }]
    output = tmp_path / "reviewed.zip"
    summary = export_reviewed_rds_bundle(
        sources=[(logical_name, source)],
        rows_by_artifact={
            logical_name: [
                {
                    "source_artifact": logical_name,
                    "logical_name": logical_name,
                    "identifier": str(frame["individual_local_identifier"].iloc[index - 1]),
                    "source_row": index,
                    "fix_key": f"file:{logical_name}#row:{index}",
                }
                for index in range(1, len(frame) + 1)
            ],
        },
        annotations=annotations,
        output_zip=output,
        writer="python",
    )

    assert output.exists()
    assert summary["file_count"] == 1
    assert summary["writer_engine"] == "python"
    with zipfile.ZipFile(output) as archive:
        reviewed_path = archive.extract(logical_name, tmp_path / "reviewed")
    reviewed = read_movement_rds(Path(reviewed_path))
    assert list(reviewed.columns[: len(frame.columns)]) == list(frame.columns)
    assert list(reviewed.columns[len(frame.columns) :]) == [
        "outlier_status",
        "outlier_issue_type",
        "outlier_comments",
        "outlier_flag_step_ids",
    ]
    assert not {
        "visible", "manually-marked-outlier", "algorithm-marked-outlier",
    }.intersection(reviewed.columns)
