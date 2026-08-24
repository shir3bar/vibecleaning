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
from app.state import ensure_project_state, get_dataset_artifact
from examples.movement.rds_export import export_reviewed_rds_bundle
from examples.movement.routes import _ranking_definition_matches
from examples.movement.rds_index import (
    build_rds_fixes,
    ensure_rds_index,
    rds_burst_feature_rows,
    read_movement_rds,
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
    assert 'data-role="ranking-method-control"' not in ranking_sheet
    assert '<label data-role="ranking-method-control">Ranking type' in source
    assert '<option value="isolation_forest">Isolation forest — worst burst</option>' in source
    assert '<option value="isolation_forest_decision_margin">Isolation forest — total decision margin</option>' in source
    assert '<option value="source_is_outlier">Source is_outlier — total flagged fixes</option>' in source
    assert 'this.refs.rankingMethod.addEventListener("change"' in source
    ranking_handler_start = source.index("  handleRankingMethodChange() {")
    ranking_handler_end = source.index("\n  hasOsmContextFeatures() {", ranking_handler_start)
    assert "void this.restoreSavedAnalyses()" in source[ranking_handler_start:ranking_handler_end]
    assert "rankingMethod: this.getRankingMethod()" in source
    binary_layers = source[source.index("binaryDeckLayers(") : source.index("renderLayers(", source.index("binaryDeckLayers("))]
    assert binary_layers.count('widthUnits: "meters"') == 2
    assert "getWidth: 9" in binary_layers
    assert binary_layers.count("widthMinPixels: 2") == 2


def test_rds_frontend_loads_full_binary_only_after_selecting_all():
    source = (MOVEMENT_STATIC_ROOT / "app.js").read_text(encoding="utf-8")
    assert 'this.cancelRequest("binaryFixes")' in source
    assert "wholeRdsStudySelected" in source
    assert "if (data.overviewTruncated) {\n    return [];" in source
    assert source.count("await this.loadBinaryMovement({") == 1


def test_pre_sum_source_ranking_is_not_treated_as_a_source_total_ranking():
    assert _ranking_definition_matches("source_is_outlier", "") is False
    assert _ranking_definition_matches(
        "source_is_outlier",
        "source_is_outlier:sum_fix_counts:v1",
    ) is True
    assert _ranking_definition_matches("isolation_forest", "") is True


def test_rds_binary_renderer_reuses_attributes_and_omits_empty_overlays():
    source = (MOVEMENT_STATIC_ROOT / "app.js").read_text(encoding="utf-8")
    binary_layers = source[
        source.index("binaryDeckLayers(") : source.index(
            "renderLayers(", source.index("binaryDeckLayers(")
        )
    ]
    assert "binary.renderCache?.key === cacheKey" in source
    assert "attributes.thresholdCount > 0" in binary_layers
    assert "attributes.suspectedCount > 0" in binary_layers
    assert "attributes.confirmedCount > 0" in binary_layers


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
        assert summary["ranking_method"] == ranking_method
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
    margin_summary = summaries["isolation_forest_decision_margin"]
    assert margin_summary["individual_ranking_summary"]["aggregation"] == "sum_outlier_margin"

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
