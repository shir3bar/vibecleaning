"""Run isolated CSV/RDS acceptance checks using only production dependencies.

All mutations target a temporary copy of synthetic fixtures. This exercises the
HTTP API in-process; it does not certify a browser, network share, or another OS.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import platform
import shutil
import sys
import tempfile
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient
from app.auth import AuthManager, build_user_record, users_path, write_users_file
from app.cache_cli import clear_rds_cache
from app.state import get_dataset_artifact
from examples.movement.rds_index import read_movement_rds, validate_movement_rds
from examples.rds_movement.app import create_rds_movement_app
from examples.slim_movement.app import create_slim_movement_app


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def checked(response):
    require(response.status_code == 200, f"{response.request.url}: {response.status_code}: {response.text[:1500]}")
    return response


def hashes(root, *, include_state=False):
    return {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in root.rglob("*") if path.is_file() and (include_state or not
                {".vibecleaning", "scrubdata", ".vibecleaning-backup", ".scrubdata-migrating",
                 ".scrubdata-migration.lock"}.intersection(path.parts))}


def profile_check(root, profile):
    family = "movement_rds" if profile == "Rds" else "movement_raw"
    data = root / "données 漢字"
    shutil.copytree(ROOT / "tests/fixtures/deployment/data" / family, data / family)
    raw_hashes = hashes(data)
    password = "temporary-local-acceptance-password"
    write_users_file(users_path(data), [build_user_record(username="acceptance", display_name="José",
                                                       role="editor", password=password)])
    cache = root / "cache"
    factory = create_rds_movement_app if profile == "Rds" else create_slim_movement_app
    base = f"/api/apps/movement/family/{family}/study/acceptance"
    study = data / family / "acceptance"
    artifact = "9001_1.rds" if profile == "Rds" else "movement.csv"
    keys = [f"file:9001_1.rds#row:{i}" if profile == "Rds" else f"id:animal-A-{i}#row:{i}" for i in (1, 2)]
    notes = "José: vérifier 150° — 鹿 <tag>"
    timings = {}

    with ExitStack() as stack:
        def connect(data_root=data):
            app = factory(data_root=data_root, cache_root=cache,
                          static_root=ROOT / "examples/movement/static",
                          index_path=ROOT / "examples/movement/static/index.html",
                          auth_manager=AuthManager.from_data_root(data_root), shared_locking="required")
            client = stack.enter_context(TestClient(app))
            checked(client.post("/api/auth/login", json={"username": "acceptance", "password": password}))
            return client

        client = connect()
        start = time.perf_counter()
        loaded = checked(client.get(f"{base}/load")).json()
        timings["metadata_load_seconds"] = time.perf_counter() - start
        fixes = checked(client.get(f"{base}/dataset/{loaded['dataset_id']}/fixes",
                                   params={"logical_name": artifact, "limit": 100})).json()
        require(fixes["matching_fix_count"] == 24 and len(fixes["fixes"]) == 24, "Load lost source rows")
        timings["data_ready_seconds"] = time.perf_counter() - start

        def payload():
            loaded = checked(client.get(f"{base}/load")).json()
            return {"dataset_id": loaded["dataset_id"], "logical_name": artifact,
                    "expected_current_dataset_id": loaded["dataset_id"],
                    "expected_review_revision": loaded["edit_profile"]["review_revision"]}

        def action(name, **values):
            return checked(client.post(f"{base}/actions/{name}", json={**payload(), **values})).json()

        stale = payload()
        action("annotate-scope", scope={"kind": "fix", "fix_keys": keys}, status="suspected",
               origin="manual", issue_type="speed", comment=notes)

        def parent_id(issue_type):
            _, path = get_dataset_artifact(study, payload()["dataset_id"], "movement_review_annotations.json")
            return next(item["annotation_id"] for item in json.loads(path.read_text(encoding="utf-8"))["annotations"]
                        if item.get("issue_type") == issue_type and item.get("status") == "suspected")

        speed = parent_id("speed")
        action("annotate-scope", scope={"kind": "fix", "fix_keys": keys[:1]}, status="suspected",
               origin="manual", issue_type="GPS spike", comment=notes)
        gps = parent_id("GPS spike")
        action("dismiss-issues", dismissals=[{"parent_annotation_id": speed, "fix_keys": keys[:1]}], note="Plausible movement")
        action("confirm-issues", confirmations=[{"parent_annotation_id": gps, "fix_keys": keys[:1]}], note="Synthetic confirmation")
        action("review-individual", decision={"individual": "animal-A", "review_decision": "fix_keep",
                                              "needs_check": True, "comment": notes})
        head = payload()["dataset_id"]

        # Another app instance over the same directory must see the head and reject an old write.
        second = connect()
        require(checked(second.get(f"{base}/load")).json()["dataset_id"] == head, "Second instance did not see head")
        response = second.post(f"{base}/actions/annotate-scope", json={**stale,
            "scope": {"kind": "fix", "fix_keys": keys[:1]}, "status": "suspected", "origin": "manual", "issue_type": "stale", "comment": "Stale writer probe"})
        require(response.status_code == 423 and response.json().get("code") == "edit_locked",
                f"Historical mutation was not rejected: {response.status_code}: {response.text}")
        response = second.post(f"{base}/actions/annotate-scope", json={**stale, "dataset_id": head,
            "scope": {"kind": "fix", "fix_keys": keys[:1]}, "status": "suspected", "origin": "manual",
            "issue_type": "stale", "comment": "Stale revision probe"})
        require(response.status_code == 409, f"Stale revision was not rejected: {response.status_code}: {response.text}")
        require(payload()["dataset_id"] == head, "Rejected mutation changed head")

        issue = action("generate-report", report_type="issue_first", issue_ids=[speed])
        require(issue["summary"]["matched_fix_count"] == 1, "Dismissed parent selected an extra fix")
        combined = action("generate-report", report_type="issue_first", issue_ids=[speed, gps])
        require(combined["summary"]["matched_fix_count"] == 2, "Combined report lost or duplicated fixes")
        require(set(combined["summary"]["matched_issue_types"]) == {"speed", "GPS spike"},
                "Combined report has incorrect active issue labels")
        analysis = combined["analysis"]["analysis_id"]
        appendix = checked(client.get(f"{base}/analysis/{analysis}/artifact/movement_outlier_fixes.csv")).text
        report_rows = list(csv.DictReader(io.StringIO(appendix)))
        require([(row["status"], row["issue_type"]) for row in report_rows]
                == [("confirmed", "GPS spike"), ("suspected", "speed")], "Report appendix disagrees with saved decisions")
        report = action("generate-report", report_type="individual_profile", individuals=["animal-A"])
        analysis = report["analysis"]["analysis_id"]
        html = checked(client.get(f"{base}/analysis/{analysis}/artifact/movement_individual_reports.html")).text
        for expected in ("Synthetic species A", "Fix &amp; Keep", "José: vérifier 150° — 鹿 &lt;tag&gt;"):
            require(expected in html, f"Profile missing {expected}")
        require("9001_2.rds" not in html, "Profile used another individual's source")
        if profile == "Rds":
            require("9001_1.rds" in html, "Missing original RDS source")
        other = action("generate-report", report_type="individual_profile", individuals=["animal-B"])
        analysis = other["analysis"]["analysis_id"]
        html = checked(client.get(f"{base}/analysis/{analysis}/artifact/movement_individual_reports.html")).text
        require("Synthetic species B" in html and "Not reviewed" in html, "Second profile used another animal's metadata/decision")
        if profile == "Rds":
            require("9001_2.rds" in html and "9001_1.rds" not in html, "Second profile used the first RDS source")

        export = action("export-reviewed-rds" if profile == "Rds" else "export-reviewed-csv", writer="python")
        analysis = export["analysis"]["analysis_id"]
        filename = "movement_reviewed_rds.zip" if profile == "Rds" else "movement_reviewed.csv"
        content = checked(client.get(f"{base}/analysis/{analysis}/artifact/{filename}")).content
        if profile == "Rds":
            exported = root / "exported"
            exported.mkdir()
            with zipfile.ZipFile(io.BytesIO(content)) as bundle:
                bundle.extractall(exported)
            for source in study.glob("*.rds"):
                dest = exported / source.name
                original = read_movement_rds(source)
                reviewed = read_movement_rds(dest)
                validate_movement_rds(dest, reviewed)
                require(len(original) == len(reviewed), "RDS row count changed")
                from examples.movement.rds_export import _compare_original_columns
                from examples.movement.rds_index import RDS_REVIEW_COLUMNS
                columns = {name: [None if v is None or str(v) == "<NA>" else str(v) for v in reviewed[name]]
                           for name in RDS_REVIEW_COLUMNS}
                _compare_original_columns(source, dest, columns)
            frame = read_movement_rds(exported / "9001_1.rds")
            statuses = list(frame["outlier_status"][:2])
            types = list(frame["outlier_issue_type"][:2])
        else:
            rows = list(csv.DictReader(io.StringIO(content.decode("utf-8"))))
            require(len(rows) == 24, "CSV row count changed")
            with (study / artifact).open(encoding="utf-8", newline="") as source:
                originals = list(csv.DictReader(source))
            require(all(all(row[name] == old[name] for name in old) for row, old in zip(rows, originals)), "Original CSV fields changed")
            statuses = [row["outlier_status"] for row in rows[:2]]
            types = [row["outlier_issue_type"] for row in rows[:2]]
        require(statuses == ["confirmed", "suspected"], f"Incorrect exported statuses: {statuses}")
        require(types == ["GPS spike", "speed"], f"Incorrect exported issues: {types}")
        require(hashes(data) == raw_hashes, "Raw files changed")

        imported_data = root / "reimported"
        imported_study = imported_data / family / "acceptance"
        imported_study.mkdir(parents=True)
        if profile == "Rds":
            for path in exported.glob("*.rds"):
                shutil.copy2(path, imported_study / path.name)
        else:
            (imported_study / artifact).write_bytes(content)
        write_users_file(users_path(imported_data), [build_user_record(username="acceptance", display_name="José",
                                                                     role="editor", password=password)])
        imported_client = connect(imported_data)
        imported_head = checked(imported_client.get(f"{base}/load")).json()["dataset_id"]
        imported_fixes = checked(imported_client.get(f"{base}/dataset/{imported_head}/fixes",
            params={"logical_name": artifact, "limit": 100})).json()
        require(imported_fixes["matching_fix_count"] == 24, "Reimport lost rows")
        require([fix["review"]["status"] for fix in imported_fixes["fixes"][:2]] == statuses,
                "Reopened export disagrees with saved statuses")

        # Quiescent backup and relocation: no mutations run during this copy.
        backup = root / "backup"
        restored = root / "restored"
        shutil.copytree(data, backup)
        shutil.copytree(backup, restored)
        require(hashes(data, include_state=True) == hashes(restored, include_state=True),
                "Restore did not preserve all authoritative files")
        clear_rds_cache(cache, remove_all=True)
        reopened = connect(restored)
        require(checked(reopened.get(f"{base}/load")).json()["dataset_id"] == head, "Restored head changed")
        restored_report = checked(reopened.post(f"{base}/actions/generate-report", json={
            "dataset_id": head, "logical_name": artifact, "report_type": "individual_profile", "individuals": ["animal-A"]})).json()
        require(restored_report["summary"]["individual_count"] == 1, "Restored report failed")
    return {"profile": profile, "status": "passed", "timings": timings,
            "checks": ["load", "annotate", "partial dismissal", "confirm", "individual decision",
                       "second instance", "stale write rejection", "reports", "export/reimport",
                       "raw preservation", "backup/restore to another path", "cache rebuild"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = ROOT / "release-manifest.json"
    record = {"release_id": json.loads(manifest.read_text(encoding="utf-8"))["release_id"] if manifest.exists() else "working-tree",
              "system": platform.platform(), "machine": platform.machine(), "python": sys.version,
              "time_utc": datetime.now(timezone.utc).isoformat(), "profiles": [],
              "limits": "In-process HTTP API on synthetic data and local filesystem; no browser, shared-drive or cross-machine certification."}
    try:
        with tempfile.TemporaryDirectory(prefix="vibecleaning-acceptance-") as directory:
            for profile in ("Csv", "Rds"):
                result = profile_check(Path(directory) / profile, profile)
                record["profiles"].append(result)
                print(f"{profile}: passed", flush=True)
        record["status"] = "passed"
    except Exception as exc:
        record.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
