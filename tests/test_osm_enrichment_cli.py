import csv
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import subprocess
import sys
import threading

import osmium


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from examples.movement.osm_extracts import MAX_SOURCE_BYTES_FOR_TINY_FOOTPRINT


def _write_local_pbf(path: Path):
    writer = osmium.SimpleWriter(str(path))
    try:
        for node_id, lon, lat in [
            (1, -70.0, 39.9999),
            (2, -70.0, 40.0001),
            (3, -70.0002, 39.9999),
            (4, -70.0002, 40.0001),
        ]:
            writer.add_node(osmium.osm.mutable.Node(id=node_id, location=(lon, lat)))
        writer.add_way(
            osmium.osm.mutable.Way(id=1, nodes=[1, 2], tags={"highway": "track"})
        )
        writer.add_way(
            osmium.osm.mutable.Way(id=2, nodes=[3, 4], tags={"railway": "rail"})
        )
    finally:
        writer.close()


def _start_geofabrik_server(
    tmp_path: Path,
    *,
    large_unconfirmed: bool = False,
    tiny_oversized: bool = False,
):
    pbf_path = tmp_path / "test-region.osm.pbf"
    _write_local_pbf(pbf_path)
    pbf_content = pbf_path.read_bytes()

    class FakeGeofabrikHandler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            if self.path == "/test-region.osm.pbf":
                self.send_response(200)
                self.send_header(
                    "Content-Length",
                    str(600 * 1024 * 1024 if large_unconfirmed else len(pbf_content)),
                )
                self.send_header("ETag", '"test-pbf"')
                self.end_headers()
                return
            if tiny_oversized and self.path == "/continent.osm.pbf":
                self.send_response(200)
                self.send_header("Content-Length", str(MAX_SOURCE_BYTES_FOR_TINY_FOOTPRINT + 1))
                self.send_header("ETag", '"continent-pbf"')
                self.end_headers()
                return
            self.send_response(404)
            self.end_headers()

        def do_GET(self):
            if self.path == "/index-v1.json":
                features = [
                    {
                        "type": "Feature",
                        "properties": {
                            "id": "test-region",
                            "name": "Test Region",
                            "urls": {
                                "pbf": f"http://127.0.0.1:{self.server.server_port}/test-region.osm.pbf"
                            },
                        },
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [[[-71.0, 39.0], [-69.0, 39.0], [-69.0, 41.0], [-71.0, 41.0], [-71.0, 39.0]]]
                            ],
                        },
                    }
                ]
                if tiny_oversized:
                    features.append(
                        {
                            "type": "Feature",
                            "properties": {
                                "id": "continent",
                                "name": "Continent",
                                "urls": {
                                    "pbf": f"http://127.0.0.1:{self.server.server_port}/continent.osm.pbf"
                                },
                            },
                            "geometry": {
                                "type": "MultiPolygon",
                                "coordinates": [
                                    [[[40.0, 40.0], [60.0, 40.0], [60.0, 60.0], [40.0, 60.0], [40.0, 40.0]]]
                                ],
                            },
                        }
                    )
                payload = {"type": "FeatureCollection", "features": features}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("ETag", '"test-index"')
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path == "/test-region.osm.pbf":
                self.server.pbf_get_count += 1
                self.send_response(200)
                self.send_header("Content-Length", str(len(pbf_content)))
                self.end_headers()
                self.wfile.write(pbf_content)
                return
            if tiny_oversized and self.path == "/continent.osm.pbf":
                self.server.continent_get_count += 1
                self.send_response(500)
                self.end_headers()
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, format, *args):
            return

    server = HTTPServer(("127.0.0.1", 0), FakeGeofabrikHandler)
    server.pbf_get_count = 0
    server.continent_get_count = 0
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _write_input_csv(path: Path):
    path.write_text(
        """eventid,individual,timestamp,longitude,latitude,set
fix_1,alpha,2024-01-01T00:00:00Z,-70.0,40.0,train
fix_2,alpha,2024-01-01T01:00:00Z,-70.0,40.01,train
""",
        encoding="utf-8",
    )


def _write_input_csv_with_tiny_oversized_footprint(path: Path):
    rows = [
        f"local_{index},alpha,2024-01-01T00:0{index}:00Z,-70.0,40.0,train"
        for index in range(5)
    ] + [
        "tiny_1,beta,2024-01-01T01:00:00Z,50.10,50.10,train",
        "tiny_2,beta,2024-01-01T01:01:00Z,50.11,50.11,train",
    ]
    path.write_text(
        "eventid,individual,timestamp,longitude,latitude,set\n"
        + "\n".join(rows)
        + "\n",
        encoding="utf-8",
    )


def _run_cli(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "examples.movement.osm_enrichment_cli", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_offline_osm_enrichment_cli_does_not_import_app_routes():
    source = (
        REPO_ROOT / "examples" / "movement" / "osm_enrichment_cli.py"
    ).read_text(encoding="utf-8")

    assert "routes" not in source
    assert "create_step" not in source
    assert "create_analysis" not in source
    assert "httpx" not in source

    result = _run_cli(["--help"])

    assert result.returncode == 0
    assert "--input-csv" in result.stdout


def test_offline_osm_enrichment_cli_writes_csv_metadata_and_direct_cache_root(tmp_path):
    input_csv = tmp_path / "movement.csv"
    output_csv = tmp_path / "movement_osm_context.csv"
    metadata_json = tmp_path / "context.metadata.json"
    cache_root = tmp_path / "osm_cache"
    _write_input_csv(input_csv)
    server, thread = _start_geofabrik_server(tmp_path)
    try:
        result = _run_cli(
            [
                "--input-csv",
                str(input_csv),
                "--output-csv",
                str(output_csv),
                "--metadata-json",
                str(metadata_json),
                "--radius-m",
                "50",
                "--cache-root",
                str(cache_root),
                "--geofabrik-index-url",
                f"http://127.0.0.1:{server.server_port}/index-v1.json",
                "--progress-json",
            ]
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert result.returncode == 0, result.stderr
    assert output_csv.is_file()
    assert metadata_json.is_file()
    assert (cache_root / "registry" / "geofabrik" / "index-v1.json").is_file()
    assert not (cache_root / ".vibecleaning").exists()
    progress_events = [
        json.loads(line)
        for line in result.stderr.splitlines()
        if line.strip()
    ]
    assert [event.get("stage") for event in progress_events[:8]] == [
        "read_input",
        "build_footprints",
        "load_registry",
        "resolve_sources",
        "preflight_sources",
        "prepare_feature_caches",
        "compute_context",
        "write_output",
    ]

    with output_csv.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["osm:nearest_road_class"] == "track"
    assert rows[0]["osm:nearest_railway_class"] == "rail"
    assert rows[1]["osm:road_match_status"] == "not_found_within_radius"
    metadata = json.loads(metadata_json.read_text(encoding="utf-8"))
    assert metadata["offline_cli"] is True
    assert metadata["input_csv"] == str(input_csv.resolve())
    assert metadata["output_csv"] == str(output_csv.resolve())
    assert metadata["run_status"] == "completed"
    assert metadata["selected_region_ids"] == ["test-region"]


def test_offline_osm_enrichment_cli_refuses_to_overwrite_existing_outputs(tmp_path):
    input_csv = tmp_path / "movement.csv"
    output_csv = tmp_path / "movement_osm_context.csv"
    metadata_json = tmp_path / "movement_osm_context.metadata.json"
    _write_input_csv(input_csv)
    output_csv.write_text("existing output\n", encoding="utf-8")
    metadata_json.write_text("{}\n", encoding="utf-8")

    result = _run_cli(
        [
            "--input-csv",
            str(input_csv),
            "--output-csv",
            str(output_csv),
            "--radius-m",
            "50",
            "--cache-root",
            str(tmp_path / "osm_cache"),
        ]
    )

    assert result.returncode == 1
    assert "Refusing to overwrite" in result.stderr
    assert output_csv.read_text(encoding="utf-8") == "existing output\n"
    assert metadata_json.read_text(encoding="utf-8") == "{}\n"


def test_offline_osm_enrichment_cli_confirmation_required_writes_no_outputs(tmp_path):
    input_csv = tmp_path / "movement.csv"
    output_csv = tmp_path / "movement_osm_context.csv"
    metadata_json = tmp_path / "movement_osm_context.metadata.json"
    _write_input_csv(input_csv)
    server, thread = _start_geofabrik_server(tmp_path, large_unconfirmed=True)
    try:
        result = _run_cli(
            [
                "--input-csv",
                str(input_csv),
                "--output-csv",
                str(output_csv),
                "--radius-m",
                "50",
                "--cache-root",
                str(tmp_path / "osm_cache"),
                "--geofabrik-index-url",
                f"http://127.0.0.1:{server.server_port}/index-v1.json",
            ]
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert result.returncode == 2
    assert "requires confirmation before download" in result.stderr
    assert server.pbf_get_count == 0
    assert not output_csv.exists()
    assert not metadata_json.exists()


def test_offline_osm_enrichment_cli_marks_tiny_oversized_footprint_not_planned(tmp_path):
    input_csv = tmp_path / "movement.csv"
    output_csv = tmp_path / "movement_osm_context.csv"
    metadata_json = tmp_path / "movement_osm_context.metadata.json"
    _write_input_csv_with_tiny_oversized_footprint(input_csv)
    server, thread = _start_geofabrik_server(tmp_path, tiny_oversized=True)
    try:
        result = _run_cli(
            [
                "--input-csv",
                str(input_csv),
                "--output-csv",
                str(output_csv),
                "--radius-m",
                "50",
                "--cache-root",
                str(tmp_path / "osm_cache"),
                "--geofabrik-index-url",
                f"http://127.0.0.1:{server.server_port}/index-v1.json",
                "--confirmed-large-download",
            ]
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert result.returncode == 0, result.stderr
    assert server.pbf_get_count == 1
    assert server.continent_get_count == 0
    with output_csv.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 7
    assert [row["eventid"] for row in rows[:5]] == [f"local_{index}" for index in range(5)]
    assert rows[5]["osm:nearest_road_distance_m"] == ""
    assert rows[5]["osm:nearest_road_class"] == ""
    assert rows[5]["osm:road_match_status"] == "context_not_planned"
    assert rows[5]["osm:nearest_railway_distance_m"] == ""
    assert rows[5]["osm:nearest_railway_class"] == ""
    assert rows[5]["osm:railway_match_status"] == "context_not_planned"
    assert rows[6]["osm:road_match_status"] == "context_not_planned"
    metadata = json.loads(metadata_json.read_text(encoding="utf-8"))
    assert metadata["selected_region_ids"] == ["test-region"]
    assert metadata["excluded_footprint_count"] == 1
    assert metadata["context_not_planned_fix_count"] == 2
    excluded = metadata["excluded_footprints"][0]
    assert excluded["region_id"] == "continent"
    assert excluded["reason"] == "tiny_footprint_requires_oversized_source"
