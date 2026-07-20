import hashlib
import json
from pathlib import Path
import sys

import httpx
import osmium
import pytest
from shapely.geometry import shape

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from examples.movement.osm_context import (
    MAX_CONTEXT_SEARCH_RADIUS_M,
    build_fix_osm_context,
    build_osm_fetch_scopes,
)
from examples.movement.osm_extracts import (
    FOOTPRINT_GRID_DEGREES,
    GEOFABRIK_INDEX_URL,
    MAX_REQUIRED_FOOTPRINTS,
    MAX_SOURCE_BYTES_FOR_TINY_FOOTPRINT,
    MAX_SELECTED_SOURCES,
    MAX_UNCONFIRMED_DOWNLOAD_BYTES,
    MAX_UNCONFIRMED_SOURCE_COUNT,
    MIN_FIXES_TO_FORCE_LARGE_OSM_SOURCE,
    OSM_CACHE_SCHEMA_VERSION,
    OSM_COVERAGE_MODEL,
    OSMExtractSourceError,
    OSM_LAYER_SPEC_VERSION,
    OSM_SOURCE_PROVIDER,
    OSM_SOURCE_TYPE,
    SUPPORTED_CONTEXT_LAYER_NAMES,
    TINY_FOOTPRINT_EXCLUSION_REASON,
    apply_tiny_footprint_source_guardrail,
    build_required_extent,
    build_required_footprints,
    cache_geofabrik_source_pbf,
    derived_extent_key,
    derived_feature_paths,
    derived_metadata,
    extract_context_feature_cache,
    find_cached_geofabrik_source,
    geofabrik_registry_paths,
    geofabrik_source_paths,
    get_geofabrik_registry,
    load_cached_context_features,
    parse_geofabrik_extracts,
    preflight_multi_source_caches,
    prepare_multi_source_feature_caches,
    production_layer_specs,
    registry_metadata,
    resolve_geofabrik_extract,
    resolve_geofabrik_sources,
    source_metadata,
    source_revision_key,
)


def _feature(region_id: str, coordinates: list, *, parent: str | None = None) -> dict:
    properties = {
        "id": region_id,
        "name": region_id.replace("-", " ").title(),
        "urls": {"pbf": f"https://download.example/{region_id}-latest.osm.pbf"},
    }
    if parent is not None:
        properties["parent"] = parent
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": {"type": "MultiPolygon", "coordinates": [[coordinates]]},
    }


def _catalog(*features: dict) -> dict:
    return {"type": "FeatureCollection", "features": list(features)}


def _extent(lon: float = 138.55, lat: float = -34.92, radius: float = 100.0) -> dict:
    return build_required_extent(
        [{"fix_key": "fix-1", "lon": lon, "lat": lat}],
        search_radius_m=radius,
    )


def test_production_layers_are_limited_to_existing_road_and_railway_specs():
    specs = production_layer_specs()

    assert OSM_SOURCE_PROVIDER == "geofabrik"
    assert OSM_SOURCE_TYPE == "local_extract"
    assert OSM_COVERAGE_MODEL == "occupied_grid_footprints_v1"
    assert FOOTPRINT_GRID_DEGREES == 0.25
    assert SUPPORTED_CONTEXT_LAYER_NAMES == ("road", "railway")
    assert [spec["layer_name"] for spec in specs] == ["road", "railway"]
    assert [spec["class_tag"] for spec in specs] == ["highway", "railway"]


def test_parse_and_resolve_selects_smallest_covering_geofabrik_extract():
    extracts = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "australia",
                [[110.0, -45.0], [155.0, -45.0], [155.0, -10.0], [110.0, -10.0], [110.0, -45.0]],
            ),
            _feature(
                "south-australia",
                [[129.0, -39.9], [141.1, -39.9], [141.1, -25.9], [129.0, -25.9], [129.0, -39.9]],
                parent="australia",
            ),
        )
    )

    resolution = resolve_geofabrik_extract(extracts, _extent())

    assert resolution["run_status"] == "resolved"
    assert resolution["resolution_mode"] == "smallest_covering_region"
    assert resolution["source"]["region_id"] == "south-australia"
    assert resolution["source"]["parent_region_id"] == "australia"
    assert resolution["candidate_region_ids"] == ["south-australia", "australia"]


def test_coverage_uses_region_geometry_not_only_its_bounding_box():
    concave_region = _feature(
        "not-covering",
        [
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 2.0],
            [2.0, 2.0],
            [2.0, 8.0],
            [10.0, 8.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ],
    )
    larger_region = _feature(
        "covering",
        [[-1.0, -1.0], [11.0, -1.0], [11.0, 11.0], [-1.0, 11.0], [-1.0, -1.0]],
    )
    extracts = parse_geofabrik_extracts(_catalog(concave_region, larger_region))

    resolution = resolve_geofabrik_extract(extracts, _extent(lon=5.0, lat=5.0, radius=1.0))

    assert resolution["source"]["region_id"] == "covering"


def test_explicit_region_override_must_cover_required_extent():
    extracts = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "south-australia",
                [[129.0, -39.9], [141.1, -39.9], [141.1, -25.9], [129.0, -25.9], [129.0, -39.9]],
            ),
            _feature(
                "victoria",
                [[141.0, -39.5], [150.0, -39.5], [150.0, -33.0], [141.0, -33.0], [141.0, -39.5]],
            ),
        )
    )

    accepted = resolve_geofabrik_extract(extracts, _extent(), region_id="south-australia")
    rejected = resolve_geofabrik_extract(extracts, _extent(), region_id="victoria")

    assert accepted["run_status"] == "resolved"
    assert accepted["resolution_mode"] == "explicit_region"
    assert rejected["run_status"] == "unresolved"
    assert "does not cover" in rejected["error"]


def test_no_single_covering_extract_returns_unresolved():
    extracts = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "west",
                [[0.0, 0.0], [4.0, 0.0], [4.0, 10.0], [0.0, 10.0], [0.0, 0.0]],
            ),
            _feature(
                "east",
                [[6.0, 0.0], [10.0, 0.0], [10.0, 10.0], [6.0, 10.0], [6.0, 0.0]],
            ),
        )
    )

    resolution = resolve_geofabrik_extract(extracts, _extent(lon=5.0, lat=5.0))

    assert resolution["run_status"] == "unresolved"
    assert resolution["source"] is None
    assert "No single Geofabrik regional extract" in resolution["error"]


def test_required_footprints_are_deterministic_and_cover_only_occupied_areas():
    fixes = [
        {"fix_key": "near-a", "lon": 0.01, "lat": 0.01},
        {"fix_key": "far", "lon": 12.51, "lat": 55.71},
        {"fix_key": "near-b", "lon": 0.02, "lat": 0.02},
    ]

    first = build_required_footprints(fixes, search_radius_m=50.0)
    second = build_required_footprints(list(reversed(fixes)), search_radius_m=50.0)

    assert first == second
    assert len(first) == 2
    assert [footprint["footprint_id"] for footprint in first] == sorted(
        footprint["footprint_id"] for footprint in first
    )
    assert sum(footprint["valid_fix_count"] for footprint in first) == 3
    assert all(footprint["coverage_model"] == OSM_COVERAGE_MODEL for footprint in first)
    assert first[0]["movement_bbox"]["east"] == 0.02
    assert first[0]["movement_bbox"]["west"] == 0.01
    assert first[0]["buffered_bbox"]["east"] < 1.0
    assert shape(first[0]["geometry"]).covers(shape(first[0]["geometry"]))


def test_required_footprints_enforce_occupied_cell_limit(monkeypatch):
    monkeypatch.setattr("examples.movement.osm_extracts.MAX_REQUIRED_FOOTPRINTS", 1)

    with pytest.raises(OSMExtractSourceError, match="occupied OSM footprints"):
        build_required_footprints(
            [{"lon": 0.0, "lat": 0.0}, {"lon": 10.0, "lat": 10.0}],
            search_radius_m=25.0,
        )

    assert MAX_REQUIRED_FOOTPRINTS >= 1


def test_multi_source_resolution_handles_dispersed_occupied_footprints():
    extracts = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "north-america",
                [[-170.0, 15.0], [-50.0, 15.0], [-50.0, 75.0], [-170.0, 75.0], [-170.0, 15.0]],
            ),
            _feature(
                "denmark",
                [[7.0, 54.0], [16.0, 54.0], [16.0, 58.0], [7.0, 58.0], [7.0, 54.0]],
            ),
        )
    )
    fixes = [
        {"fix_key": "north-america-fix", "lon": -80.0, "lat": 35.0},
        {"fix_key": "denmark-fix", "lon": 12.51, "lat": 55.71},
    ]

    giant_bbox_resolution = resolve_geofabrik_extract(
        extracts,
        build_required_extent(fixes, search_radius_m=50.0),
    )
    resolution = resolve_geofabrik_sources(
        extracts,
        build_required_footprints(fixes, search_radius_m=50.0),
    )

    assert giant_bbox_resolution["run_status"] == "unresolved"
    assert resolution["run_status"] == "resolved"
    assert resolution["resolution_mode"] == "occupied_footprints_consolidated"
    assert resolution["selected_region_ids"] == ["denmark", "north-america"]
    assert resolution["footprint_count"] == 2
    assert all(
        source["assigned_required_extent"]["footprint_count"] == 1
        for source in resolution["selected_sources"]
    )


def test_multi_source_resolution_consolidates_only_initially_required_regions():
    footprints = build_required_footprints(
        [
            {"lon": 0.1, "lat": 0.1},
            {"lon": 2.1, "lat": 0.1},
            {"lon": 4.1, "lat": 0.1},
        ],
        search_radius_m=10.0,
    )
    extracts = parse_geofabrik_extracts(
        _catalog(
            _feature("local-a", [[0.0, 0.0], [0.5, 0.0], [0.5, 0.5], [0.0, 0.5], [0.0, 0.0]]),
            _feature("local-b", [[2.0, 0.0], [2.5, 0.0], [2.5, 0.5], [2.0, 0.5], [2.0, 0.0]]),
            _feature(
                "required-broad",
                [[-1.0, -1.0], [5.0, -1.0], [5.0, 1.0], [-1.0, 1.0], [-1.0, -1.0]],
            ),
        )
    )

    resolution = resolve_geofabrik_sources(extracts, footprints)

    assert resolution["initial_region_ids"] == ["local-a", "local-b", "required-broad"]
    assert resolution["selected_region_ids"] == ["local-a", "local-b", "required-broad"]
    by_region = {
        source["region_id"]: source["assigned_footprint_ids"]
        for source in resolution["selected_sources"]
    }
    assert by_region["local-a"] == [footprints[0]["footprint_id"]]
    assert by_region["local-b"] == [footprints[1]["footprint_id"]]
    assert by_region["required-broad"] == [footprints[2]["footprint_id"]]
    assert sorted(
        footprint_id
        for ids in by_region.values()
        for footprint_id in ids
    ) == [
        footprint["footprint_id"] for footprint in footprints
    ]

    without_broad_need = resolve_geofabrik_sources(extracts, footprints[:2])
    assert without_broad_need["initial_region_ids"] == ["local-a", "local-b"]
    assert without_broad_need["selected_region_ids"] == ["local-a", "local-b"]


def test_multi_source_resolution_reports_uncovered_footprint():
    footprints = build_required_footprints(
        [{"lon": 0.1, "lat": 0.1}, {"lon": 10.1, "lat": 10.1}],
        search_radius_m=10.0,
    )
    extracts = parse_geofabrik_extracts(
        _catalog(
            _feature("first-only", [[0.0, 0.0], [0.5, 0.0], [0.5, 0.5], [0.0, 0.5], [0.0, 0.0]])
        )
    )

    resolution = resolve_geofabrik_sources(extracts, footprints)

    assert resolution["run_status"] == "unresolved"
    assert footprints[1]["footprint_id"] in resolution["error"]
    assert resolution["selected_sources"] == []


def test_multi_source_resolution_enforces_selected_source_guardrail(monkeypatch):
    footprints = build_required_footprints(
        [{"lon": 0.1, "lat": 0.1}, {"lon": 10.1, "lat": 10.1}],
        search_radius_m=10.0,
    )
    extracts = parse_geofabrik_extracts(
        _catalog(
            _feature("first", [[0.0, 0.0], [0.5, 0.0], [0.5, 0.5], [0.0, 0.5], [0.0, 0.0]]),
            _feature("second", [[10.0, 10.0], [10.5, 10.0], [10.5, 10.5], [10.0, 10.5], [10.0, 10.0]]),
        )
    )
    monkeypatch.setattr("examples.movement.osm_extracts.MAX_SELECTED_SOURCES", 1)

    resolution = resolve_geofabrik_sources(extracts, footprints)

    assert MAX_SELECTED_SOURCES >= 1
    assert resolution["run_status"] == "unresolved"
    assert "more than 1 regional extracts" in resolution["error"]


def test_tiny_footprint_requiring_oversized_source_is_excluded_before_download(tmp_path):
    fixes = [
        {"fix_key": f"local-{index}", "lon": 0.10 + index * 0.001, "lat": 0.10}
        for index in range(MIN_FIXES_TO_FORCE_LARGE_OSM_SOURCE)
    ] + [
        {"fix_key": "tiny-1", "lon": 50.10, "lat": 50.10},
        {"fix_key": "tiny-2", "lon": 50.11, "lat": 50.11},
    ]
    footprints = build_required_footprints(fixes, search_radius_m=50.0)
    extracts = parse_geofabrik_extracts(
        _catalog(
            _feature("local", [[0.0, 0.0], [0.5, 0.0], [0.5, 0.5], [0.0, 0.5], [0.0, 0.0]]),
            _feature(
                "continent",
                [[40.0, 40.0], [60.0, 40.0], [60.0, 60.0], [40.0, 60.0], [40.0, 40.0]],
            ),
        )
    )
    resolution = resolve_geofabrik_sources(extracts, footprints)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={"content-length": str(MAX_SOURCE_BYTES_FOR_TINY_FOOTPRINT + 1)},
            )
        raise AssertionError("Oversized tiny-footprint source must not be downloaded.")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = apply_tiny_footprint_source_guardrail(
            tmp_path,
            resolution,
            client=client,
        )

    updated = result["resolution"]
    assert result["run_status"] == "completed"
    assert updated["selected_region_ids"] == ["local"]
    assert updated["planned_footprint_count"] == 1
    assert updated["excluded_footprint_count"] == 1
    assert updated["context_not_planned_fix_count"] == 2
    assert updated["excluded_footprints"][0]["reason"] == TINY_FOOTPRINT_EXCLUSION_REASON
    assert updated["excluded_footprints"][0]["region_id"] == "continent"
    assert updated["excluded_footprints"][0]["source_size_bytes"] == MAX_SOURCE_BYTES_FOR_TINY_FOOTPRINT + 1
    assert calls == ["HEAD"]


def test_tiny_footprint_with_unknown_uncached_source_size_is_excluded(tmp_path):
    footprints = build_required_footprints(
        [
            {"fix_key": "tiny-1", "lon": 50.10, "lat": 50.10},
            {"fix_key": "tiny-2", "lon": 50.11, "lat": 50.11},
        ],
        search_radius_m=50.0,
    )
    extracts = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "continent",
                [[40.0, 40.0], [60.0, 40.0], [60.0, 60.0], [40.0, 60.0], [40.0, 40.0]],
            )
        )
    )
    resolution = resolve_geofabrik_sources(extracts, footprints)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "HEAD":
            return httpx.Response(200)
        raise AssertionError("Unknown-size tiny-footprint source must not be downloaded.")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = apply_tiny_footprint_source_guardrail(
            tmp_path,
            resolution,
            client=client,
        )

    assert result["run_status"] == "unresolved"
    assert "All occupied OSM footprints were excluded" in result["error"]
    assert result["resolution"]["excluded_footprint_count"] == 1
    assert result["resolution"]["excluded_footprints"][0]["source_size_known"] is False
    assert result["resolution"]["excluded_footprints"][0]["source_size_status"] == "head_unknown_size"
    assert calls == ["HEAD"]


def test_non_tiny_footprint_large_source_still_uses_confirmation_path(tmp_path):
    fixes = [
        {"fix_key": f"fix-{index}", "lon": 50.10 + index * 0.001, "lat": 50.10}
        for index in range(MIN_FIXES_TO_FORCE_LARGE_OSM_SOURCE)
    ]
    footprints = build_required_footprints(fixes, search_radius_m=50.0)
    extracts = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "continent",
                [[40.0, 40.0], [60.0, 40.0], [60.0, 60.0], [40.0, 60.0], [40.0, 40.0]],
            )
        )
    )
    resolution = resolve_geofabrik_sources(extracts, footprints)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={"content-length": str(MAX_UNCONFIRMED_DOWNLOAD_BYTES + 1)},
            )
        raise AssertionError("Large source must not be downloaded before confirmation.")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        guarded = apply_tiny_footprint_source_guardrail(tmp_path, resolution, client=client)
        result = prepare_multi_source_feature_caches(
            tmp_path,
            guarded["resolution"],
            client=client,
        )

    assert guarded["resolution"]["excluded_footprint_count"] == 0
    assert result["run_status"] == "confirmation_required"
    assert "Aggregate uncached OSM download size exceeds" in result["preflight"]["confirmation_reasons"][0]
    assert calls == ["HEAD"]


def test_cache_paths_keys_and_metadata_contract_are_deterministic(tmp_path):
    required_extent = _extent()
    revision = source_revision_key(
        pbf_url="https://download.example/south-australia-latest.osm.pbf",
        etag='"source-etag"',
        last_modified="Tue, 26 May 2026 03:55:09 GMT",
    )
    same_revision = source_revision_key(
        pbf_url="https://download.example/south-australia-latest.osm.pbf",
        etag='"source-etag"',
        last_modified="Tue, 26 May 2026 03:55:09 GMT",
    )
    extent_key = derived_extent_key(
        source_sha256="a" * 64,
        required_extent=required_extent,
    )

    assert revision == same_revision
    assert geofabrik_registry_paths(tmp_path)["index"] == (
        tmp_path / ".vibecleaning/osm/registry/geofabrik/index-v1.json"
    )
    assert geofabrik_source_paths(
        tmp_path,
        region_id="south-australia",
        source_revision=revision,
    )["pbf"] == (
        tmp_path
        / ".vibecleaning"
        / "osm"
        / "sources"
        / "geofabrik"
        / "south-australia"
        / revision
        / "source.osm.pbf"
    )
    assert derived_feature_paths(
        tmp_path,
        source_sha256="a" * 64,
        extent_key=extent_key,
    )["features"].name == "road_railway_features.geojsonl.gz"

    registry = registry_metadata(fetched_at="2026-05-26T00:00:00Z")
    source = source_metadata(
        {
            "region_id": "south-australia",
            "region_name": "South Australia",
            "pbf_url": "https://download.example/south-australia-latest.osm.pbf",
            "coverage_geometry": required_extent["geometry"],
        },
        source_revision=revision,
        downloaded_at="2026-05-26T00:00:00Z",
        sha256="a" * 64,
        cache_valid=True,
    )
    derived = derived_metadata(
        source_sha256="a" * 64,
        source_region_id="south-australia",
        required_extent=required_extent,
        feature_counts={"road": 3, "railway": 1},
        complete=True,
    )

    assert registry["catalog_url"] == GEOFABRIK_INDEX_URL
    assert source["source_type"] == OSM_SOURCE_TYPE
    assert source["cache_valid"] is True
    assert derived["schema_version"] == OSM_CACHE_SCHEMA_VERSION
    assert derived["layer_spec_version"] == OSM_LAYER_SPEC_VERSION
    assert derived["feature_counts"] == {"road": 3, "railway": 1}
    assert derived["complete"] is True


def test_local_context_and_extent_accept_radius_larger_than_overpass_fetch_limit():
    large_radius_m = MAX_CONTEXT_SEARCH_RADIUS_M + 1_000.0

    required_extent = build_required_extent(
        [{"fix_key": "fix-1", "lon": 138.55, "lat": -34.92}],
        search_radius_m=large_radius_m,
    )
    context = build_fix_osm_context(
        [{"fix_key": "fix-1", "lon": 138.55, "lat": -34.92}],
        {"road": [], "railway": []},
        search_radius_m=large_radius_m,
    )

    assert required_extent["search_radius_m"] == large_radius_m
    assert context["fix-1"]["osm:road_match_status"] == "not_found_within_radius"
    with pytest.raises(ValueError, match="must be <="):
        build_osm_fetch_scopes(
            [{"fix_key": "fix-1", "lon": 138.55, "lat": -34.92}],
            search_radius_m=large_radius_m,
        )


def test_local_extract_contract_has_no_fetch_or_anomaly_coupling():
    source = (REPO_ROOT / "examples" / "movement" / "osm_extracts.py").read_text(
        encoding="utf-8"
    )
    ranking = (REPO_ROOT / "examples" / "movement" / "anomaly_ranking.py").read_text(
        encoding="utf-8"
    )
    analysis = (
        REPO_ROOT / "examples" / "movement" / "anomaly_analysis_template.py"
    ).read_text(encoding="utf-8")

    assert "fetch_osm_features" not in source
    assert "anomaly_ranking" not in source
    assert "anomaly_analysis_template" not in source
    assert "fetch_osm_features" not in ranking
    assert "fetch_osm_features" not in analysis


def test_geofabrik_registry_is_cached_atomically_and_reused_without_network(tmp_path):
    payload = _catalog(
        _feature(
            "south-australia",
            [[129.0, -39.9], [141.1, -39.9], [141.1, -25.9], [129.0, -25.9], [129.0, -39.9]],
        )
    )
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, str(request.url)))
        return httpx.Response(
            200,
            content=json.dumps(payload).encode("utf-8"),
            headers={"etag": '"catalog-etag"', "last-modified": "Tue, 26 May 2026 03:55:09 GMT"},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        first = get_geofabrik_registry(
            tmp_path,
            client=client,
            fetched_at="2026-05-26T00:00:00+00:00",
        )
        second = get_geofabrik_registry(tmp_path, client=client)

    assert first["cache_hit"] is False
    assert second["cache_hit"] is True
    assert len(second["extracts"]) == 1
    assert calls == [("GET", GEOFABRIK_INDEX_URL)]
    assert first["paths"]["index"].is_file()
    assert first["paths"]["metadata"].is_file()
    assert first["metadata"]["etag"] == '"catalog-etag"'


def test_pbf_download_is_cached_and_valid_cached_source_is_reused(tmp_path):
    source = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "south-australia",
                [[129.0, -39.9], [141.1, -39.9], [141.1, -25.9], [129.0, -25.9], [129.0, -39.9]],
            )
        )
    )[0]
    pbf_content = b"cached pbf bytes"
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={
                    "content-length": str(len(pbf_content)),
                    "etag": '"pbf-etag"',
                    "last-modified": "Tue, 26 May 2026 03:55:09 GMT",
                },
            )
        return httpx.Response(200, content=pbf_content)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        first = cache_geofabrik_source_pbf(
            tmp_path,
            source,
            client=client,
            downloaded_at="2026-05-26T00:00:00+00:00",
        )
        second = cache_geofabrik_source_pbf(tmp_path, source, client=client)

    assert first["run_status"] == "completed"
    assert first["cache_hit"] is False
    assert second["cache_hit"] is True
    assert calls == ["HEAD", "GET"]
    assert first["paths"]["pbf"].read_bytes() == pbf_content
    assert first["metadata"]["sha256"] == hashlib.sha256(pbf_content).hexdigest()
    assert first["metadata"]["cache_valid"] is True


def test_pbf_download_retries_read_timeout_without_valid_partial_cache(tmp_path):
    source = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "georgia",
                [[-86.0, 30.0], [-80.0, 30.0], [-80.0, 36.0], [-86.0, 36.0], [-86.0, 30.0]],
            )
        )
    )[0]
    pbf_content = b"retry pbf bytes"
    calls = []
    get_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal get_count
        calls.append(request.method)
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={
                    "content-length": str(len(pbf_content)),
                    "etag": '"retry-etag"',
                    "last-modified": "Tue, 26 May 2026 03:55:09 GMT",
                },
            )
        get_count += 1
        if get_count == 1:
            raise httpx.ReadTimeout("simulated read timeout", request=request)
        return httpx.Response(200, content=pbf_content)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = cache_geofabrik_source_pbf(tmp_path, source, client=client)

    assert result["run_status"] == "completed"
    assert result["cache_hit"] is False
    assert calls == ["HEAD", "GET", "GET"]
    assert result["paths"]["pbf"].read_bytes() == pbf_content
    assert result["metadata"]["cache_valid"] is True
    assert result["warnings"]
    assert "Retrying Geofabrik source download" in result["warnings"][0]
    assert not list(result["paths"]["directory"].glob(".source.osm.pbf.*.tmp"))


def test_failed_pbf_download_does_not_create_a_valid_source_cache(tmp_path):
    source = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "south-australia",
                [[129.0, -39.9], [141.1, -39.9], [141.1, -25.9], [129.0, -25.9], [129.0, -39.9]],
            )
        )
    )[0]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={"content-length": "100", "etag": '"bad-download"'},
            )
        return httpx.Response(200, content=b"short")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(Exception, match="size mismatch"):
            cache_geofabrik_source_pbf(tmp_path, source, client=client)

    revision = source_revision_key(
        pbf_url=source["pbf_url"],
        etag='"bad-download"',
        last_modified=None,
    )
    paths = geofabrik_source_paths(
        tmp_path,
        region_id=source["region_id"],
        source_revision=revision,
    )
    assert not paths["pbf"].exists()
    assert not paths["metadata"].exists()


def test_invalid_cached_source_metadata_is_not_reused(tmp_path):
    source = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "south-australia",
                [[129.0, -39.9], [141.1, -39.9], [141.1, -25.9], [129.0, -25.9], [129.0, -39.9]],
            )
        )
    )[0]
    revision = "invalid-source"
    paths = geofabrik_source_paths(
        tmp_path,
        region_id=source["region_id"],
        source_revision=revision,
    )
    paths["directory"].mkdir(parents=True)
    paths["pbf"].write_bytes(b"corrupt")
    paths["metadata"].write_text(
        json.dumps(
            {
                "cache_valid": True,
                "region_id": source["region_id"],
                "pbf_url": source["pbf_url"],
                "source_revision": revision,
                "sha256": "not-the-file-checksum",
            }
        ),
        encoding="utf-8",
    )

    assert find_cached_geofabrik_source(tmp_path, source) is None


def test_large_uncached_pbf_requires_confirmation_before_download(tmp_path):
    source = parse_geofabrik_extracts(
        _catalog(
            _feature(
                "australia",
                [[110.0, -45.0], [155.0, -45.0], [155.0, -10.0], [110.0, -10.0], [110.0, -45.0]],
            )
        )
    )[0]
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={"content-length": str(MAX_UNCONFIRMED_DOWNLOAD_BYTES + 1)},
            )
        raise AssertionError("Large source must not be downloaded before confirmation")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = cache_geofabrik_source_pbf(tmp_path, source, client=client)

    assert result["run_status"] == "confirmation_required"
    assert result["estimated_content_length"] == MAX_UNCONFIRMED_DOWNLOAD_BYTES + 1
    assert calls == ["HEAD"]
    assert not result["paths"]["pbf"].exists()


def _write_fixture_pbf(
    path: Path,
    *,
    feature_id_offset: int = 0,
    ignored_way_id: int = 13,
):
    writer = osmium.SimpleWriter(str(path))
    try:
        nodes = [
            (1, 138.50, -34.90),
            (2, 138.60, -34.90),
            (3, 138.50, -34.91),
            (4, 138.60, -34.91),
            (5, 140.00, -34.90),
            (6, 140.10, -34.90),
            (7, 138.53, -34.89),
            (8, 138.54, -34.89),
        ]
        for node_id, lon, lat in nodes:
            writer.add_node(osmium.osm.mutable.Node(id=node_id, location=(lon, lat)))
        writer.add_way(
            osmium.osm.mutable.Way(
                id=10 + feature_id_offset, nodes=[1, 2], tags={"highway": "track"}
            )
        )
        writer.add_way(
            osmium.osm.mutable.Way(
                id=11 + feature_id_offset, nodes=[3, 4], tags={"railway": "rail"}
            )
        )
        writer.add_way(
            osmium.osm.mutable.Way(
                id=12 + feature_id_offset, nodes=[5, 6], tags={"highway": "motorway"}
            )
        )
        writer.add_way(
            osmium.osm.mutable.Way(id=ignored_way_id, nodes=[7, 8], tags={"building": "yes"})
        )
    finally:
        writer.close()


def _fixture_source(region_id: str) -> dict:
    return parse_geofabrik_extracts(
        _catalog(
            _feature(
                region_id,
                [[129.0, -39.9], [141.1, -39.9], [141.1, -25.9], [129.0, -25.9], [129.0, -39.9]],
            )
        )
    )[0]


def _multi_source_plan(*sources: dict, extent: dict | None = None) -> dict:
    assigned_extent = extent or build_required_extent(
        [{"fix_key": "fix-1", "lon": 138.55, "lat": -34.90}],
        search_radius_m=10_000.0,
    )
    return {
        "run_status": "resolved",
        "selected_sources": [
            {
                **source,
                "assigned_footprint_ids": [f"footprint-{index}"],
                "assigned_required_extent": assigned_extent,
            }
            for index, source in enumerate(sources)
        ],
    }


def _cache_fixture_source(
    tmp_path: Path,
    source: dict,
    *,
    feature_id_offset: int = 0,
    ignored_way_id: int = 13,
):
    revision = f"cached-{source['region_id']}"
    paths = geofabrik_source_paths(
        tmp_path,
        region_id=source["region_id"],
        source_revision=revision,
    )
    paths["directory"].mkdir(parents=True, exist_ok=True)
    _write_fixture_pbf(
        paths["pbf"],
        feature_id_offset=feature_id_offset,
        ignored_way_id=ignored_way_id,
    )
    sha256 = hashlib.sha256(paths["pbf"].read_bytes()).hexdigest()
    paths["metadata"].write_text(
        json.dumps(
            source_metadata(
                source,
                source_revision=revision,
                downloaded_at="2026-05-26T00:00:00+00:00",
                sha256=sha256,
                cache_valid=True,
            )
        ),
        encoding="utf-8",
    )


def test_pbf_extraction_writes_complete_clipped_road_railway_cache(tmp_path):
    pbf_path = tmp_path / "fixture.osm.pbf"
    _write_fixture_pbf(pbf_path)
    source_sha256 = hashlib.sha256(pbf_path.read_bytes()).hexdigest()
    source_cache = {
        "paths": {"pbf": pbf_path},
        "metadata": {
            "cache_valid": True,
            "sha256": source_sha256,
            "region_id": "south-australia",
            "coverage_geometry": _feature(
                "south-australia",
                [[129.0, -39.9], [141.1, -39.9], [141.1, -25.9], [129.0, -25.9], [129.0, -39.9]],
            )["geometry"],
        },
    }
    extent = build_required_extent(
        [{"fix_key": "fix-1", "lon": 138.55, "lat": -34.90}],
        search_radius_m=10_000.0,
    )

    result = extract_context_feature_cache(
        tmp_path,
        source_cache,
        extent,
        created_at="2026-05-26T00:00:00+00:00",
    )
    grouped = load_cached_context_features(result)
    reused = extract_context_feature_cache(tmp_path, source_cache, extent)

    assert result["cache_hit"] is False
    assert result["metadata"]["complete"] is True
    assert result["metadata"]["feature_counts"] == {"road": 1, "railway": 1}
    assert result["paths"]["features"].is_file()
    assert result["paths"]["metadata"].is_file()
    assert [feature["properties"]["tags"] for feature in grouped["road"]] == [
        {"highway": "track"}
    ]
    assert [feature["properties"]["tags"] for feature in grouped["railway"]] == [
        {"railway": "rail"}
    ]
    assert reused["cache_hit"] is True


def test_pbf_extraction_rejects_source_without_complete_extent_coverage(tmp_path):
    pbf_path = tmp_path / "fixture.osm.pbf"
    _write_fixture_pbf(pbf_path)
    source_cache = {
        "paths": {"pbf": pbf_path},
        "metadata": {
            "cache_valid": True,
            "sha256": hashlib.sha256(pbf_path.read_bytes()).hexdigest(),
            "region_id": "elsewhere",
            "coverage_geometry": _feature(
                "elsewhere",
                [[120.0, -39.9], [121.0, -39.9], [121.0, -38.9], [120.0, -38.9], [120.0, -39.9]],
            )["geometry"],
        },
    }
    extent = _extent()

    with pytest.raises(OSMExtractSourceError, match="coverage"):
        extract_context_feature_cache(tmp_path, source_cache, extent)

    assert not list((tmp_path / ".vibecleaning" / "osm" / "derived").rglob("*"))


def test_multi_source_cached_features_merge_completed_road_and_railway_layers(tmp_path):
    first_source = _fixture_source("first-region")
    second_source = _fixture_source("second-region")
    _cache_fixture_source(tmp_path, first_source)
    _cache_fixture_source(tmp_path, second_source, feature_id_offset=100, ignored_way_id=113)

    result = prepare_multi_source_feature_caches(
        tmp_path,
        _multi_source_plan(first_source, second_source),
    )

    assert result["run_status"] == "completed"
    assert result["complete"] is True
    assert result["feature_counts"] == {"road": 2, "railway": 2}
    assert result["duplicate_counts"] == {"road": 0, "railway": 0, "total": 0}
    assert [record["region_id"] for record in result["regional_caches"]] == [
        "first-region",
        "second-region",
    ]
    assert all(record["source_cache_hit"] for record in result["regional_caches"])


def test_multi_source_feature_merge_deduplicates_overlapping_osm_identities(tmp_path):
    first_source = _fixture_source("first-region")
    second_source = _fixture_source("second-region")
    _cache_fixture_source(tmp_path, first_source, ignored_way_id=13)
    _cache_fixture_source(tmp_path, second_source, ignored_way_id=113)

    result = prepare_multi_source_feature_caches(
        tmp_path,
        _multi_source_plan(first_source, second_source),
    )

    assert result["feature_counts"] == {"road": 1, "railway": 1}
    assert result["duplicate_counts"] == {"road": 1, "railway": 1, "total": 2}
    assert result["features_by_layer"]["road"][0]["properties"]["osm_id"] == 10
    assert result["features_by_layer"]["railway"][0]["properties"]["osm_id"] == 11
    assert result["regional_caches"][0]["duplicate_counts"]["total"] == 0
    assert result["regional_caches"][1]["duplicate_counts"] == {
        "road": 1,
        "railway": 1,
        "total": 2,
    }


def test_multi_source_preflight_requires_confirmation_for_aggregate_size_before_get(tmp_path):
    sources = [_fixture_source("first-region"), _fixture_source("second-region")]
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={"content-length": str((MAX_UNCONFIRMED_DOWNLOAD_BYTES // 2) + 1)},
            )
        raise AssertionError("No PBF GET may occur before aggregate download confirmation.")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = prepare_multi_source_feature_caches(
            tmp_path,
            _multi_source_plan(*sources),
            client=client,
        )

    assert result["run_status"] == "confirmation_required"
    assert result["preflight"]["uncached_source_count"] == 2
    assert result["preflight"]["aggregate_uncached_download_bytes"] > MAX_UNCONFIRMED_DOWNLOAD_BYTES
    assert calls == ["HEAD", "HEAD"]


def test_multi_source_preflight_requires_confirmation_for_unknown_size_before_get(tmp_path):
    source = _fixture_source("unknown-size")
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "HEAD":
            return httpx.Response(200)
        raise AssertionError("No PBF GET may occur when download size is unknown.")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = preflight_multi_source_caches(
            tmp_path,
            _multi_source_plan(source),
            client=client,
        )

    assert result["run_status"] == "confirmation_required"
    assert result["aggregate_uncached_download_bytes"] is None
    assert "Download size is unknown" in result["confirmation_reasons"][0]
    assert calls == ["HEAD"]


def test_multi_source_preflight_requires_confirmation_for_many_uncached_regions_before_get(tmp_path):
    sources = [
        _fixture_source(f"region-{index}")
        for index in range(MAX_UNCONFIRMED_SOURCE_COUNT + 1)
    ]
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "HEAD":
            return httpx.Response(200, headers={"content-length": "1"})
        raise AssertionError("No PBF GET may occur before source-count confirmation.")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = prepare_multi_source_feature_caches(
            tmp_path,
            _multi_source_plan(*sources),
            client=client,
        )

    assert result["run_status"] == "confirmation_required"
    assert result["preflight"]["uncached_source_count"] == MAX_UNCONFIRMED_SOURCE_COUNT + 1
    assert calls == ["HEAD"] * (MAX_UNCONFIRMED_SOURCE_COUNT + 1)


def test_multi_source_regional_extraction_failure_returns_no_complete_merge(tmp_path):
    bad_source = _fixture_source("bad-region")
    _cache_fixture_source(tmp_path, bad_source)
    outside_extent = build_required_extent(
        [{"fix_key": "outside", "lon": 12.51, "lat": 55.71}],
        search_radius_m=50.0,
    )

    result = prepare_multi_source_feature_caches(
        tmp_path,
        _multi_source_plan(bad_source, extent=outside_extent),
    )

    assert result["run_status"] == "failed"
    assert result["complete"] is False
    assert result["features_by_layer"] is None
    assert "bad-region" in result["error"]
    assert "coverage" in result["error"]


def test_derived_cache_keys_change_with_assigned_extent_and_extractor_version():
    source_sha256 = "a" * 64
    first_extent = _extent(lon=138.55, lat=-34.92)
    second_extent = _extent(lon=138.65, lat=-34.92)

    first_key = derived_extent_key(
        source_sha256=source_sha256,
        required_extent=first_extent,
    )
    assignment_changed = derived_extent_key(
        source_sha256=source_sha256,
        required_extent=second_extent,
    )
    extractor_changed = derived_extent_key(
        source_sha256=source_sha256,
        required_extent=first_extent,
        extractor_version="local-pbf-v2",
    )

    assert first_key != assignment_changed
    assert first_key != extractor_changed
