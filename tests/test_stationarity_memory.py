"""Large RDS previews must not allocate Python records for the entire study."""

from contextlib import closing
import sqlite3
import tracemalloc

import pytest

from examples.movement import rds_index, stationarity


SPEC = {
    "kind": "stationarity", "radius_m": 100, "minimum_duration_s": 120,
    "maximum_gap_s": 120, "minimum_fixes": 3, "position": "anywhere",
}


def make_index(path, *, individuals=3, points=7, sources=2, interleaved=True):
    with closing(sqlite3.connect(path)) as db:
        db.executescript("""
            CREATE TABLE artifacts (artifact_id INTEGER, logical_name TEXT);
            CREATE TABLE individuals (individual_key INTEGER, identifier TEXT,
                                      individual_id TEXT, study_id TEXT);
            CREATE TABLE fixes (
                artifact_id INTEGER, individual_key INTEGER, fix_key TEXT,
                time_ms INTEGER, source_row INTEGER, lon REAL, lat REAL,
                burst_value INTEGER, tag_identifier TEXT,
                source_outlier_status TEXT, source_outlier_issue_type TEXT);
        """)
        db.executemany("INSERT INTO individuals VALUES (?, ?, ?, 'study')",
                       ((i, f"animal-{i}", str(i)) for i in range(individuals)))
        for source in range(sources):
            name = f"source-{source}.rds"
            db.execute("INSERT INTO artifacts VALUES (?, ?)", (source, name))

            def rows():
                for individual in range(individuals):
                    for point in range(points):
                        row = (point * individuals + individual + 1 if interleaved
                               else individual * points + point + 1)
                        yield (source, individual, f"file:{name}#row:{row}",
                               (points - point) * 60_000, row, source, individual,
                               1, "tag", "confirmed" if row == 2 else "", "Owner flag")

            db.executemany("INSERT INTO fixes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows())
        db.commit()
    return path


@pytest.mark.parametrize("algorithm", sorted(stationarity.SUPPORTED_ALGORITHMS))
def test_streaming_matches_full_study_scopes_and_input_history(tmp_path, algorithm):
    index = make_index(tmp_path / "tracks.sqlite")
    spec = {**SPEC, "algorithm": algorithm}
    gps = {"annotation_id": "gps", "status": "suspected", "issue_type": "GPS spikes",
           "scope": {"kind": "fix", "source_rows": [
               {"logical_name": "source-0.rds", "row_ranges": [[4, 7]]}]}}
    dismissal = {**gps, "annotation_id": "dismissal", "parent_annotation_id": "gps",
                 "status": "dismissed", "scope": {"kind": "fix", "source_rows": [
                     {"logical_name": "source-0.rds", "row_ranges": [[5, 5]]}]}}
    annotations = [gps, dismissal]
    records = rds_index.rds_stationarity_records(index, spec)
    matches, inputs = stationarity.evaluate_stationarity(records, spec, annotations)
    scope, count = rds_index.resolve_rds_review_scope(
        index, {"kind": "filter", "filter": spec}, annotations=annotations)
    assert count == len(matches) > 0
    assert scope["source_rows"] == rds_index.source_rows_from_fix_keys(matches)
    assert scope["stationarity_inputs"] == inputs
    assert len(inputs) == 6  # Distinct individuals AND distinct source files.


def test_preview_working_memory_is_bounded_for_many_tracks(tmp_path):
    index = make_index(tmp_path / "large.sqlite", individuals=48, points=1000,
                       sources=1, interleaved=False)
    spec = {**SPEC, "minimum_duration_s": 100_000}
    stationarity._SCAN_CACHE.clear()
    tracemalloc.start()
    try:
        scope, count = rds_index.resolve_rds_review_scope(
            index, {"kind": "filter", "filter": spec})
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        stationarity._SCAN_CACHE.clear()
    assert count == 0
    assert len(scope["stationarity_inputs"]) == 48
    # Generous allowance for one 1,000-fix track. The old whole-study record
    # lists and copies exceed this by several times, even without matches.
    assert peak < 20 * 1024 * 1024, f"Stationarity allocated {peak / 1024**2:.1f} MiB"


def test_rds_middle_and_end_periods_partition_whole_runs_per_individual(tmp_path):
    longitudes = [.1, .2, *([0] * 5), .3, .4, *([1] * 4), .5, .6, *([2] * 5), .7, .8]
    index = make_index(tmp_path / "periods.sqlite", individuals=2, points=len(longitudes),
                       sources=1, interleaved=False)
    with closing(sqlite3.connect(index)) as db:
        db.execute("UPDATE fixes SET source_outlier_status=''")
        for individual in range(2):
            db.executemany("UPDATE fixes SET lon=? WHERE source_row=?", [
                (lon, individual * len(longitudes) + i + 1) for i, lon in enumerate(longitudes)])
        db.commit()
    expected = {"ends": [*range(3, 8), *range(16, 21)], "middle": list(range(10, 14))}
    for position, rows in expected.items():
        for individual in range(2):
            scope, count = rds_index.resolve_rds_review_scope(index, {"kind": "filter", "filter": {
                **SPEC, "position": position, "individuals": [f"animal-{individual}"],
            }})
            assert count == len(rows)
            assert scope["source_rows"] == [{"logical_name": "source-0.rds", "row_ranges":
                stationarity.compressed_rows(row + individual * len(longitudes) for row in rows)}]
            assert scope["filter"]["position"] == position


def test_failed_scan_releases_connection_and_next_preview_can_run(tmp_path, monkeypatch):
    index = make_index(tmp_path / "tracks.sqlite")
    evaluate = rds_index.evaluate_stationarity

    def fail(*args, **kwargs):
        raise ValueError("scan failed")

    monkeypatch.setattr(rds_index, "evaluate_stationarity", fail)
    with pytest.raises(ValueError, match="scan failed"):
        rds_index.resolve_rds_review_scope(index, {"kind": "filter", "filter": SPEC})
    # An abandoned streaming cursor must not leave the disposable cache locked.
    with closing(sqlite3.connect(index, timeout=0)) as db:
        db.execute("BEGIN EXCLUSIVE")
        db.rollback()
    monkeypatch.setattr(rds_index, "evaluate_stationarity", evaluate)
    assert rds_index.resolve_rds_review_scope(index, {"kind": "filter", "filter": SPEC})[1] > 0


def test_cached_result_is_independent_and_invalidates_for_settings_reviews_and_source(tmp_path, monkeypatch):
    index = make_index(tmp_path / "tracks.sqlite")
    original = rds_index.evaluate_stationarity
    calls = []
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(rds_index, "evaluate_stationarity", counted)
    scope = {"kind": "filter", "filter": SPEC}
    first, count = rds_index.resolve_rds_review_scope(index, scope)
    expected = first["source_rows"].copy()
    calls.clear()
    first["source_rows"].clear()
    cached, cached_count = rds_index.resolve_rds_review_scope(index, scope)
    assert not calls
    assert cached_count == count and cached["source_rows"] == expected
    rds_index.resolve_rds_review_scope(index, {"kind": "filter", "filter": {**SPEC, "radius_m": 51}})
    assert calls
    calls.clear()
    rds_index.resolve_rds_review_scope(index, scope, annotations=[{
        "annotation_id": "gps", "status": "suspected", "issue_type": "GPS spikes",
        "scope": {"kind": "fix", "source_rows": [{"logical_name": "source-0.rds", "row_ranges": [[4, 7]]}]},
    }])
    assert calls
    calls.clear()
    with closing(sqlite3.connect(index)) as db:
        db.execute("UPDATE fixes SET lon=lon+1 WHERE source_row=1")
        db.commit()
    rds_index.resolve_rds_review_scope(index, scope)
    assert calls
