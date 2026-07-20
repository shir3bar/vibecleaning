"""Stored app nearest-neighbor diagnostics for synthetic burst labels.

This module does not compute feature-space similarities. It only reads the
app-produced ``burst_feature_space.json``, joins fix-level synthetic labels from
the raw movement CSV onto app-produced bursts, and summarizes stored top-k
nearest-neighbor output.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any

import pandas as pd


BURST_LABEL_COLUMNS = [
    "burst_id",
    "individual_id",
    "burst_type",
    "n_fixes",
    "n_anomaly_fixes",
    "fraction_anomaly",
]
SIMILARITY_EVAL_COLUMNS = [
    "query_burst_id",
    "query_type",
    "nearest_same_type_rank",
    "same_type_count_top_k",
    "n_eligible_neighbors_top_k",
]

NORMAL_TYPE = "normal"
SYNTHETIC_IS_ANOMALY_COL = "synthetic:is_anomaly"
SYNTHETIC_ANOMALY_TYPE_COL = "synthetic:anomaly_type"

_EVENT_FIX_KEY_RE = re.compile(r"^id:(?P<event_id>.*?)#row:(?P<row_number>\d+)$")
_ROW_FIX_KEY_RE = re.compile(r"^row:(?P<row_number>\d+)(?:\||$)")


@dataclass(frozen=True)
class FixKeyReference:
    """Reference parsed from a vibecleaning fix key."""

    event_id: str | None
    row_number: int | None


@dataclass(frozen=True)
class StoredAppNeighborEvalTables:
    """Tables from stored app nearest-neighbor output plus synthetic labels."""

    burst_labels: pd.DataFrame
    similarity_eval: pd.DataFrame
    neighbor_type_matrix: pd.DataFrame


def parse_fix_key_reference(fix_key: object) -> FixKeyReference:
    """Parse event-id and/or 1-based CSV row number from a fix key."""
    text = str(fix_key or "")
    event_match = _EVENT_FIX_KEY_RE.match(text)
    if event_match:
        return FixKeyReference(
            event_id=event_match.group("event_id"),
            row_number=int(event_match.group("row_number")),
        )
    row_match = _ROW_FIX_KEY_RE.match(text)
    if row_match:
        return FixKeyReference(
            event_id=None,
            row_number=int(row_match.group("row_number")),
        )
    return FixKeyReference(event_id=None, row_number=None)


def evaluate_stored_app_neighbors(
    raw_csv_path: str | Path,
    feature_space: str | Path | dict[str, Any],
    *,
    include_same_individual: bool = False,
    top_k: int | None = 10,
) -> StoredAppNeighborEvalTables:
    """Evaluate stored app nearest-neighbor output with synthetic labels.

    ``top_k`` is applied after optional same-individual exclusion. These are
    stored top-k diagnostics for the app-produced nearest-neighbor list only.
    """
    feature_space_payload = load_feature_space(feature_space)
    burst_labels = build_burst_labels(raw_csv_path, feature_space_payload)
    similarity_eval = build_similarity_eval(
        feature_space_payload,
        burst_labels,
        include_same_individual=include_same_individual,
        top_k=top_k,
    )
    neighbor_type_matrix = build_neighbor_type_matrix(
        feature_space_payload,
        burst_labels,
        include_same_individual=include_same_individual,
        top_k=top_k,
    )
    return StoredAppNeighborEvalTables(
        burst_labels=burst_labels,
        similarity_eval=similarity_eval,
        neighbor_type_matrix=neighbor_type_matrix,
    )


def load_feature_space(feature_space: str | Path | dict[str, Any]) -> dict[str, Any]:
    """Load a feature-space payload from a path or return a shallow dict copy."""
    if isinstance(feature_space, dict):
        return dict(feature_space)
    path = Path(feature_space)
    return json.loads(path.read_text(encoding="utf-8"))


def build_burst_labels(
    raw_csv_path: str | Path,
    feature_space: str | Path | dict[str, Any],
) -> pd.DataFrame:
    """Join synthetic labels from raw CSV onto app feature-space bursts."""
    feature_space_payload = load_feature_space(feature_space)
    records = _load_synthetic_records(raw_csv_path)
    rows = []
    for point in _feature_space_points(feature_space_payload):
        burst_id = str(point.get("burst_id") or "")
        fix_keys = [str(value) for value in (point.get("fix_keys") or [])]
        fix_records = [_lookup_fix_record(records, fix_key) for fix_key in fix_keys]
        n_fixes = len(fix_records)
        anomaly_records = [
            record for record in fix_records if _record_is_anomalous(record)
        ]
        anomaly_types = sorted(
            {
                _record_anomaly_type(record)
                for record in anomaly_records
                if _record_anomaly_type(record) != NORMAL_TYPE
            }
        )
        if len(anomaly_types) > 1:
            raise ValueError(
                f"Burst {burst_id!r} contains multiple synthetic anomaly types: "
                + ", ".join(anomaly_types)
            )
        burst_type = anomaly_types[0] if anomaly_types else NORMAL_TYPE
        individual_id = str(point.get("individual") or "").strip()
        if not individual_id and fix_records:
            individual_id = _record_individual(fix_records[0])
        rows.append(
            {
                "burst_id": burst_id,
                "individual_id": individual_id,
                "burst_type": burst_type,
                "n_fixes": int(n_fixes),
                "n_anomaly_fixes": int(len(anomaly_records)),
                "fraction_anomaly": (
                    float(len(anomaly_records) / n_fixes) if n_fixes else 0.0
                ),
            }
        )
    return pd.DataFrame(rows, columns=BURST_LABEL_COLUMNS)


def build_similarity_eval(
    feature_space: str | Path | dict[str, Any],
    burst_labels: pd.DataFrame,
    *,
    include_same_individual: bool = False,
    top_k: int | None = 10,
) -> pd.DataFrame:
    """Evaluate app-stored top-k nearest-neighbor output for anomalous bursts."""
    feature_space_payload = load_feature_space(feature_space)
    labels_by_burst = _labels_by_burst(burst_labels)
    rows = []
    for query in _anomalous_label_rows(burst_labels):
        neighbors = _eligible_neighbor_labels(
            feature_space_payload,
            labels_by_burst,
            query,
            include_same_individual=include_same_individual,
            top_k=top_k,
        )
        same_type_ranks = [
            index
            for index, neighbor in enumerate(neighbors, start=1)
            if neighbor["burst_type"] == query["burst_type"]
        ]
        rows.append(
            {
                "query_burst_id": query["burst_id"],
                "query_type": query["burst_type"],
                "nearest_same_type_rank": (
                    same_type_ranks[0] if same_type_ranks else pd.NA
                ),
                "same_type_count_top_k": int(len(same_type_ranks)),
                "n_eligible_neighbors_top_k": int(len(neighbors)),
            }
        )
    return pd.DataFrame(rows, columns=SIMILARITY_EVAL_COLUMNS)


def build_neighbor_type_matrix(
    feature_space: str | Path | dict[str, Any],
    burst_labels: pd.DataFrame,
    *,
    include_same_individual: bool = False,
    top_k: int | None = 10,
) -> pd.DataFrame:
    """Count app-stored top-k neighbor types by anomalous query burst type."""
    feature_space_payload = load_feature_space(feature_space)
    labels_by_burst = _labels_by_burst(burst_labels)
    counts: dict[tuple[str, str], int] = {}
    query_types = set()
    neighbor_types = set()
    for query in _anomalous_label_rows(burst_labels):
        query_type = str(query["burst_type"])
        query_types.add(query_type)
        neighbors = _eligible_neighbor_labels(
            feature_space_payload,
            labels_by_burst,
            query,
            include_same_individual=include_same_individual,
            top_k=top_k,
        )
        for neighbor in neighbors:
            neighbor_type = str(neighbor["burst_type"])
            neighbor_types.add(neighbor_type)
            key = (query_type, neighbor_type)
            counts[key] = counts.get(key, 0) + 1

    matrix = pd.DataFrame(
        0,
        index=sorted(query_types),
        columns=sorted(neighbor_types),
        dtype=int,
    )
    for (query_type, neighbor_type), count in counts.items():
        matrix.loc[query_type, neighbor_type] = int(count)
    matrix.index.name = "query_type"
    matrix.columns.name = "neighbor_burst_type"
    return matrix


def _load_synthetic_records(raw_csv_path: str | Path) -> dict[str, Any]:
    path = Path(raw_csv_path)
    raw = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [
        column
        for column in (SYNTHETIC_IS_ANOMALY_COL, SYNTHETIC_ANOMALY_TYPE_COL)
        if column not in raw.columns
    ]
    if missing:
        raise ValueError("Synthetic movement CSV is missing: " + ", ".join(missing))

    row_by_number: dict[int, dict[str, Any]] = {}
    row_by_event_id: dict[str, dict[str, Any]] = {}
    for position, record in enumerate(raw.to_dict("records"), start=1):
        record["_synthetic_row_number"] = position
        row_by_number[position] = record
        event_id = str(record.get("event-id") or "").strip()
        if event_id:
            row_by_event_id[event_id] = record
    return {
        "row_by_number": row_by_number,
        "row_by_event_id": row_by_event_id,
    }


def _lookup_fix_record(records: dict[str, Any], fix_key: object) -> dict[str, Any]:
    reference = parse_fix_key_reference(fix_key)
    if reference.event_id is not None:
        record = records["row_by_event_id"].get(reference.event_id)
        if record is not None:
            return record
    if reference.row_number is not None:
        record = records["row_by_number"].get(reference.row_number)
        if record is not None:
            return record
    raise KeyError(f"Could not map fix_key to synthetic CSV row: {fix_key!r}")


def _feature_space_points(feature_space: dict[str, Any]) -> list[dict[str, Any]]:
    points = feature_space.get("points")
    if not isinstance(points, list):
        raise ValueError("Feature-space payload must include a points list")
    return [dict(point) for point in points]


def _feature_space_points_by_burst(
    feature_space: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    points_by_burst: dict[str, dict[str, Any]] = {}
    for point in _feature_space_points(feature_space):
        burst_id = str(point.get("burst_id") or "")
        if not burst_id:
            raise ValueError("Feature-space point is missing burst_id")
        if burst_id in points_by_burst:
            raise ValueError(f"Duplicate feature-space burst_id: {burst_id}")
        points_by_burst[burst_id] = point
    return points_by_burst


def _stored_neighbors(
    feature_space: dict[str, Any],
    burst_id: str,
) -> list[dict[str, Any]]:
    point = _feature_space_points_by_burst(feature_space).get(burst_id)
    if point is None:
        raise ValueError(f"Query burst not found in feature-space points: {burst_id}")
    raw_neighbors = point.get("nearest_neighbors")
    if raw_neighbors is None:
        raw_neighbors = dict(feature_space.get("nearest_neighbors") or {}).get(
            burst_id,
            [],
        )
    if not isinstance(raw_neighbors, list):
        raise ValueError(f"Invalid nearest_neighbors for burst: {burst_id}")
    neighbors = []
    for index, raw_neighbor in enumerate(raw_neighbors, start=1):
        if not isinstance(raw_neighbor, dict):
            raise ValueError(f"Invalid neighbor record for burst: {burst_id}")
        neighbor = dict(raw_neighbor)
        neighbor["burst_id"] = str(neighbor.get("burst_id") or "")
        neighbor["rank"] = _neighbor_rank(neighbor.get("rank"), index)
        neighbors.append(neighbor)
    neighbors.sort(
        key=lambda item: (
            int(item["rank"]),
            float(item.get("distance", 0.0) or 0.0),
            str(item.get("burst_id") or ""),
        )
    )
    return neighbors


def _neighbor_rank(raw_rank: object, fallback: int) -> int:
    if isinstance(raw_rank, bool):
        return fallback
    try:
        rank = int(raw_rank)
    except (TypeError, ValueError):
        return fallback
    return rank if rank > 0 else fallback


def _labels_by_burst(burst_labels: pd.DataFrame) -> dict[str, dict[str, Any]]:
    return {
        str(row["burst_id"]): dict(row)
        for row in burst_labels.to_dict("records")
    }


def _anomalous_label_rows(burst_labels: pd.DataFrame) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in burst_labels.to_dict("records")
        if str(row.get("burst_type") or NORMAL_TYPE) != NORMAL_TYPE
    ]
    rows.sort(key=lambda item: str(item["burst_id"]))
    return rows


def _eligible_neighbor_labels(
    feature_space: dict[str, Any],
    labels_by_burst: dict[str, dict[str, Any]],
    query: dict[str, Any],
    *,
    include_same_individual: bool,
    top_k: int | None,
) -> list[dict[str, Any]]:
    query_burst_id = str(query["burst_id"])
    query_individual = str(query["individual_id"])
    eligible = []
    for neighbor in _stored_neighbors(feature_space, query_burst_id):
        neighbor_burst_id = str(neighbor.get("burst_id") or "")
        if neighbor_burst_id not in labels_by_burst:
            raise ValueError(
                f"Neighbor burst {neighbor_burst_id!r} not found in burst labels"
            )
        neighbor_label = labels_by_burst[neighbor_burst_id]
        if (
            not include_same_individual
            and str(neighbor_label["individual_id"]) == query_individual
        ):
            continue
        eligible.append(neighbor_label)
        if top_k is not None and len(eligible) >= int(top_k):
            break
    return eligible


def _record_is_anomalous(record: dict[str, Any]) -> bool:
    anomaly_type = _record_anomaly_type(record)
    return (
        _parse_bool(record.get(SYNTHETIC_IS_ANOMALY_COL))
        or anomaly_type != NORMAL_TYPE
    )


def _record_anomaly_type(record: dict[str, Any]) -> str:
    value = str(record.get(SYNTHETIC_ANOMALY_TYPE_COL) or "").strip()
    return value or NORMAL_TYPE


def _record_individual(record: dict[str, Any]) -> str:
    return str(
        record.get("individual-local-identifier")
        or record.get("individual")
        or ""
    ).strip()


def _parse_bool(value: object) -> bool:
    text = str(value or "").strip().lower()
    return text in {"true", "t", "yes", "y", "1"}
