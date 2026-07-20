from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sqlite3


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE = ROOT / "data" / "movement_raw" / "synthetic_demo_cp2" / "synthetic_demo_cp2.csv"
DEFAULT_OUTPUT = ROOT / "examples" / "move_viz" / "sample_data" / "synthetic_demo_cp2.sqlite"


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def create_sample_database(source: Path, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    with source.open("r", encoding="utf-8-sig", newline="") as source_handle:
        reader = csv.DictReader(source_handle)
        columns = list(reader.fieldnames or [])
        if not columns:
            raise ValueError(f"CSV has no header: {source}")
        with sqlite3.connect(output) as connection:
            definitions = ", ".join(f"{quote_identifier(column)} TEXT" for column in columns)
            connection.execute(f"CREATE TABLE movement ({definitions})")
            placeholders = ", ".join("?" for _ in columns)
            column_sql = ", ".join(quote_identifier(column) for column in columns)
            connection.executemany(
                f"INSERT INTO movement ({column_sql}) VALUES ({placeholders})",
                ([row.get(column, "") for column in columns] for row in reader),
            )
            connection.execute('CREATE INDEX movement_individual_time ON movement ("individual-local-identifier", "timestamp")')


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the move_viz SQLite example from a movement_raw CSV.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    create_sample_database(args.source, args.output)
    print(args.output)


if __name__ == "__main__":
    main()
