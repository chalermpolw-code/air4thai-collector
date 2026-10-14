#!/usr/bin/env python3
"""
Automatic Monthly CSV Exporter for Air4Thai SQLite Database.
Generates UTF-8-SIG CSV files partitioned by YYYY_MM so Thai station names render
cleanly in Excel, Python (pandas), and R.
"""

import argparse
import csv
import logging
from pathlib import Path
from typing import Iterable, List, Optional

from db import DEFAULT_DB_PATH, get_connection, init_db

DEFAULT_CSV_DIR = Path(__file__).resolve().parent.parent / "data" / "csv_monthly"

CSV_COLUMNS = [
    "obs_datetime",
    "obs_date",
    "obs_time",
    "station_id",
    "name_en",
    "name_th",
    "area_en",
    "area_th",
    "station_type",
    "lat",
    "lon",
    "aqi",
    "aqi_level",
    "pm25",
    "pm10",
    "o3",
    "co",
    "no2",
    "so2",
    "ws",
    "wd",
    "temp",
    "rh",
    "bp",
    "rain",
    "source_endpoint",
    "ingested_at",
]


def get_available_months(db_path: Path = DEFAULT_DB_PATH) -> List[str]:
    """Return all distinct YYYY-MM values present in the measurements table."""
    with get_connection(db_path) as conn:
        cur = conn.execute(
            "SELECT DISTINCT year_month FROM measurements ORDER BY year_month ASC;"
        )
        return [row["year_month"] for row in cur.fetchall() if row["year_month"]]


def export_months(
    months: Optional[Iterable[str]] = None,
    db_path: Path = DEFAULT_DB_PATH,
    csv_dir: Path = DEFAULT_CSV_DIR,
) -> List[Path]:
    """
    Export specified YYYY-MM months (or all months if None) from SQLite to monthly CSV files.
    Uses atomic write via a temporary file so interrupted exports never corrupt an existing CSV.
    """
    init_db(db_path)
    csv_dir.mkdir(parents=True, exist_ok=True)
    target_months = list(months) if months is not None else get_available_months(db_path)
    exported_files: List[Path] = []

    with get_connection(db_path) as conn:
        for ym in target_months:
            safe_ym = ym.replace("-", "_")
            out_path = csv_dir / f"air4thai_{safe_ym}.csv"
            tmp_path = csv_dir / f".air4thai_{safe_ym}.csv.tmp"

            cur = conn.execute(
                """
                SELECT
                    m.obs_datetime,
                    m.obs_date,
                    m.obs_time,
                    m.station_id,
                    s.name_en,
                    s.name_th,
                    s.area_en,
                    s.area_th,
                    s.station_type,
                    s.lat,
                    s.lon,
                    m.aqi,
                    m.aqi_level,
                    m.pm25,
                    m.pm10,
                    m.o3,
                    m.co,
                    m.no2,
                    m.so2,
                    m.ws,
                    m.wd,
                    m.temp,
                    m.rh,
                    m.bp,
                    m.rain,
                    m.source_endpoint,
                    m.ingested_at
                FROM measurements m
                LEFT JOIN stations s ON m.station_id = s.station_id
                WHERE m.year_month = ?
                ORDER BY m.obs_datetime ASC, m.station_id ASC;
                """,
                (ym,),
            )

            row_count = 0
            with open(tmp_path, mode="w", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(CSV_COLUMNS)
                for row in cur:
                    writer.writerow([row[col] for col in CSV_COLUMNS])
                    row_count += 1

            tmp_path.replace(out_path)
            exported_files.append(out_path)
            logging.info("Exported %s (%d rows)", out_path.name, row_count)

    return exported_files


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Export Air4Thai SQLite measurements to clean monthly CSV files."
    )
    parser.add_argument(
        "--month",
        action="append",
        help="Specific month(s) in YYYY-MM format to export (default: export all months).",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to SQLite database file.",
    )
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=DEFAULT_CSV_DIR,
        help="Output directory for monthly CSV files.",
    )
    args = parser.parse_args()
    export_months(months=args.month, db_path=args.db_path, csv_dir=args.csv_dir)


if __name__ == "__main__":
    main()
