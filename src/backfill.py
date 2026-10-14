#!/usr/bin/env python3
"""
Pre-2026 Historical Backfill Script for Air4Thai.
Pulls historical hourly measurements across all stations in resumable date chunks
from the Air4Thai Historical Data Portal API, upserts into SQLite, and exports monthly CSVs.
"""

import argparse
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, List, Tuple

from collector import (
    LIVE_ENDPOINTS,
    fetch_json_with_retry,
    fetch_station_history_window,
    get_known_station_ids,
)
from db import DEFAULT_DB_PATH, get_connection, init_db, upsert_measurements, upsert_stations
from export_csv import DEFAULT_CSV_DIR, export_months

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_FILE = BASE_DIR / "logs" / "backfill.log"


def date_chunks(
    start_date: datetime, end_date: datetime, chunk_days: int = 30
) -> Iterator[Tuple[datetime, datetime]]:
    """Yield (chunk_start, chunk_end) windows of at most chunk_days days."""
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end_date)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def is_chunk_completed(
    station_id: str, sdate: str, edate: str, db_path: Path = DEFAULT_DB_PATH
) -> bool:
    """Check whether this (station_id, sdate, edate) window was already backfilled."""
    with get_connection(db_path) as conn:
        cur = conn.execute(
            """
            SELECT 1 FROM backfill_checkpoints
            WHERE station_id = ? AND chunk_start = ? AND chunk_end = ?;
            """,
            (station_id, sdate, edate),
        )
        return cur.fetchone() is not None


def mark_chunk_completed(
    station_id: str,
    sdate: str,
    edate: str,
    rows_saved: int,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """Record completion of a backfill window in SQLite for crash-resumable execution."""
    now_iso = datetime.now(timezone.utc).isoformat()
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO backfill_checkpoints (
                station_id, chunk_start, chunk_end, rows_saved, completed_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(station_id, chunk_start, chunk_end) DO UPDATE SET
                rows_saved = excluded.rows_saved,
                completed_at = excluded.completed_at;
            """,
            (station_id, sdate, edate, rows_saved, now_iso),
        )


def run_backfill(
    start_date_str: str,
    end_date_str: str,
    chunk_days: int = 30,
    station_filter: List[str] | None = None,
    sleep_sec: float = 0.4,
    db_path: Path = DEFAULT_DB_PATH,
    csv_dir: Path = DEFAULT_CSV_DIR,
) -> None:
    """Backfill historical hourly Air4Thai data across stations with checkpoint resume."""
    init_db(db_path)

    # Ensure station list is populated first
    live_payload = fetch_json_with_retry(LIVE_ENDPOINTS)
    if live_payload and "stations" in live_payload:
        upsert_stations(live_payload["stations"], db_path=db_path)

    station_ids = station_filter or get_known_station_ids(db_path=db_path)
    if not station_ids:
        logging.error("No station IDs found. Pass --station or check network access to Air4Thai.")
        return

    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    windows = list(date_chunks(start_dt, end_dt, chunk_days=chunk_days))

    logging.info(
        "Starting backfill from %s to %s (%d windows x %d stations)...",
        start_date_str,
        end_date_str,
        len(windows),
        len(station_ids),
    )

    for w_idx, (c_start, c_end) in enumerate(windows, start=1):
        s_str = c_start.strftime("%Y-%m-%d")
        e_str = c_end.strftime("%Y-%m-%d")
        window_months = set()
        window_rows = 0

        for sid in station_ids:
            if is_chunk_completed(sid, s_str, e_str, db_path=db_path):
                continue
            records = fetch_station_history_window(sid, c_start, c_end)
            months = upsert_measurements(records, db_path=db_path)
            window_months.update(months)
            window_rows += len(records)
            mark_chunk_completed(sid, s_str, e_str, len(records), db_path=db_path)
            if sleep_sec > 0:
                time.sleep(sleep_sec)

        logging.info(
            "Window [%d/%d] %s..%s complete (%d rows saved).",
            w_idx,
            len(windows),
            s_str,
            e_str,
            window_rows,
        )
        if window_months:
            export_months(months=sorted(window_months), db_path=db_path, csv_dir=csv_dir)


def main() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    parser = argparse.ArgumentParser(
        description="Backfill pre-2026 historical hourly Air4Thai data into SQLite and monthly CSVs."
    )
    parser.add_argument(
        "--start-date",
        default="2021-01-01",
        help="Start date in YYYY-MM-DD format (default: 2021-01-01).",
    )
    parser.add_argument(
        "--end-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date in YYYY-MM-DD format (default: today).",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=30,
        help="Days per API request window (default: 30).",
    )
    parser.add_argument(
        "--station",
        action="append",
        help="Optional specific station ID(s) to backfill (e.g., --station 02t --station 35t). Defaults to all stations.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.4,
        help="Polite delay in seconds between API calls (default: 0.4).",
    )
    args = parser.parse_args()
    run_backfill(
        start_date_str=args.start_date,
        end_date_str=args.end_date,
        chunk_days=args.chunk_days,
        station_filter=args.station,
        sleep_sec=args.sleep,
    )


if __name__ == "__main__":
    main()
