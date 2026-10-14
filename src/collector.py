#!/usr/bin/env python3
"""
Air4Thai Live + Rolling 7-Day Collector (2026-2031).
1. Pulls real-time station snapshots from Air4Thai JSON endpoints.
2. Pulls a rolling lookback window (default: 7 days) of hourly station measurements
   from the Air4Thai historical API so any hours missed while the Mac was asleep
   or offline are automatically backfilled and deduplicated.
3. Archives compressed raw JSON responses (optional) and exports updated monthly CSVs.
"""

import argparse
import gzip
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from db import DEFAULT_DB_PATH, get_connection, init_db, upsert_measurements, upsert_stations
from export_csv import DEFAULT_CSV_DIR, export_months

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_ARCHIVE_DIR = BASE_DIR / "data" / "raw_archive"
LOG_FILE = BASE_DIR / "logs" / "collector.log"

LIVE_ENDPOINTS = [
    "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php",
    "https://air4thai.pcd.go.th/services/getNewAQI_JSON.php",
    "http://air4thai.pcd.go.th/forappV2/getAQI_JSON.php",
]

HISTORY_ENDPOINTS = [
    "http://air4thai.pcd.go.th/webV2/history_api.php",
    "https://air4thai.pcd.go.th/webV2/history_api.php",
]

HISTORY_PARAMS = "PM25,PM10,O3,CO,NO2,SO2,WS,WD,TEMP,RH,BP,RAIN"


def fetch_json_with_retry(
    urls: List[str],
    params: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    backoff: float = 2.0,
    timeout: int = 30,
) -> Optional[Dict[str, Any]]:
    """Fetch JSON from a list of fallback URLs with exponential backoff."""
    headers = {
        "User-Agent": "Air4Thai-Research-Collector/1.0 (Academic Air Quality Modeling)",
        "Accept": "application/json, text/plain, */*",
        "Cache-Control": "no-cache",
    }
    for attempt in range(1, retries + 1):
        for base_url in urls:
            full_url = f"{base_url}?{urlencode(params)}" if params else base_url
            try:
                req = Request(full_url, headers=headers)
                with urlopen(req, timeout=timeout) as resp:
                    raw = resp.read().decode("utf-8", errors="replace").strip()
                    if not raw:
                        continue
                    return json.loads(raw)
            except (URLError, json.JSONDecodeError, TimeoutError, OSError) as exc:
                logging.debug("Attempt %d failed for %s: %s", attempt, full_url, exc)
        if attempt < retries:
            time.sleep(backoff ** attempt)
    return None


def extract_live_records(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract station measurements from getNewAQI_JSON.php payload."""
    stations = payload.get("stations") or []
    records: List[Dict[str, Any]] = []

    for st in stations:
        station_id = str(st.get("stationID") or "").strip()
        if not station_id:
            continue

        # Air4Thai JSON v2/v3 places latest values under 'LastUpdate' or 'AQILast'
        last = st.get("LastUpdate") or st.get("AQILast") or {}
        obs_date = str(last.get("date") or "").strip()
        obs_time = str(last.get("time") or "00:00").strip()
        if not obs_date:
            continue

        obs_datetime = f"{obs_date} {obs_time}:00" if len(obs_time) == 5 else f"{obs_date} {obs_time}"
        aqi_block = last.get("AQI") or {}

        def _val(param_key: str) -> Any:
            p = last.get(param_key)
            if isinstance(p, dict):
                return p.get("value")
            return p

        records.append(
            {
                "station_id": station_id,
                "obs_datetime": obs_datetime,
                "aqi": aqi_block.get("aqi") if isinstance(aqi_block, dict) else None,
                "aqi_level": aqi_block.get("Level") or aqi_block.get("color_id")
                if isinstance(aqi_block, dict)
                else None,
                "pm25": _val("PM25"),
                "pm10": _val("PM10"),
                "o3": _val("O3"),
                "co": _val("CO"),
                "no2": _val("NO2"),
                "so2": _val("SO2"),
                "ws": _val("WS"),
                "wd": _val("WD"),
                "temp": _val("TEMP"),
                "rh": _val("RH"),
                "bp": _val("BP"),
                "rain": _val("RAIN"),
                "source_endpoint": "live_json",
            }
        )
    return records


def fetch_station_history_window(
    station_id: str,
    start_dt: datetime,
    end_dt: datetime,
) -> List[Dict[str, Any]]:
    """
    Query Air4Thai history_api.php for a single station over [start_dt, end_dt].
    Returns normalized hourly measurement dicts ready for SQLite upsert.
    """
    params = {
        "stationID": station_id,
        "param": HISTORY_PARAMS,
        "type": "hr",
        "sdate": start_dt.strftime("%Y-%m-%d"),
        "edate": end_dt.strftime("%Y-%m-%d"),
        "stime": "00",
        "etime": "23",
    }
    payload = fetch_json_with_retry(HISTORY_ENDPOINTS, params=params, retries=2, backoff=1.5)
    if not payload or payload.get("result") == "Error":
        return []

    stations_list = payload.get("stations") or []
    records: List[Dict[str, Any]] = []
    for st_entry in stations_list:
        sid = str(st_entry.get("stationID") or station_id).strip()
        for row in st_entry.get("data") or []:
            raw_dt = str(row.get("DATETIMEDATA") or "").strip()
            if len(raw_dt) < 10:
                continue
            obs_datetime = raw_dt if len(raw_dt) >= 16 else f"{raw_dt[:10]} 00:00:00"
            records.append(
                {
                    "station_id": sid,
                    "obs_datetime": obs_datetime,
                    "aqi": row.get("AQI"),
                    "aqi_level": None,
                    "pm25": row.get("PM25"),
                    "pm10": row.get("PM10"),
                    "o3": row.get("O3"),
                    "co": row.get("CO"),
                    "no2": row.get("NO2"),
                    "so2": row.get("SO2"),
                    "ws": row.get("WS"),
                    "wd": row.get("WD"),
                    "temp": row.get("TEMP"),
                    "rh": row.get("RH"),
                    "bp": row.get("BP"),
                    "rain": row.get("RAIN"),
                    "source_endpoint": "history_api",
                }
            )
    return records


def save_raw_snapshot(payload: Dict[str, Any], archive_dir: Path = RAW_ARCHIVE_DIR) -> Path:
    """Save compressed JSON snapshot partitioned by YYYY/MM for long-term auditability."""
    now = datetime.now()
    month_dir = archive_dir / now.strftime("%Y") / now.strftime("%m")
    month_dir.mkdir(parents=True, exist_ok=True)
    out_file = month_dir / f"air4thai_{now.strftime('%Y%m%d_%H%M%S')}.json.gz"
    with gzip.open(out_file, "wt", encoding="utf-8") as gz:
        json.dump(payload, gz, ensure_ascii=False)
    return out_file


def get_known_station_ids(db_path: Path = DEFAULT_DB_PATH) -> List[str]:
    """Return all station IDs known in the SQLite stations table."""
    with get_connection(db_path) as conn:
        cur = conn.execute("SELECT station_id FROM stations ORDER BY station_id ASC;")
        return [r["station_id"] for r in cur.fetchall()]


def run_collection(
    lookback_days: int = 7,
    save_raw: bool = True,
    db_path: Path = DEFAULT_DB_PATH,
    csv_dir: Path = DEFAULT_CSV_DIR,
    station_sleep: float = 0.25,
) -> None:
    """Execute live snapshot collection + rolling lookback catch-up + monthly CSV export."""
    init_db(db_path)
    affected_months = set()

    logging.info("Fetching live station data from Air4Thai JSON API...")
    live_payload = fetch_json_with_retry(LIVE_ENDPOINTS)
    if live_payload and "stations" in live_payload:
        if save_raw:
            snap_path = save_raw_snapshot(live_payload)
            logging.info("Saved compressed raw snapshot to %s", snap_path.name)

        stations_count = upsert_stations(live_payload["stations"], db_path=db_path)
        live_records = extract_live_records(live_payload)
        months = upsert_measurements(live_records, db_path=db_path)
        affected_months.update(months)
        logging.info(
            "Upserted %d stations and %d live station readings.",
            stations_count,
            len(live_records),
        )
    else:
        logging.warning("Live endpoint did not return station list; proceeding with known stations.")

    if lookback_days > 0:
        station_ids = get_known_station_ids(db_path=db_path)
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=lookback_days)
        logging.info(
            "Running %d-day rolling hourly catch-up (%s to %s) across %d stations...",
            lookback_days,
            start_dt.strftime("%Y-%m-%d"),
            end_dt.strftime("%Y-%m-%d"),
            len(station_ids),
        )
        total_hist_rows = 0
        for idx, sid in enumerate(station_ids, start=1):
            hist_records = fetch_station_history_window(sid, start_dt, end_dt)
            if hist_records:
                months = upsert_measurements(hist_records, db_path=db_path)
                affected_months.update(months)
                total_hist_rows += len(hist_records)
            if station_sleep > 0 and idx < len(station_ids):
                time.sleep(station_sleep)
        logging.info("Rolling lookback complete: upserted %d hourly records.", total_hist_rows)

    if affected_months:
        export_months(months=sorted(affected_months), db_path=db_path, csv_dir=csv_dir)


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
        description="Collect live and rolling-window Air4Thai station measurements into SQLite and monthly CSVs."
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Number of past days of hourly data to fetch and deduplicate (default: 7, use 0 for live snapshot only).",
    )
    parser.add_argument(
        "--no-raw",
        action="store_true",
        help="Skip saving compressed raw JSON archive files (useful for lightweight cloud runs).",
    )
    parser.add_argument(
        "--station-sleep",
        type=float,
        default=0.25,
        help="Delay in seconds between station history API calls (default: 0.25).",
    )
    args = parser.parse_args()
    run_collection(
        lookback_days=args.lookback_days,
        save_raw=not args.no_raw,
        station_sleep=args.station_sleep,
    )


if __name__ == "__main__":
    main()
