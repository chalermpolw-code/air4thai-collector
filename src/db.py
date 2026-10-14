#!/usr/bin/env python3
"""
Database schema and atomic upsert utilities for Air4Thai 5-Year Collector (2026-2031).
Uses SQLite in WAL mode with strict (station_id, obs_datetime) deduplication.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "air4thai.db"

PARAMETERS = [
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
]


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Open a resilient SQLite connection configured with WAL mode."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Create stations, measurements, and backfill_state tables if they do not exist."""
    with get_connection(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS stations (
                station_id TEXT PRIMARY KEY,
                name_th TEXT,
                name_en TEXT,
                area_th TEXT,
                area_en TEXT,
                station_type TEXT,
                lat REAL,
                lon REAL,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS measurements (
                station_id TEXT NOT NULL,
                obs_datetime TEXT NOT NULL,
                obs_date TEXT NOT NULL,
                obs_time TEXT NOT NULL,
                year_month TEXT NOT NULL,
                aqi INTEGER,
                aqi_level INTEGER,
                pm25 REAL,
                pm10 REAL,
                o3 REAL,
                co REAL,
                no2 REAL,
                so2 REAL,
                ws REAL,
                wd REAL,
                temp REAL,
                rh REAL,
                bp REAL,
                rain REAL,
                source_endpoint TEXT,
                ingested_at TEXT NOT NULL,
                PRIMARY KEY (station_id, obs_datetime)
            );

            CREATE INDEX IF NOT EXISTS idx_measurements_year_month
                ON measurements (year_month, obs_datetime, station_id);

            CREATE INDEX IF NOT EXISTS idx_measurements_obs_date
                ON measurements (obs_date);

            CREATE TABLE IF NOT EXISTS backfill_checkpoints (
                station_id TEXT NOT NULL,
                chunk_start TEXT NOT NULL,
                chunk_end TEXT NOT NULL,
                rows_saved INTEGER NOT NULL DEFAULT 0,
                completed_at TEXT NOT NULL,
                PRIMARY KEY (station_id, chunk_start, chunk_end)
            );
            """
        )


def parse_float(val: Any) -> Optional[float]:
    """Convert Air4Thai string/numeric reading to float, treating '-', '', None, or negative sentinels as None."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if val >= 0 else None
    s = str(val).strip()
    if s in ("", "-", "N/A", "null", "None", "-1", "-999"):
        return None
    try:
        num = float(s)
        return num if num >= 0 else None
    except ValueError:
        return None


def parse_int(val: Any) -> Optional[int]:
    """Convert Air4Thai AQI reading to int if valid."""
    f = parse_float(val)
    return int(round(f)) if f is not None else None


def upsert_stations(stations: Iterable[Dict[str, Any]], db_path: Path = DEFAULT_DB_PATH) -> int:
    """Insert or update station metadata records."""
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for st in stations:
        station_id = str(st.get("stationID") or st.get("station_id") or "").strip()
        if not station_id:
            continue
        rows.append(
            (
                station_id,
                st.get("nameTH") or st.get("name_th"),
                st.get("nameEN") or st.get("name_en"),
                st.get("areaTH") or st.get("area_th"),
                st.get("areaEN") or st.get("area_en"),
                st.get("stationType") or st.get("station_type"),
                parse_float(st.get("lat")),
                parse_float(st.get("long") if "long" in st else st.get("lon")),
                now_iso,
            )
        )
    if not rows:
        return 0

    with get_connection(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO stations (
                station_id, name_th, name_en, area_th, area_en, station_type, lat, lon, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id) DO UPDATE SET
                name_th = COALESCE(excluded.name_th, stations.name_th),
                name_en = COALESCE(excluded.name_en, stations.name_en),
                area_th = COALESCE(excluded.area_th, stations.area_th),
                area_en = COALESCE(excluded.area_en, stations.area_en),
                station_type = COALESCE(excluded.station_type, stations.station_type),
                lat = COALESCE(excluded.lat, stations.lat),
                lon = COALESCE(excluded.lon, stations.lon),
                updated_at = excluded.updated_at;
            """,
            rows,
        )
    return len(rows)


def upsert_measurements(records: Iterable[Dict[str, Any]], db_path: Path = DEFAULT_DB_PATH) -> List[str]:
    """
    Atomically insert or update hourly/daily station measurements.
    Uses COALESCE on conflict so delayed sensor parameters fill in NULLs without overwriting valid values.
    Returns sorted list of affected YYYY-MM strings.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    affected_months = set()
    rows = []

    for rec in records:
        station_id = str(rec.get("station_id") or "").strip()
        obs_datetime = str(rec.get("obs_datetime") or "").strip()
        if not station_id or len(obs_datetime) < 10:
            continue
        obs_date = obs_datetime[:10]
        obs_time = obs_datetime[11:16] if len(obs_datetime) >= 16 else "00:00"
        year_month = obs_date[:7]
        affected_months.add(year_month)

        rows.append(
            (
                station_id,
                obs_datetime,
                obs_date,
                obs_time,
                year_month,
                parse_int(rec.get("aqi")),
                parse_int(rec.get("aqi_level")),
                parse_float(rec.get("pm25")),
                parse_float(rec.get("pm10")),
                parse_float(rec.get("o3")),
                parse_float(rec.get("co")),
                parse_float(rec.get("no2")),
                parse_float(rec.get("so2")),
                parse_float(rec.get("ws")),
                parse_float(rec.get("wd")),
                parse_float(rec.get("temp")),
                parse_float(rec.get("rh")),
                parse_float(rec.get("bp")),
                parse_float(rec.get("rain")),
                rec.get("source_endpoint", "live_json"),
                now_iso,
            )
        )

    if not rows:
        return []

    with get_connection(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO measurements (
                station_id, obs_datetime, obs_date, obs_time, year_month,
                aqi, aqi_level, pm25, pm10, o3, co, no2, so2,
                ws, wd, temp, rh, bp, rain, source_endpoint, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id, obs_datetime) DO UPDATE SET
                aqi = COALESCE(excluded.aqi, measurements.aqi),
                aqi_level = COALESCE(excluded.aqi_level, measurements.aqi_level),
                pm25 = COALESCE(excluded.pm25, measurements.pm25),
                pm10 = COALESCE(excluded.pm10, measurements.pm10),
                o3 = COALESCE(excluded.o3, measurements.o3),
                co = COALESCE(excluded.co, measurements.co),
                no2 = COALESCE(excluded.no2, measurements.no2),
                so2 = COALESCE(excluded.so2, measurements.so2),
                ws = COALESCE(excluded.ws, measurements.ws),
                wd = COALESCE(excluded.wd, measurements.wd),
                temp = COALESCE(excluded.temp, measurements.temp),
                rh = COALESCE(excluded.rh, measurements.rh),
                bp = COALESCE(excluded.bp, measurements.bp),
                rain = COALESCE(excluded.rain, measurements.rain),
                source_endpoint = excluded.source_endpoint,
                ingested_at = excluded.ingested_at;
            """,
            rows,
        )

    return sorted(affected_months)
