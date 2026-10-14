#!/usr/bin/env python3
"""
Bi-directional Google Drive Sync for Air4Thai Collector (Zero External Dependencies).
Syncs `data/air4thai.db` and `data/csv_monthly/*.csv` directly with the user's
`air4thai_collector` Google Drive folder (ID: 1XncnjNCREBk-nJKdyYDqMgVI7jz1NWJE)
so GitHub Actions pushes every 6-hour update straight into Google Drive without git pull.
"""

import argparse
import json
import logging
import os
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_DRIVE_FOLDER_ID = "1XncnjNCREBk-nJKdyYDqMgVI7jz1NWJE"
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = BASE_DIR / "data" / "air4thai.db"
DEFAULT_CSV_DIR = BASE_DIR / "data" / "csv_monthly"


def get_access_token() -> Optional[str]:
    """
    Obtain a Google Drive API OAuth2 access token from environment variables:
      1. GDRIVE_CLIENT_ID + GDRIVE_CLIENT_SECRET + GDRIVE_REFRESH_TOKEN (Pure stdlib)
      2. GDRIVE_SERVICE_ACCOUNT_JSON (via google-auth if installed)
    """
    client_id = os.environ.get("GDRIVE_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GDRIVE_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get("GDRIVE_REFRESH_TOKEN", "").strip()

    if client_id and client_secret and refresh_token:
        data = urllib.parse.urlencode(
            {
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            "https://oauth2.googleapis.com/token",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return payload["access_token"]

    sa_json = os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON", "").strip()
    if sa_json:
        try:
            from google.oauth2 import service_account
            from google.auth.transport.requests import Request as GoogleAuthRequest

            info = json.loads(sa_json)
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/drive"]
            )
            creds.refresh(GoogleAuthRequest())
            return creds.token
        except Exception as exc:
            logging.error("Failed to authenticate with GDRIVE_SERVICE_ACCOUNT_JSON: %s", exc)
            return None

    return None


def find_file_in_folder(
    token: str, folder_id: str, filename: str
) -> Optional[Dict[str, Any]]:
    """Find an existing non-trashed file by name inside a Google Drive folder."""
    escaped_name = filename.replace("'", "\\'")
    q = f"name = '{escaped_name}' and '{folder_id}' in parents and trashed = false"
    params = urllib.parse.urlencode(
        {
            "q": q,
            "fields": "files(id, name, size, modifiedTime)",
            "pageSize": 5,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
    )
    url = f"https://www.googleapis.com/drive/v3/files?{params}"
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}"}, method="GET"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        files = data.get("files") or []
        return files[0] if files else None


def download_file_from_drive(token: str, file_id: str, dest_path: Path) -> None:
    """Download a file's binary content from Google Drive to dest_path."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&supportsAllDrives=true"
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}"}, method="GET"
    )
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    with urllib.request.urlopen(req, timeout=120) as resp, open(tmp_path, "wb") as out:
        while True:
            chunk = resp.read(1024 * 256)
            if not chunk:
                break
            out.write(chunk)
    tmp_path.replace(dest_path)


def upload_or_update_file(
    token: str,
    folder_id: str,
    local_path: Path,
    mime_type: str = "application/octet-stream",
) -> str:
    """
    Update an existing file in-place (preserving its Google Drive file ID) or create
    a new file inside folder_id if it does not exist yet.
    """
    existing = find_file_in_folder(token, folder_id, local_path.name)
    file_bytes = local_path.read_bytes()

    if existing:
        file_id = existing["id"]
        url = (
            f"https://www.googleapis.com/upload/drive/v3/files/{file_id}"
            "?uploadType=media&supportsAllDrives=true"
        )
        req = urllib.request.Request(
            url,
            data=file_bytes,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": mime_type,
                "Content-Length": str(len(file_bytes)),
            },
            method="PATCH",
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            logging.info("Updated %s in Google Drive (ID: %s)", local_path.name, file_id)
            return result.get("id", file_id)
    else:
        boundary = "===Air4ThaiDriveBoundary7MA4YWxkTrZu0gW==="
        metadata = {"name": local_path.name, "parents": [folder_id]}
        meta_part = (
            f"--{boundary}\r\n"
            "Content-Type: application/json; charset=UTF-8\r\n\r\n"
            f"{json.dumps(metadata)}\r\n"
        ).encode("utf-8")
        media_header = (
            f"--{boundary}\r\n"
            f"Content-Type: {mime_type}\r\n\r\n"
        ).encode("utf-8")
        closing = f"\r\n--{boundary}--\r\n".encode("utf-8")
        body = meta_part + media_header + file_bytes + closing

        url = (
            "https://www.googleapis.com/upload/drive/v3/files"
            "?uploadType=multipart&supportsAllDrives=true"
        )
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": f"multipart/related; boundary={boundary}",
                "Content-Length": str(len(body)),
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            file_id = result.get("id", "")
            logging.info("Created %s in Google Drive (ID: %s)", local_path.name, file_id)
            return file_id


def sync_pull(token: str, folder_id: str, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Pull the latest air4thai.db from Google Drive before running a cloud collection."""
    remote_db = find_file_in_folder(token, folder_id, db_path.name)
    if not remote_db:
        logging.info("No remote %s found in Google Drive folder %s yet.", db_path.name, folder_id)
        return

    remote_size = int(remote_db.get("size") or 0)
    local_size = db_path.stat().st_size if db_path.exists() else 0
    if remote_size >= local_size and remote_size > 0:
        logging.info(
            "Downloading latest %s (%d bytes) from Google Drive...",
            db_path.name,
            remote_size,
        )
        download_file_from_drive(token, remote_db["id"], db_path)


def sync_push(
    token: str,
    folder_id: str,
    db_path: Path = DEFAULT_DB_PATH,
    csv_dir: Path = DEFAULT_CSV_DIR,
) -> List[str]:
    """Push updated air4thai.db and all monthly CSV files directly into Google Drive."""
    uploaded_ids: List[str] = []
    if db_path.exists():
        fid = upload_or_update_file(
            token, folder_id, db_path, mime_type="application/x-sqlite3"
        )
        uploaded_ids.append(fid)

    if csv_dir.exists():
        for csv_path in sorted(csv_dir.glob("air4thai_*.csv")):
            fid = upload_or_update_file(
                token, folder_id, csv_path, mime_type="text/csv"
            )
            uploaded_ids.append(fid)

    return uploaded_ids


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Sync Air4Thai SQLite database and monthly CSVs directly with Google Drive."
    )
    parser.add_argument(
        "--direction",
        choices=["pull", "push"],
        default="push",
        help="Whether to pull air4thai.db before collection or push updated DB + CSVs after collection.",
    )
    parser.add_argument(
        "--folder-id",
        default=os.environ.get("GDRIVE_FOLDER_ID", DEFAULT_DRIVE_FOLDER_ID),
        help=f"Target Google Drive folder ID (default: {DEFAULT_DRIVE_FOLDER_ID}).",
    )
    args = parser.parse_args()

    token = get_access_token()
    if not token:
        logging.warning(
            "Google Drive credentials not set in environment; skipping Drive %s step.",
            args.direction,
        )
        return

    if args.direction == "pull":
        sync_pull(token, args.folder_id)
    else:
        sync_push(token, args.folder_id)


if __name__ == "__main__":
    main()
