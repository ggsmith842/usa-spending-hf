import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from tqdm import tqdm


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" 
BASE_URL = "https://api.usaspending.gov/"


LOG_DIR = PROJECT_ROOT / "logs"
REQUEST_LOG = LOG_DIR / "bulk_download_requests.jsonl"


def _ensure_log_dir_exists() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()[:12]

def _record_bulk_request(
    payload: dict[str, Any],
    response_data: dict[str, Any],
    request_id: str
) -> None:
    _ensure_log_dir_exists()

    entry: dict[str, Any] = {
        "timestamp": datetime.now().isoformat() + "Z",
        "request_id": request_id,
        "request_name": "api/v2/bulk_download/awards/",
        "filters": payload.get("filters"),
        "date_range": payload.get("filters", {}).get("date_range"),
        "columns_requested": len(payload.get("columns", [])),
        "response_status_url": response_data.get("status_url"),
        "response_status": response_data.get("status"),
        "payload": payload,
    }

    with REQUEST_LOG.open("a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(entry, ensure_ascii=False))
        log_file.write("\n")


def request_bulk_download(session: requests.Session, payload: dict[str, Any]) -> tuple[str, str]:
    url = BASE_URL + "api/v2/bulk_download/awards/"
    headers = {"Content-Type": "application/json", "Vary": "Accept"}
    resp = session.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    status_url = data.get("status_url")
    request_id = _hash_payload(payload)
    _record_bulk_request(payload, data, request_id)

    if not status_url:
        raise ValueError(f"Missing status_url in response: {data}")
    print(f"Bulk download request submitted: status code - {resp.status_code}")
    return status_url, request_id


def poll_job(session:requests.Session, status_url: str, wait_time: int=30, timeout:int=3600):
    
    start = time.time()
    print(f"Polling bulk download request:\n- {status_url}\n-polling interval {wait_time} seconds")

    while True:
        if time.time() - start > timeout:
            raise TimeoutError(f"Polling timed out - manually monitor using the status url: {status_url}")
        

        try:
            resp = session.get(status_url, timeout=(10, 60))
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status")
            file_url = data.get("file_url")
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"{time.time()}: transient poll error: {e}; retrying in {wait_time}s...")
            time.sleep(wait_time)
            continue
        

        print(f"{datetime.now().isoformat()}: job status - {status}")
        if status == "finished" and file_url:
            print("Job finished - file ready for download")
            return file_url
        if status == "failed":
            raise RuntimeError("Bulk download job failed")

        time.sleep(wait_time)


def download_bulk_file(session: requests.Session, file_url: str):
    resp = session.get(file_url, stream=True)
    resp.raise_for_status()
    total_size = int(resp.headers.get("content-length", 0))
    DATA_DIR = PROJECT_ROOT / "data" / "raw"
    filename = file_url.split("/")[-1]
    output_path =  DATA_DIR / filename

    chunk_size = 8192

    with open(output_path, "wb") as f, tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=filename
    ) as pbar:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))
    print(f"Bulk download written to {output_path}")
    return output_path

# ref: https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/search/spending_by_award.md
def serch_spending_by_award(payload: str):

    with requests.session() as session:
        try:
            session.post(BASE_URL+"api/v2/search/spending_by_award/", json=payload)
        except Exception as e:
            raise e
    
    return


def test_logger(payload: dict[str, Any], request_id="1234") -> None:
    data={"status_url": "test", "status": "finished"}
    _record_bulk_request(payload, data, request_id)
    return None