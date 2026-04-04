import requests
import time

from pathlib import Path
from typing import Any
from tqdm import tqdm


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" 
BASE_URL = "https://api.usaspending.gov/"


def request_bulk_download(session: requests.Session, payload: dict[str, Any]) -> str:
    url = BASE_URL + "api/v2/bulk_download/awards/"
    headers = {"Content-Type": "application/json", "Vary": "Accept"}
    resp = session.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    
    status_url = data.get("status_url")
    if not status_url:
        raise ValueError(f"Missing status_url in response: {data}")

    return status_url


def poll_job(session:requests.Session, status_url: str, wait_time: int=10, timeout:int=1800):
    
    start = time.time()

    while True:
        resp = session.get(status_url)
        resp.raise_for_status()
        data = resp.json()

        status = data.get("status")
        file_url = data.get("file_url")

        if status == "finished" and file_url:
            print("Job finished - file ready for download")
            return file_url

        if status == "failed":
            raise RuntimeError("Bulk download job failed")

        if time.time() - start > timeout:
            raise TimeoutError("Polling timed out")

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

