import requests
import time
import os 

from pathlib import Path
from typing import Any
from dotenv import load_dotenv
from tqdm import tqdm
from huggingface_hub import create_bucket, HfFileSystem

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" 
BASE_URL = "https://api.usaspending.gov/"

load_dotenv()

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
    
    return output_path

def stream_to_hf_bucket(session: requests.Session, file_url:str):
    #initialize HF parameters
    HF_TOKEN=os.getenv("HF_TOKEN")
    HF_NAMESPACE=os.getenv("HF_NAMESPACE")
    HF_BUCKET_NAME=os.getenv("HF_BUCKET_NAME")
    file_name = file_url.split("/")[-1]

    fs = HfFileSystem(token=HF_TOKEN)
    bucket_path = f"hf://buckets/{HF_NAMESPACE}/{HF_BUCKET_NAME}/{file_name}"
    create_bucket(f"{HF_NAMESPACE}/{HF_BUCKET_NAME}", token=HF_TOKEN, exist_ok=True)

    print(f"Streaming data directly from USAspending to {bucket_path}...")

    zip_stream = session.get(file_url, stream=True)
    zip_stream.raise_for_status()

    total_size = int(zip_stream.headers.get('content-length', 0))
    progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True, desc="Uploading")

    with fs.open(bucket_path, "wb") as hf_file:
        # stream in 5mb chunks 
        for chunk in zip_stream.iter_content(chunk_size=5*1024*1024):
            if chunk:
                hf_file.write(chunk)
                progress_bar.update(len(chunk))
    
    progress_bar.close()