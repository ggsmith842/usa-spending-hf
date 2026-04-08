import os
import zipfile
import requests
import polars as pl
import shutil
import tempfile
from dotenv import load_dotenv
from tqdm import tqdm
from pathlib import Path
from huggingface_hub import HfFileSystem, create_bucket, batch_bucket_files

# project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
TEMP_DIR = PROJECT_ROOT / "data" / "temp"


load_dotenv()
# initialize HF parameters
HF_TOKEN = os.getenv("HF_TOKEN")
HF_NAMESPACE = os.getenv("HF_NAMESPACE")
HF_BUCKET_NAME = os.getenv("HF_BUCKET_NAME")


SCHEMA = {
    "award_id_piid": pl.Utf8,
    "recipient_name": pl.Utf8,
    "recipient_duns": pl.Utf8,
    "period_of_performance_start_date": pl.Date,
    "period_of_performance_current_end_date": pl.Date,
    "transaction_description": pl.Utf8,
    "current_total_value_of_award": pl.Float64,
    "awarding_agency_name": pl.Utf8,
    "awarding_sub_agency_name": pl.Utf8,
    "award_type": pl.Utf8,
    "award_or_idv_flag": pl.Utf8,
    "funding_agency_name": pl.Utf8,
    "funding_sub_agency_name": pl.Utf8,
    "naics_code": pl.Utf8,
    "product_or_service_code": pl.Utf8,
}


def stream_parquet(zip_path):
    uploads = []
    bucket_id = f"{HF_NAMESPACE}/{HF_BUCKET_NAME}"
    create_bucket(bucket_id=bucket_id, token=HF_TOKEN, exist_ok=True)

    with tempfile.TemporaryDirectory(dir=TEMP_DIR) as tmp:
        tmp_dir = Path(tmp)

        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_file = [f for f in zf.infolist() if f.filename.lower().endswith(".csv")]

            for file in tqdm(csv_file, desc="CSV -> Parquet", unit="file"):
                csv_name = Path(file.filename).name
                csv_tmp = tmp_dir / csv_name
                parquet_tmp = tmp_dir / f"{Path(csv_name).stem}.parquet"

                with zf.open(file, "r") as src, open(csv_tmp, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)

                pl.scan_csv(
                    csv_tmp, infer_schema_length=10000, schema_overrides=SCHEMA
                ).sink_parquet(
                    parquet_tmp,
                    compression="zstd",
                    maintain_order=False,
                )
                uploads.append((str(parquet_tmp), f"parquet/{parquet_tmp.name}"))
                csv_tmp.unlink(missing_ok=True)

        batch_bucket_files(bucket_id, add=uploads, token=HF_TOKEN)


def stream_to_hf_bucket(
    session: requests.Session,
    file_url: str,
    start_dt: str,
    request_id: str,
    hf_bucket: str = f"{HF_NAMESPACE}/{HF_BUCKET_NAME}",
):
    """
    Stream a file from a remote URL directly to a Hugging Face Hub bucket.
    This function downloads a file from a given URL in chunks and streams it
    to a specified Hugging Face Hub bucket without loading the entire file
    into memory.
    Args:
        session (requests.Session): An active requests session object used to
            make HTTP requests.
        file_url (str): The complete URL of the file to be downloaded and streamed.
        hf_bucket (str): Target Bucket in format "namespace/bucketname"

    Note:
        - Files are downloaded in 5MB chunks to optimize memory usage.
        - The bucket is automatically created if it does not already exist.
    """

    file_name = f"{request_id}_fy{start_dt}_{file_url.split('/')[-1]}"

    fs = HfFileSystem(token=HF_TOKEN)
    bucket_path = f"hf://buckets/{hf_bucket}/raw/{file_name}"
    create_bucket(f"{HF_NAMESPACE}/{HF_BUCKET_NAME}", token=HF_TOKEN, exist_ok=True)

    print(f"Streaming data directly from USAspending to {bucket_path}...")

    zip_stream = session.get(file_url, stream=True)
    zip_stream.raise_for_status()

    total_size = int(zip_stream.headers.get("content-length", 0))
    progress_bar = tqdm(total=total_size, unit="iB", unit_scale=True, desc="Uploading")

    with fs.open(bucket_path, "wb") as hf_file:
        # stream in 5mb chunks
        for chunk in zip_stream.iter_content(chunk_size=5 * 1024 * 1024):
            if chunk:
                hf_file.write(chunk)
                progress_bar.update(len(chunk))

    progress_bar.close()
