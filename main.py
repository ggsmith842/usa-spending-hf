import requests
from pipeline.ingest.usaspend import (
    request_bulk_download,
    poll_job,
    download_bulk_file
    )

from pipeline.transforms.parse import stream_parquet

def pipeline():

    print("Starting pipeline")
    payload = {
        "filters": {
            "date_range": {"start_date": "2023-10-01", "end_date": "2024-09-30"},
            "prime_award_types": ["A", "B", "C", "D"],
        },
        "columns": [],
        "file_format": "csv",
    }
    with requests.Session() as session:

        

        status_url = request_bulk_download(session, payload)
        file_url = poll_job(session, status_url)
        

        # stream zip file directly to hf
        # stream_to_hf_bucket(session, file_url)

        # convert to parquet locally then upload to hf buckets
        zip_path = download_bulk_file(session, file_url)
        stream_parquet(zip_path)

if __name__ == "__main__":
    pipeline()
