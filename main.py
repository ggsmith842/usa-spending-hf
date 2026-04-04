import requests
from transforms.usaspend import (
    request_bulk_download,
    poll_job,
    stream_to_hf_bucket
    )

def main():

    print("Hello from hf-usa-spending!")

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
        stream_to_hf_bucket(session, file_url)

if __name__ == "__main__":
    main()
