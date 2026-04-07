import requests
from src.extract import (
    request_bulk_download,
    poll_job
)

from src.upload import stream_to_hf_bucket


def pipeline():
    print("Starting pipeline")
    fiscal_years = [
        ("2021-10-01", "2022-09-30"),
        # ("2022-10-01", "2023-09-30"),
        # ("2023-10-01", "2024-09-30"),
        ("2024-10-01", "2025-09-30"),
    ]
    for date_range in fiscal_years:
        start_date, end_date = date_range
        print(f"Requesting data for FY{start_date}...")
        payload = {
            "filters": {
                "date_range": {"start_date": start_date, "end_date": end_date},
                "prime_award_types": ["A", "B", "C", "D"],
            },
            "columns": [
                "award_id_piid",
                "recipient_name",
                "recipient_duns",
                "period_of_performance_start_date",
                "period_of_performance_current_end_date",
                "transaction_description",
                "current_total_value_of_award",
                "awarding_agency_name",
                "awarding_sub_agency_name",
                "award_type",
                "award_or_idv_flag",
                "funding_agency_name",
                "funding_sub_agency_name",
                "naics_code",
                "product_or_service_code",
            ],
            "file_format": "csv",
        }

        with requests.Session() as session:

            status_url, request_id = request_bulk_download(session, payload)
            file_url = poll_job(session, status_url)

            # stream zip file directly to hf
            stream_to_hf_bucket(session, file_url, start_date, request_id)
            # convert to parquet locally then upload to hf buckets
            # zip_path = download_bulk_file(session, file_url)
            # stream_parquet(zip_path)


if __name__ == "__main__":
    pipeline()
