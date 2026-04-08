from src.transform import convert_to_parquet
from dotenv import load_dotenv

import os


load_dotenv()
# initialize HF parameters
HF_TOKEN = os.getenv("HF_TOKEN")
HF_NAMESPACE = os.getenv("HF_NAMESPACE")
HF_BUCKET_NAME = os.getenv("HF_BUCKET_NAME")


def main():
    convert_to_parquet(
        bucket_id=f"{HF_NAMESPACE}/{HF_BUCKET_NAME}",
        remote_zip_path="raw/494414914c50_fy2023-10-01_All_PrimeTransactions_2026-04-05_H19M19S57061533.zip",
    )


if __name__ == "__main__":
    main()
