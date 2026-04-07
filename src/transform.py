from tempfile import TemporaryDirectory
from pathlib import Path
import zipfile

import polars as pl
import pyarrow.parquet as pq

from huggingface_hub import download_bucket_files, sync_bucket


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" 

def convert_to_parquet(
        bucket_id: str,
        remote_zip_path: str
    ):
    with TemporaryDirectory(dir=DATA_DIR) as tmp:
        tmp = Path(tmp)
        zip_path = tmp / "data.zip"
        extract_dir = tmp / "extract"
        out_dir = tmp / "parquet"
        out_prefix = remote_zip_path[4:23]
        extract_dir.mkdir()
        out_dir.mkdir()

        # download zips from huggingface
        download_bucket_files(
            bucket_id=bucket_id,
            files=[(remote_zip_path, str(zip_path))]
        )

        # process each csv found in the zip file to one parquet file
        with zipfile.ZipFile(zip_path) as zf:
            csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_files:
                raise ValueError("No CSV files found in zip")
            
            for csv_name in csv_files:
                extracted_csv = extract_dir / Path(csv_name).name
                with zf.open(csv_name) as src, open(extracted_csv, "wb") as dst:
                    dst.write(src.read())

                parquet_path = out_dir / f"{out_prefix}_{Path(csv_name).stem}.parquet"
                writer = None

                for df in pl.scan_csv(
                    str(extracted_csv),
                    infer_schema_length=10_000,
                ).collect_batches():
                    table = df.to_arrow()

                    if writer is None:
                        writer = pq.ParquetWriter(
                            parquet_path,
                            table.schema,
                            compression="zstd"
                        )
                    
                    writer.write_table(table)

                if writer is not None:
                    writer.close()

            sync_bucket(
                str(out_dir),
                f"hf://buckets/{bucket_id}/parquet/"
            )