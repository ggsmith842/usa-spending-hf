# USA Spending -> Hugging Face Bucket Pipeline

Python pipeline for requesting USAspending bulk award exports, uploading raw ZIP files to a Hugging Face bucket, and converting bucketed ZIPs into parquet files.

## What this project does

- Submits USAspending bulk download requests for selected fiscal date ranges.
- Polls job status until the export file is ready.
- Streams the ZIP directly to a Hugging Face bucket under `raw/`.
- Logs request metadata and payloads to `logs/bulk_download_requests.jsonl`.
- Converts a raw ZIP in the bucket into parquet files and syncs them back to the bucket.

## Project layout

- `main.py`: end-to-end raw ingestion pipeline (request -> poll -> upload ZIP).
- `converter_pipeline.py`: conversion entry script (bucket ZIP -> parquet sync).
- `src/extract.py`: USAspending request + polling + request logging.
- `src/upload.py`: raw ZIP upload to Hugging Face bucket.
- `src/transform.py`: ZIP CSV -> parquet conversion + bucket sync.
- `logs/bulk_download_requests.jsonl`: append-only request log.

## Prerequisites

- Python `>=3.14`
- [`uv`](https://docs.astral.sh/uv/)
- Hugging Face account + token with bucket write access

## Setup

1. Install dependencies:

```bash
uv sync
```

2. Create `.env` in project root:

```env
HF_TOKEN=hf_xxx
HF_NAMESPACE=your-hf-username-or-org
HF_BUCKET_NAME=usa-spending
```

3. Confirm environment:

```bash
uv run python -c "import requests, polars, pyarrow, huggingface_hub; print('ok')"
```

## Usage

### 1) Run ingestion pipeline (USAspending -> bucket/raw)

```bash
uv run main.py
```

What it does:
- Uses fiscal year date ranges in `main.py`
- Calls `request_bulk_download(...)`
- Polls with `poll_job(...)`
- Uploads ready ZIP to `hf://buckets/<namespace>/<bucket>/raw/`

### 2) Run converter pipeline (bucket/raw ZIP -> bucket/parquet)

```bash
uv run converter_pipeline.py
```

What it does:
- Downloads a ZIP from a bucket path like `raw/<request_id>_fyYYYY-MM-DD_<zip_name>.zip`
- Converts all CSV files in that ZIP to parquet files
- Syncs outputs to `hf://buckets/<namespace>/<bucket>/parquet/`

## Request logging and traceability

Each bulk download request to USAspending is logged in:

- `logs/bulk_download_requests.jsonl`

Fields include:
- `timestamp`
- `request_id` (hash of payload)
- `date_range`
- `columns_requested`
- `response_status_url`
- full `payload`

This gives you a deterministic link between:
- the request payload/query
- the uploaded raw ZIP file name (which includes request hash + FY)
- downstream parquet artifacts

## Common edits you will make

- Change fiscal years in `main.py`:
  - `fiscal_years = [("2021-10-01","2022-09-30"), ...]`
- Change selected USAspending columns in `main.py` payload.
- Change converter input ZIP in `converter_pipeline.py`:
  - `remote_zip_path="raw/<your-file>.zip"`

## Troubleshooting

### `AttributeError: module 'pyarrow' has no attribute 'PyExtensionType'`

Cause: very old `datasets` with newer `pyarrow`.

Fix:

```bash
uv sync
```

This project pins `datasets` and `pyarrow` to compatible versions in `pyproject.toml`/`uv.lock`.

### `TqdmWarning: IProgress not found...`

If running notebooks, install widget support in the same environment:

```bash
uv pip install jupyter ipywidgets
```

Then restart your VS Code kernel.

### `No CSV files found in zip`

The file at `remote_zip_path` is not the expected USAspending ZIP, or the path is wrong. Verify the object exists under `raw/` in your bucket.

## Notes

- `converter_pipeline.py` currently contains a hardcoded `remote_zip_path`; update it before each run or refactor to accept CLI args.
- `src/transform.py` emits concise `[transform]` progress logs for operator visibility.
