# Financial Data Utilities

A small collection of standalone Python utilities for financial data processing workflows.

These scripts cover practical data tasks such as consolidating Morningstar-style Excel exports, normalizing investment CSV files, computing trailing index returns from BigQuery data, and exporting BigQuery tables to local CSV files.

This repository is intentionally lightweight. The scripts are designed to be run directly from the command line rather than packaged as a framework.

## Scripts

| Script | Purpose |
|:-------|:--------|
| `combine_morningstar_exports.py` | Combines Morningstar-style `.xlsx` exports into grouped CSV files. |
| `normalize_investment_csv.py` | Normalizes investment/model-portfolio CSV exports. |
| `compute_index_returns.py` | Computes trailing index returns from BigQuery time-series data. |
| `export_bigquery_table_to_csv.py` | Exports a BigQuery table to a local CSV file via Google Cloud Storage. |

## Setup

Create and activate a virtual environment if desired:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash id="e20tvr"
pip install pandas openpyxl google-cloud-bigquery google-cloud-storage
```

Some environments may also require `db-dtypes` when using BigQuery DataFrame exports:

```bash id="znkxyt"
pip install db-dtypes
```

## Google Cloud authentication

The BigQuery-related scripts use Google Application Default Credentials.

Run:

```bash id="93wzhq"
gcloud auth application-default login
```

The authenticated account must have the necessary permissions for the relevant BigQuery datasets and, where applicable, Google Cloud Storage buckets.

Scripts that require Google Cloud access:

- `compute_index_returns.py`
- `export_bigquery_table_to_csv.py`

## Usage

### `combine_morningstar_exports.py`

Combines Morningstar-style Excel exports into consolidated CSV files.

The script scans an input directory for `.xlsx` files, groups them by filename pattern, reads each file using a configurable header row, filters valid data rows, and writes one CSV per output group.

```bash id="zatma3"
python scripts/combine_morningstar_exports.py \
  --input-dir ~/Downloads/morningstar_exports \
  --output-dir ./combined_exports
```

By default, the script uses these grouping rules:

| Input files | Output file |
|:------------|:------------|
| `MF*.xlsx` | `morningstar_mf.csv` |
| `ETF*.xlsx` | `morningstar_etf.csv` |
| `SMA*.xlsx` | `morningstar_sma.csv` |
| `Envestnet Strategies*.xlsx` | `morningstar_envestnet_plus.csv` |
| `AMPF Select Strategies*.xlsx` | `morningstar_envestnet_plus.csv` |
| `MF*.xlsx`, `ETF*.xlsx`, `SMA*.xlsx` | `morningstar_ms_extract.csv` |

Useful options:

```bash id="osi5lz"
python scripts/combine_morningstar_exports.py \
  --input-dir ~/Downloads/morningstar_exports \
  --output-dir ./combined_exports \
  --header-row 10 \
  --identifier-length 10 \
  --verbose
```

Notes:

- Header rows are 1-based.
- The default header row is `10`.
- A valid data row is identified by a first-column string of length `10`.
- If `--output-dir` is omitted, output files are written to the input directory.

### `normalize_investment_csv.py`

Normalizes investment/model-portfolio CSV exports.

The script performs targeted cleanup for selected text, percentage, and valuation fields.

```bash id="ch2ki1"
python scripts/normalize_investment_csv.py \
  --input raw_investments.csv \
  --output normalized_investments.csv
```

It currently supports:

- Smart title-casing for descriptive text columns
- Preservation of common financial acronyms such as `ETF`, `SMA`, `MSCI`, and `USD`
- Selected brand casing fixes such as `JPMorgan` and `iShares`
- Conversion of percentage-like values such as `12.34%` to decimal values such as `0.1234`
- Conversion of `Forward P/E` values such as `18.5x` to numeric values

By default, missing expected columns are skipped with warnings.

To fail when expected columns are missing:

```bash id="nnusy2"
python scripts/normalize_investment_csv.py \
  --input raw_investments.csv \
  --output normalized_investments.csv \
  --strict-columns
```

### `compute_index_returns.py`

Computes trailing returns from BigQuery time-series index data.

By default, this script does **not** write to BigQuery. It can write computed returns to a local CSV file, append them to a BigQuery table, or both.

Local CSV output:

```bash id="ib3gqk"
python scripts/compute_index_returns.py \
  --project my-gcp-project \
  --dataset market_data \
  --source-table index_values \
  --mapping-file id_to_name.csv \
  --output index_returns.csv
```

Write computed rows to BigQuery:

```bash id="jb7u9p"
python scripts/compute_index_returns.py \
  --project my-gcp-project \
  --dataset market_data \
  --source-table index_values \
  --destination-table index_returns \
  --mapping-file id_to_name.csv \
  --as-of-date 2025-03-31 \
  --write-to-bigquery
```

Expected source table schema:

| Column | Type |
|:-------|:-----|
| `id` | `STRING` |
| `time` | `DATETIME` or `TIMESTAMP` |
| `value` | `FLOAT` |

Expected mapping CSV schema:

| Column | Description |
|:-------|:------------|
| `id` | Index identifier matching the source table |
| `name` | Display name for the index |

Computed columns include:

- `return_7day`
- `return_30day`
- `return_3mo`
- `return_ytd`
- `return_1yr`
- `return_3yr_ann`
- `return_5yr_ann`
- `return_itd`

Notes:

- `--destination-table` is required only when `--write-to-bigquery` is used.
- `--as-of-date` should be supplied in `YYYY-MM-DD` format.
- If `--as-of-date` is omitted, the script defaults to yesterday.
- Return windows use fixed-day approximations, such as `90` days for 3 months and `365` days for 1 year.

### `export_bigquery_table_to_csv.py`

Exports a BigQuery table to a local CSV file via Google Cloud Storage.

BigQuery exports table data to Cloud Storage first. This script performs that export, downloads the resulting CSV locally, and can optionally remove the intermediate GCS object afterward.

```bash id="52nejv"
python scripts/export_bigquery_table_to_csv.py \
  --project my-gcp-project \
  --dataset analytics \
  --table daily_metrics \
  --bucket my-export-bucket \
  --gcs-object exports/daily_metrics.csv \
  --output daily_metrics.csv \
  --delete-gcs-object
```

Useful options:

```bash id="3ljdc2"
python scripts/export_bigquery_table_to_csv.py \
  --project my-gcp-project \
  --dataset analytics \
  --table daily_metrics \
  --bucket my-export-bucket \
  --gcs-object exports/daily_metrics.csv \
  --output daily_metrics.csv \
  --location US \
  --verbose
```

Notes:

- The default BigQuery job location is `US`.
- Use `--location` if your dataset is in another location.
- Use `--delete-gcs-object` to remove the intermediate GCS object after download.
- Without `--delete-gcs-object`, the exported object remains in the specified bucket.

## Data assumptions

These scripts are intentionally practical and somewhat opinionated. They are not universal data-cleaning tools.

### Morningstar Excel exports

`combine_morningstar_exports.py` assumes:

- Source files are `.xlsx` files.
- Files follow recognizable Morningstar-style naming patterns.
- The header row is consistent across files, defaulting to row `10`.
- Valid data rows can be identified by a first-column identifier of length `10`.

### Investment CSV normalization

`normalize_investment_csv.py` assumes:

- The source file is a CSV.
- Certain known column names may be present.
- Missing expected columns can be skipped unless `--strict-columns` is used.
- Percentage-like fields should be converted into decimal numeric values.

### Index return calculation

`compute_index_returns.py` assumes:

- Source index values live in BigQuery.
- Each source row has an `id`, `time`, and `value`.
- A separate mapping CSV provides `id` to `name` mappings.
- The `as_of_date` should correspond to available observations in the source table.
- Missing historical values produce null return values.

## Operational notes

Review command-line arguments before running scripts against cloud resources.

`compute_index_returns.py` only writes to BigQuery when `--write-to-bigquery` is explicitly provided.

`export_bigquery_table_to_csv.py` may create intermediate objects in Google Cloud Storage. Use `--delete-gcs-object` if those objects should be removed after download.

Google Cloud usage may incur costs depending on query size, table size, storage usage, and export volume.

## Repository structure

```text id="p2grxx"
financial-data-utils/
  README.md
  scripts/
    combine_morningstar_exports.py
    compute_index_returns.py
    export_bigquery_table_to_csv.py
    normalize_investment_csv.py
```

## Status

This repository contains small, standalone utilities extracted from real financial data workflows. The scripts favor clarity and direct command-line usage over abstraction.