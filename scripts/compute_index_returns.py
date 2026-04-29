#!/usr/bin/env python3
"""
Compute trailing returns from time-series index values.

This script reads index/value time-series data from BigQuery, computes trailing
returns for each index, and either writes the result to a local CSV file or loads
it into a destination BigQuery table.

Expected source table schema:

    id      STRING
    time    DATETIME or TIMESTAMP
    value   FLOAT

Expected mapping CSV schema:

    id      STRING
    name    STRING

By default, this script does not write to BigQuery. Use --write-to-bigquery to
load computed rows into the destination table.

Example:
    python scripts/compute_index_returns.py \\
        --project my-gcp-project \\
        --dataset market_data \\
        --source-table index_values \\
        --mapping-file id_to_name.csv \\
        --output index_returns.csv

Example with BigQuery load:
    python scripts/compute_index_returns.py \\
        --project my-gcp-project \\
        --dataset market_data \\
        --source-table index_values \\
        --destination-table index_returns \\
        --mapping-file id_to_name.csv \\
        --as-of-date 2025-03-31 \\
        --write-to-bigquery
"""

from __future__ import annotations

import argparse
import logging
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError


LOGGER = logging.getLogger(__name__)


RETURN_WINDOWS: tuple[tuple[str, timedelta, bool], ...] = (
    ("return_7day", timedelta(days=7), False),
    ("return_30day", timedelta(days=30), False),
    ("return_3mo", timedelta(days=90), False),
    ("return_ytd", timedelta(days=0), False),  # handled separately
    ("return_1yr", timedelta(days=365), False),
    ("return_3yr_ann", timedelta(days=3 * 365), True),
    ("return_5yr_ann", timedelta(days=5 * 365), True),
)


def compute_return(current_value: float | None, past_value: float | None) -> float | None:
    """Compute a simple return."""
    if not is_valid_numeric_value(current_value) or not is_valid_numeric_value(past_value):
        return None

    if past_value == 0:
        return None

    return current_value / past_value - 1


def compute_annualized_return(
    current_value: float | None,
    past_value: float | None,
    years: int,
) -> float | None:
    """Compute an annualized return over a multi-year period."""
    if not is_valid_numeric_value(current_value) or not is_valid_numeric_value(past_value):
        return None

    if past_value == 0:
        return None

    return math.pow(current_value / past_value, 1 / years) - 1


def is_valid_numeric_value(value: Any) -> bool:
    """Return whether a value can be used in return calculations."""
    if value is None:
        return False

    if isinstance(value, float) and math.isnan(value):
        return False

    return True


def get_latest_value_on_or_before(
    index_df: pd.DataFrame,
    target_date: date,
) -> float | None:
    """Return the latest available value on or before the target date."""
    available = index_df[index_df["date"] <= target_date]

    if available.empty:
        return None

    return available.sort_values("date", ascending=False).iloc[0]["value"]


def get_first_available_value(index_df: pd.DataFrame) -> float | None:
    """Return the first non-null value available for an index."""
    valid = index_df[index_df["value"].notnull()].sort_values("date", ascending=True)

    if valid.empty:
        return None

    return valid.iloc[0]["value"]


def load_id_name_mapping(mapping_file: Path) -> dict[str, str]:
    """Load an ID-to-name mapping CSV."""
    mapping_file = mapping_file.expanduser().resolve()

    if not mapping_file.is_file():
        raise FileNotFoundError(f"Mapping file does not exist: {mapping_file}")

    mapping_df = pd.read_csv(mapping_file, dtype={"id": str, "name": str})

    required_columns = {"id", "name"}
    missing_columns = required_columns - set(mapping_df.columns)

    if missing_columns:
        raise ValueError(
            f"Mapping file is missing required column(s): {sorted(missing_columns)}"
        )

    return mapping_df.set_index("id")["name"].to_dict()


def read_index_values_from_bigquery(
    *,
    project_id: str,
    dataset_id: str,
    source_table: str,
) -> pd.DataFrame:
    """Read source index values from BigQuery."""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{source_table}"

    query = f"""
        SELECT
            CAST(id AS STRING) AS id,
            time,
            value
        FROM `{table_ref}`
    """

    LOGGER.info("Reading source data from %s", table_ref)

    df = client.query(query).to_dataframe()

    required_columns = {"id", "time", "value"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Source table is missing required column(s): {sorted(missing_columns)}"
        )

    df["date"] = pd.to_datetime(df["time"]).dt.date

    return df


def compute_index_returns(
    *,
    values_df: pd.DataFrame,
    id_to_name: dict[str, str],
    as_of_date: date,
) -> pd.DataFrame:
    """
    Compute trailing returns for each index ID.

    The as-of date is expected to be the target observation date. If an index has
    no value exactly on that date, it is skipped.
    """
    results: list[dict[str, Any]] = []

    for index_id in sorted(values_df["id"].dropna().unique()):
        index_df = values_df[values_df["id"] == index_id].copy()

        current_rows = index_df[index_df["date"] == as_of_date]

        if current_rows.empty:
            LOGGER.warning("No data available for id %s on %s", index_id, as_of_date)
            continue

        current_value = current_rows.sort_values("time", ascending=False).iloc[0]["value"]

        value_7day = get_latest_value_on_or_before(
            index_df,
            as_of_date - timedelta(days=7),
        )
        value_30day = get_latest_value_on_or_before(
            index_df,
            as_of_date - timedelta(days=30),
        )
        value_3mo = get_latest_value_on_or_before(
            index_df,
            as_of_date - timedelta(days=90),
        )
        value_ytd = get_latest_value_on_or_before(
            index_df,
            date(as_of_date.year, 1, 1),
        )
        value_1yr = get_latest_value_on_or_before(
            index_df,
            as_of_date - timedelta(days=365),
        )
        value_3yr = get_latest_value_on_or_before(
            index_df,
            as_of_date - timedelta(days=3 * 365),
        )
        value_5yr = get_latest_value_on_or_before(
            index_df,
            as_of_date - timedelta(days=5 * 365),
        )
        value_itd = get_first_available_value(index_df)

        results.append(
            {
                "id": index_id,
                "name": id_to_name.get(index_id, index_id),
                "as_of_date": as_of_date.isoformat(),
                "return_7day": compute_return(current_value, value_7day),
                "return_30day": compute_return(current_value, value_30day),
                "return_3mo": compute_return(current_value, value_3mo),
                "return_ytd": compute_return(current_value, value_ytd),
                "return_1yr": compute_return(current_value, value_1yr),
                "return_3yr_ann": compute_annualized_return(
                    current_value,
                    value_3yr,
                    years=3,
                ),
                "return_5yr_ann": compute_annualized_return(
                    current_value,
                    value_5yr,
                    years=5,
                ),
                "return_itd": compute_return(current_value, value_itd),
            }
        )

    return pd.DataFrame(results)


def write_returns_to_bigquery(
    *,
    project_id: str,
    dataset_id: str,
    destination_table: str,
    returns_df: pd.DataFrame,
) -> None:
    """Append computed returns to a BigQuery table."""
    if returns_df.empty:
        LOGGER.warning("No return rows to write to BigQuery")
        return

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{destination_table}"

    rows = returns_df.where(pd.notnull(returns_df), None).to_dict(orient="records")

    LOGGER.info("Writing %d row(s) to %s", len(rows), table_ref)

    errors = client.insert_rows_json(table_ref, rows)

    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")

    LOGGER.info("Successfully wrote %d row(s) to BigQuery", len(rows))


def parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD date string."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}. Expected YYYY-MM-DD."
        ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute trailing returns from BigQuery time-series index data."
    )

    parser.add_argument(
        "--project",
        required=True,
        help="Google Cloud project ID.",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="BigQuery dataset ID.",
    )
    parser.add_argument(
        "--source-table",
        required=True,
        help="Source BigQuery table containing id, time, and value columns.",
    )
    parser.add_argument(
        "--destination-table",
        help=(
            "Destination BigQuery table for computed returns. "
            "Required when --write-to-bigquery is used."
        ),
    )
    parser.add_argument(
        "--mapping-file",
        required=True,
        type=Path,
        help="CSV file containing id and name columns.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional local CSV output path for computed returns.",
    )
    parser.add_argument(
        "--as-of-date",
        type=parse_date,
        default=date.today() - timedelta(days=1),
        help=(
            "Date to compute returns as of, in YYYY-MM-DD format. "
            "Defaults to yesterday."
        ),
    )
    parser.add_argument(
        "--write-to-bigquery",
        action="store_true",
        help="Append computed return rows to the destination BigQuery table.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )

    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)

    if args.write_to_bigquery and not args.destination_table:
        LOGGER.error("--destination-table is required when --write-to-bigquery is used")
        return 1

    if not args.output and not args.write_to_bigquery:
        LOGGER.error("Specify --output, --write-to-bigquery, or both")
        return 1

    try:
        id_to_name = load_id_name_mapping(args.mapping_file)

        values_df = read_index_values_from_bigquery(
            project_id=args.project,
            dataset_id=args.dataset,
            source_table=args.source_table,
        )

        returns_df = compute_index_returns(
            values_df=values_df,
            id_to_name=id_to_name,
            as_of_date=args.as_of_date,
        )

        if returns_df.empty:
            LOGGER.warning("No returns were computed")
            return 0

        if args.output:
            output_path = args.output.expanduser().resolve()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            returns_df.to_csv(output_path, index=False)
            LOGGER.info("Wrote computed returns to %s", output_path)

        if args.write_to_bigquery:
            write_returns_to_bigquery(
                project_id=args.project,
                dataset_id=args.dataset,
                destination_table=args.destination_table,
                returns_df=returns_df,
            )

    except (GoogleCloudError, OSError, ValueError, RuntimeError) as exc:
        LOGGER.error("Failed to compute index returns: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())