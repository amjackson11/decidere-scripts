#!/usr/bin/env python3
"""
Normalize investment CSV exports.

This script cleans selected text, percentage, and numeric valuation columns in
an investment/model-portfolio CSV file.

It currently supports three common cleanup operations:

    - Smart title-casing for descriptive text columns
    - Preservation of common financial acronyms such as ETF, SMA, MSCI, USD
    - Conversion of percentage strings such as "12.34%" to decimals such as 0.1234
    - Conversion of Forward P/E values such as "18.5x" to numeric values

Example:
    python scripts/normalize_investment_csv.py \\
        --input raw_investments.csv \\
        --output normalized_investments.csv

By default, missing expected columns are skipped with warnings. Use
--strict-columns to fail when expected columns are missing.
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Iterable

import pandas as pd


LOGGER = logging.getLogger(__name__)


TITLECASE_COLUMNS: tuple[str, ...] = (
    "Name",
    "Model provider",
    "Category",
    "Model type",
    "Status",
    "Primary portfolio objective",
    "Strategic/Tactical",
    "Tax sensitivity",
)


TITLECASE_ACRONYMS: frozenset[str] = frozenset(
    {
        "AAA",
        "ADR",
        "AGG",
        "ACWI",
        "BNY",
        "CDA",
        "CL",
        "CLO",
        "CON",
        "EAFE",
        "ESG",
        "ETF",
        "ETFS",
        "ETN",
        "FTSE",
        "GBL",
        "GNMA",
        "LLC",
        "LTD",
        "MDL",
        "MD",
        "MDT",
        "MF",
        "MOD",
        "MFS",
        "MSCI",
        "NR",
        "NYLI",
        "PGIM",
        "QQQ",
        "SMA",
        "SPDR",
        "USA",
        "US",
        "USD",
    }
)


TITLECASE_SPECIAL_REPLACEMENTS: dict[str, str] = {
    "JPMORGAN": "JPMorgan",
    "ISHARES": "iShares",
}


PERCENT_COLUMNS: tuple[str, ...] = (
    "Manager fee",
    "Estimated net expenses",
    "Yield",
    "3-5 year earnings growth rate",
    "12 month turnover ratio",
    "3 year average annual turnover",
    "Standard deviation (3 year)",
    "Alpha (3 year)",
    "R-squared (3 year)",
    "R-squared (5 year)",
    "Up market capture ratio (3 year)",
    "Up market capture ratio (5 year)",
    "Down market capture ratio (3 year)",
    "Down market capture ratio (5 year)",
    "Performance (most recent quarter)",
    "Performance (1 year)",
    "Performance (3 year)",
    "Performance (5 year)",
    "Performance (10 year)",
    "Performance (since inception)",
)


FORWARD_PE_COLUMN = "Forward P/E"


def smart_titlecase(value: object) -> object:
    """
    Title-case text while preserving common financial acronyms and brand casing.

    Args:
        value:
            Input cell value.

    Returns:
        The transformed value, or the original value if it is null.
    """
    if pd.isna(value):
        return value

    text = str(value).title()

    for original, replacement in TITLECASE_SPECIAL_REPLACEMENTS.items():
        text = re.sub(
            rf"\b{re.escape(original.title())}\b",
            replacement,
            text,
        )

    for acronym in TITLECASE_ACRONYMS:
        text = re.sub(
            rf"\b{re.escape(acronym.title())}\b",
            acronym,
            text,
        )

    return text


def normalize_percent_series(series: pd.Series) -> pd.Series:
    """
    Convert percentage-like values to decimal numeric values.

    Examples:
        "12.5%" -> 0.125
        "12.5"  -> 0.125
        ""      -> NaN
    """
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace(",", "", regex=False)
        .str.rstrip("%")
    )

    return (pd.to_numeric(cleaned, errors="coerce") / 100).round(4)


def normalize_forward_pe_series(series: pd.Series) -> pd.Series:
    """
    Convert Forward P/E values to numeric values.

    Examples:
        "18.5x" -> 18.5
        "18.5"  -> 18.5
        ""      -> NaN
    """
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace(",", "", regex=False)
        .str.rstrip("x")
        .str.rstrip("X")
    )

    return pd.to_numeric(cleaned, errors="coerce")


def existing_columns(
    df: pd.DataFrame,
    expected_columns: Iterable[str],
    *,
    strict: bool,
) -> list[str]:
    """
    Return expected columns present in a DataFrame.

    If strict=True, raise an error when any expected columns are missing.
    Otherwise, log a warning and skip missing columns.
    """
    expected = list(expected_columns)
    missing = [column for column in expected if column not in df.columns]

    if missing and strict:
        raise ValueError(f"Missing expected column(s): {missing}")

    for column in missing:
        LOGGER.warning("Skipping missing column: %s", column)

    return [column for column in expected if column in df.columns]


def normalize_investment_dataframe(
    df: pd.DataFrame,
    *,
    strict_columns: bool = False,
) -> pd.DataFrame:
    """
    Normalize investment CSV data.

    Args:
        df:
            Input investment data.
        strict_columns:
            Whether to fail when expected columns are missing.

    Returns:
        A normalized copy of the input DataFrame.
    """
    normalized_df = df.copy()

    titlecase_columns = existing_columns(
        normalized_df,
        TITLECASE_COLUMNS,
        strict=strict_columns,
    )
    percent_columns = existing_columns(
        normalized_df,
        PERCENT_COLUMNS,
        strict=strict_columns,
    )

    for column in titlecase_columns:
        LOGGER.info("Title-casing column: %s", column)
        normalized_df[column] = normalized_df[column].apply(smart_titlecase)

    for column in percent_columns:
        LOGGER.info("Normalizing percent column: %s", column)
        normalized_df[column] = normalize_percent_series(normalized_df[column])

    if FORWARD_PE_COLUMN in normalized_df.columns:
        LOGGER.info("Normalizing numeric column: %s", FORWARD_PE_COLUMN)
        normalized_df[FORWARD_PE_COLUMN] = normalize_forward_pe_series(
            normalized_df[FORWARD_PE_COLUMN]
        )
    elif strict_columns:
        raise ValueError(f"Missing expected column: {FORWARD_PE_COLUMN}")
    else:
        LOGGER.warning("Skipping missing column: %s", FORWARD_PE_COLUMN)

    return normalized_df


def normalize_investment_csv(
    *,
    input_path: Path,
    output_path: Path,
    strict_columns: bool = False,
) -> None:
    """
    Read, normalize, and write an investment CSV file.

    Args:
        input_path:
            Source CSV path.
        output_path:
            Destination CSV path.
        strict_columns:
            Whether to fail when expected columns are missing.
    """
    input_path = input_path.expanduser().resolve()
    output_path = output_path.expanduser().resolve()

    if not input_path.is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {input_path}")

    LOGGER.info("Reading %s", input_path)
    df = pd.read_csv(input_path)

    LOGGER.info("Normalizing %d row(s) and %d column(s)", *df.shape)

    normalized_df = normalize_investment_dataframe(
        df,
        strict_columns=strict_columns,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized_df.to_csv(output_path, index=False)

    LOGGER.info("Wrote normalized CSV to %s", output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize investment CSV exports."
    )

    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Input CSV file.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output CSV file.",
    )
    parser.add_argument(
        "--strict-columns",
        action="store_true",
        help="Fail if expected columns are missing instead of warning and skipping.",
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

    try:
        normalize_investment_csv(
            input_path=args.input,
            output_path=args.output,
            strict_columns=args.strict_columns,
        )
    except (OSError, ValueError, pd.errors.ParserError) as exc:
        LOGGER.error("Failed to normalize investment CSV: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())