#!/usr/bin/env python3
"""
Combine Morningstar-style Excel exports into consolidated CSV files.

The script scans an input directory for `.xlsx` files, groups them by filename
patterns, reads each file using a configurable header row, filters valid data
rows, and writes one CSV per output group.

By default, the grouping rules are tailored to common Morningstar export files:

    MF*.xlsx                      -> morningstar_mf.csv
    ETF*.xlsx                     -> morningstar_etf.csv
    SMA*.xlsx                     -> morningstar_sma.csv
    Envestnet Strategies*.xlsx    -> morningstar_envestnet_plus.csv
    AMPF Select Strategies*.xlsx  -> morningstar_envestnet_plus.csv
    MF/ETF/SMA*.xlsx              -> morningstar_ms_extract.csv

Example:
    python scripts/combine_morningstar_exports.py \\
        --input-dir ~/Downloads/morningstar_exports \\
        --output-dir ./combined_exports

Notes:
    - Header rows are 1-based. The default header row is 10.
    - A valid data row is identified by a first-column string of length 10.
      This matches the original Morningstar-export workflow, where the first
      column is expected to contain a 10-character identifier.
"""

from __future__ import annotations

import argparse
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd


LOGGER = logging.getLogger(__name__)

DEFAULT_HEADER_ROW = 10


@dataclass(frozen=True)
class ExportGroup:
    """A named group of input files and its corresponding CSV output."""

    name: str
    pattern: str
    output_filename: str


DEFAULT_GROUPS: tuple[ExportGroup, ...] = (
    ExportGroup(
        name="Mutual Funds",
        pattern=r"^MF.*\.xlsx$",
        output_filename="morningstar_mf.csv",
    ),
    ExportGroup(
        name="ETFs",
        pattern=r"^ETF.*\.xlsx$",
        output_filename="morningstar_etf.csv",
    ),
    ExportGroup(
        name="SMAs",
        pattern=r"^SMA.*\.xlsx$",
        output_filename="morningstar_sma.csv",
    ),
    ExportGroup(
        name="Envestnet Plus",
        pattern=r"(Envestnet\sStrategies|AMPF\sSelect\sStrategies).*\.xlsx$",
        output_filename="morningstar_envestnet_plus.csv",
    ),
    ExportGroup(
        name="Morningstar Extract",
        pattern=r"^(MF|ETF|SMA).*\.xlsx$",
        output_filename="morningstar_ms_extract.csv",
    ),
)


def find_excel_files(input_dir: Path) -> list[Path]:
    """Return all `.xlsx` files directly inside the input directory."""
    return sorted(input_dir.glob("*.xlsx"))


def matching_files(files: Iterable[Path], pattern: str) -> list[Path]:
    """Return files whose names match the supplied regular expression."""
    compiled_pattern = re.compile(pattern)
    return [file_path for file_path in files if compiled_pattern.search(file_path.name)]


def default_header_row_for_file(file_path: Path, default_header_row: int) -> int:
    """
    Return the 1-based header row to use for an input file.

    This function exists as an extension point for future per-file overrides.
    For now, all supported Morningstar-style exports use the same default.
    """
    return default_header_row


def extract_valid_rows(df: pd.DataFrame, identifier_length: int) -> pd.DataFrame:
    """
    Keep rows where the first column contains an identifier of the expected length.

    Args:
        df:
            DataFrame loaded from a Morningstar-style Excel export.
        identifier_length:
            Required string length for the identifier in the first column.

    Returns:
        Filtered DataFrame containing only rows that match the identifier rule.
    """
    if df.empty:
        return df

    first_column = df.columns[0]
    valid_row_mask = df[first_column].apply(
        lambda value: isinstance(value, str) and len(value.strip()) == identifier_length
    )

    return df.loc[valid_row_mask].copy()


def combine_excel_files(
    *,
    files: list[Path],
    output_path: Path,
    header_row_fn: Callable[[Path], int],
    identifier_length: int,
) -> None:
    """
    Read, filter, concatenate, and write a group of Excel files.

    Args:
        files:
            Excel files to combine.
        output_path:
            Destination CSV path.
        header_row_fn:
            Function returning the 1-based header row for each file.
        identifier_length:
            Required first-column identifier length for valid rows.
    """
    if not files:
        LOGGER.warning("No matching files found for %s", output_path.name)
        return

    frames: list[pd.DataFrame] = []
    total_raw_rows = 0
    expected_column_count: int | None = None

    LOGGER.info("Combining %d file(s) into %s", len(files), output_path)

    for index, file_path in enumerate(files, start=1):
        header_row = header_row_fn(file_path)

        LOGGER.info(
            "[%d/%d] Reading %s using header row %d",
            index,
            len(files),
            file_path.name,
            header_row,
        )

        df = pd.read_excel(file_path, header=header_row - 1)
        raw_rows, column_count = df.shape

        total_raw_rows += raw_rows

        if expected_column_count is None:
            expected_column_count = column_count
        elif column_count != expected_column_count:
            LOGGER.warning(
                "%s has %d column(s); expected %d",
                file_path.name,
                column_count,
                expected_column_count,
            )

        filtered_df = extract_valid_rows(df, identifier_length=identifier_length)

        LOGGER.info(
            "%s: raw rows=%d, valid rows=%d, columns=%d",
            file_path.name,
            raw_rows,
            len(filtered_df),
            column_count,
        )

        frames.append(filtered_df)

    if not frames:
        LOGGER.warning("No data frames were created for %s", output_path.name)
        return

    combined_df = pd.concat(frames, ignore_index=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(output_path, index=False)

    LOGGER.info(
        "Wrote %d valid row(s) from %d raw row(s) to %s",
        len(combined_df),
        total_raw_rows,
        output_path,
    )


def combine_morningstar_exports(
    *,
    input_dir: Path,
    output_dir: Path,
    groups: tuple[ExportGroup, ...] = DEFAULT_GROUPS,
    default_header_row: int = DEFAULT_HEADER_ROW,
    identifier_length: int = 10,
) -> None:
    """
    Combine Morningstar-style Excel exports into grouped CSV files.

    Args:
        input_dir:
            Directory containing source `.xlsx` files.
        output_dir:
            Directory where output CSV files should be written.
        groups:
            Export grouping rules.
        default_header_row:
            Default 1-based row number containing column headers.
        identifier_length:
            Required first-column identifier length for valid data rows.
    """
    input_dir = input_dir.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()

    if not input_dir.is_dir():
        raise NotADirectoryError(f"Input directory does not exist: {input_dir}")

    excel_files = find_excel_files(input_dir)

    if not excel_files:
        LOGGER.warning("No .xlsx files found in %s", input_dir)
        return

    LOGGER.info("Found %d .xlsx file(s) in %s", len(excel_files), input_dir)

    def header_row_fn(file_path: Path) -> int:
        return default_header_row_for_file(file_path, default_header_row)

    for group in groups:
        files = matching_files(excel_files, group.pattern)
        output_path = output_dir / group.output_filename

        LOGGER.info("Processing group: %s", group.name)

        combine_excel_files(
            files=files,
            output_path=output_path,
            header_row_fn=header_row_fn,
            identifier_length=identifier_length,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine Morningstar-style Excel exports into grouped CSV files."
    )

    parser.add_argument(
        "--input-dir",
        required=True,
        type=Path,
        help="Directory containing Morningstar-style .xlsx export files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Directory where combined CSV files should be written. "
            "Defaults to the input directory."
        ),
    )
    parser.add_argument(
        "--header-row",
        type=int,
        default=DEFAULT_HEADER_ROW,
        help="1-based row number containing column headers. Defaults to 10.",
    )
    parser.add_argument(
        "--identifier-length",
        type=int,
        default=10,
        help=(
            "Required length of the first-column identifier for valid data rows. "
            "Defaults to 10."
        ),
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

    output_dir = args.output_dir if args.output_dir is not None else args.input_dir

    try:
        combine_morningstar_exports(
            input_dir=args.input_dir,
            output_dir=output_dir,
            default_header_row=args.header_row,
            identifier_length=args.identifier_length,
        )
    except (OSError, ValueError, pd.errors.EmptyDataError) as exc:
        LOGGER.error("Failed to combine Morningstar exports: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())