#!/usr/bin/env python3
import re
import logging
from pathlib import Path
from typing import List, Dict

import pandas as pd

# ---- Configuration ----
COLUMN_ROW_NUMBER = 10
LOG_LEVEL = logging.INFO

# Map a group name to the regex that selects its files and final CSV suffix
GROUPS: Dict[str, Dict[str, str]] = {
    "MF":        {"pattern": r"^MF.*\.xlsx$",                   "suffix": "_MF.csv"},
    "ETF":       {"pattern": r"^ETF.*\.xlsx$",                  "suffix": "_ETF.csv"},
    "SMA":       {"pattern": r"^SMA.*\.xlsx$",                  "suffix": "_SMA.csv"},
    "Envestnet": {"pattern": r"(Envestnet\sStrategies|AMPF\sSelect\sStrategies).*\.xlsx$",
                  "suffix": "_EnvestnetPlus.csv"},
    "MS_Extract": {"pattern": r"^(MF|ETF|SMA).*\.xlsx$",         "suffix": "_MS_Extract.csv"},
}

# ---- Setup logging ----
logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s %(levelname)-8s %(message)s")

def get_header_row(file_name: Path) -> int:
    """
    Determine which 1-based row to use as header for this file.
    Can be customized per-file by inspecting file_name.
    """
    # Example override: AMPF Select Strategies also uses the default
    if re.search(r"AMPF\sSelect\sStrategies", file_name.name):
        return COLUMN_ROW_NUMBER
    return COLUMN_ROW_NUMBER

def extract_valid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only those rows where the first column is a string of length 10.
    """
    first_col = df.columns[0]
    mask = (
        df[first_col].apply(lambda x: isinstance(x, str) and len(x) == 10)
    )
    return df.loc[mask]

def combine_files(
    directory: Path,
    files: List[Path],
    output_file: Path,
    header_row_fn=get_header_row,
    filter_fn=extract_valid_rows,
) -> None:
    """
    Reads each Excel file, filters its rows, concatenates them,
    and writes out a single CSV.
    """
    if not files:
        logging.warning("No files to combine for %s", output_file.name)
        return

    logging.info("Combining %d files into %s", len(files), output_file.name)

    dfs: List[pd.DataFrame] = []
    total_expected_rows = 0
    expected_columns = None

    for idx, fp in enumerate(files, start=1):
        hrow = header_row_fn(fp)
        logging.info("[%d/%d] Reading %s (header row=%d)",
                     idx, len(files), fp.name, hrow)

        df = pd.read_excel(fp, header=hrow - 1)
        nrows, ncols = df.shape
        logging.info("  → raw rows=%d, cols=%d", nrows, ncols)

        total_expected_rows += nrows
        expected_columns = ncols if expected_columns is None else expected_columns

        valid = filter_fn(df)
        dfs.append(valid)

    # concatenate all filtered frames
    result = pd.concat(dfs, ignore_index=True)
    actual_rows, actual_cols = result.shape

    logging.info("Expected total rows: %d, columns: %d",
                 total_expected_rows, expected_columns)
    logging.info("Actual   total rows: %d, columns: %d",
                 actual_rows, actual_cols)
    logging.info("Differences: rows=%d, cols=%d",
                 total_expected_rows - actual_rows,
                 expected_columns - actual_cols if expected_columns is not None else 0)

    result.to_csv(output_file, index=False)
    logging.info("Written combined CSV to %s", output_file)

def find_excel_files(directory: Path) -> List[Path]:
    """Return all .xlsx files under the given directory."""
    return sorted(directory.glob("*.xlsx"))


def group_files(
    files: List[Path],
    groups: Dict[str, Dict[str, str]]
) -> Dict[str, List[Path]]:
    """
    For each group, select the files whose names match that group's pattern.
    Returns a map group_name → list of matching Path objects.
    """
    grouped = {}
    for name, info in groups.items():
        pat = re.compile(info["pattern"])
        matched = [f for f in files if pat.search(f.name)]
        grouped[name] = matched
    return grouped


def main():
    # 1) Base directory
    directory = Path("/home/andrew/Documents/Morningstar-March-2026/")
    if not directory.is_dir():
        logging.error("Directory %s does not exist!", directory)
        return

    # 2) Find and group files
    all_files = find_excel_files(directory)
    grouped  = group_files(all_files, GROUPS)

    # 3) Combine each group
    for name, info in GROUPS.items():
        files       = grouped.get(name, [])
        out_suffix  = info["suffix"]
        out_path    = directory / out_suffix
        combine_files(directory, files, out_path)


if __name__ == "__main__":
    main()