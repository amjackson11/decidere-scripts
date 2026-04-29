#!/usr/bin/env python3

import pandas as pd

import re

titlecase_columns = ["Name",
                     "Model provider",
                     "Category",
                     "Model type",
                     "Status",
                     "Primary portfolio objective",
                     "Strategic/Tactical",
                     "Tax sensitivity"
                     ]

titlecase_ignore = {
    "AAA","ADR","AGG","ACWI","BNY","CDA","CL","CLO","CON","EAFE","ESG",
    "ETF","ETFS","ETN","FTSE","GBL","GNMA","LLC","LTD","MDL",
    "MD","MDT","MF","MOD","MFS","MSCI","NR","NYLI","PGIM","QQQ","SMA",
    "SPDR","USA","US","USD"
}

titlecase_special = {
    "JPMORGAN": "JPMorgan",
    "ISHARES": "iShares"
}

percent_columns = [
"Manager fee",
    "Estimated net expenses",
    "Yield",
#"Forward P/E",
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
]

def smart_titlecase(text):
    if pd.isna(text):
        return text

    s = str(text).title()

    # apply special replacements
    for k, v in titlecase_special.items():
        s = re.sub(rf"\b{k.title()}\b", v, s)

    # restore acronyms
    for word in titlecase_ignore:
        s = re.sub(rf"\b{word.title()}\b", word, s)

    return s



df = pd.read_csv("03-march-arc-investments.csv")

for col in titlecase_columns:
    df[col] = df[col].apply(smart_titlecase)

for col in percent_columns:
    df[col] = (
            pd.to_numeric(
                df[col].astype(str).str.rstrip("%"),
                errors="coerce"
            ) / 100
    ).round(4)

df["Forward P/E"] = pd.to_numeric(
    df["Forward P/E"].astype(str).str.rstrip("x"),
    errors="coerce"
)

df.to_csv("03-march-arc-investments-updated.csv", index=False)