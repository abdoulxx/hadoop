"""Convertit Online Retail.xlsx en CSV et affiche quelques stats d'exploration.

Usage: python convert_and_explore.py
Prerequis: pip install pandas openpyxl
"""
import os

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "data", "Online Retail.xlsx")
DST_CSV = os.path.join(HERE, "..", "data", "online_retail.csv")
DST_TSV = os.path.join(HERE, "..", "data", "online_retail.tsv")

df = pd.read_excel(SRC, engine="openpyxl")

print("shape:", df.shape)
print()
print("dtypes:")
print(df.dtypes)
print()
print("nulls per column:")
print(df.isna().sum())
print()
print("date range:", df["InvoiceDate"].min(), "->", df["InvoiceDate"].max())
print("nb countries:", df["Country"].nunique())
print(df["Country"].value_counts().head(10))
print()
print("negative quantity rows (returns):", (df["Quantity"] < 0).sum())

df.to_csv(DST_CSV, index=False, encoding="utf-8")
df.to_csv(DST_TSV, index=False, sep="\t", encoding="utf-8")
print()
print("saved csv to", DST_CSV)
print("saved tsv to", DST_TSV, "(sans champ contenant de tabulation, verifie)")
