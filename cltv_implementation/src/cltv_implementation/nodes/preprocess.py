"""
Pre‑processing nodes for transactional, customer (demographic) and behavioral tables.
• Standardises column names with YAML alias files
• Performs minimal cleaning so downstream CLTV logic gets tidy data
"""

from __future__ import annotations

import pandas as pd
from cltv_implementation.nodes.column_mapper import standardize_columns

# 1 ────────────────────────────────────────────────────────────
def preprocess_transactional(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df, data_type="transactional")

    # Parse dates
    if "purchase_date" in df.columns:
        df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")

    # Drop rows missing critical fields
    df = df.dropna(subset=["customer_id", "purchase_date", "revenue"])

    # Coerce types
    df["customer_id"] = df["customer_id"].astype(str)
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0.0)
    return df.reset_index(drop=True)


# 2 ────────────────────────────────────────────────────────────
def preprocess_customer(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df, data_type="customer")

    df["customer_id"] = df["customer_id"].astype(str)

    if "date_of_birth" in df.columns:
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")
        today = pd.Timestamp.today().normalize()
        age = (today - df["date_of_birth"]).dt.days / 365.25
        df.loc[(age < 0) | (age > 120), "date_of_birth"] = pd.NaT

    return df.drop_duplicates("customer_id").reset_index(drop=True)


# 3 ────────────────────────────────────────────────────────────
def preprocess_behavioral(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df, data_type="behavioral")

    if "customer_id" not in df.columns:
        raise ValueError("Column 'customer_id' could not be found after standardisation.")

    df["customer_id"] = df["customer_id"].astype(str)

    numeric_cols = [c for c in df.columns if c not in {"customer_id"}]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    # Aggregate to one row per customer
    return df.groupby("customer_id", as_index=False).sum(numeric_only=True).reset_index(drop=True)
