from __future__ import annotations
import pandas as pd
from cltv_implementation.nodes.column_mapper import standardize_columns

# ───────────────────────────────────────────────────────────
def preprocess_transactional(df: pd.DataFrame, selected_tables: list[str]) -> pd.DataFrame:
    if "transactional" not in selected_tables:
        return df.head(0)

    df = standardize_columns(df, data_type="transactional")

    if "purchase_date" in df.columns:
        df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce", dayfirst=False)

    df = df.dropna(subset=["customer_id", "purchase_date", "revenue"])
    df["customer_id"] = df["customer_id"].astype(str)
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0.0)

    return df.reset_index(drop=True)
# ───────────────────────────────────────────────────────────
def preprocess_customer(df: pd.DataFrame, selected_tables: list[str]) -> pd.DataFrame | None:
    if "customer" not in selected_tables or df is None:
        return df.head(0) if df is not None else None

    df = standardize_columns(df, data_type="customer")
    df["customer_id"] = df["customer_id"].astype(str)

    if "date_of_birth" in df.columns:
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce", dayfirst=False)
        today = pd.Timestamp.today().normalize()
        age = (today - df["date_of_birth"]).dt.days / 365.25
        df.loc[(age < 0) | (age > 120), "date_of_birth"] = pd.NaT

    return df.drop_duplicates("customer_id").reset_index(drop=True)

# ───────────────────────────────────────────────────────────
def preprocess_behavioral(df: pd.DataFrame | None, selected_tables: list[str]) -> pd.DataFrame | None:
    if "behavioral" not in selected_tables or df is None:
        return df.head(0) if df is not None else None

    df = standardize_columns(df, data_type="behavioral")

    if "customer_id" not in df.columns:
        raise ValueError("Column 'customer_id' could not be found after standardisation.")

    df["customer_id"] = df["customer_id"].astype(str)

    numeric_cols = [c for c in df.columns if c not in {"customer_id"}]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    return df.groupby("customer_id", as_index=False).sum(numeric_only=True).reset_index(drop=True)
