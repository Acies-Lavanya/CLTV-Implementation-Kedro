import pandas as pd
from lifetimes.utils import summary_data_from_transaction_data
from lifetimes import BetaGeoFitter, GammaGammaFitter

def train_bg_nbd_model(transactions_df: pd.DataFrame) -> pd.DataFrame:
    # Ensure date column is datetime
    transactions_df = transactions_df.copy()

    if 'purchase_date' not in transactions_df.columns:
        raise KeyError("Missing required column 'Purchase Date' in the transaction dataset.")

    transactions_df['purchase_date'] = pd.to_datetime(transactions_df['purchase_date'])

    purchase_dates = (
    transactions_df.groupby("customer_id")
    .agg(
        last_purchase_date=('purchase_date', 'max'),
        first_purchase_date=('purchase_date', 'min')
    )
    .reset_index()
)

    # Create summary from raw transaction log
    summary_df = summary_data_from_transaction_data(
        transactions_df,
        customer_id_col='customer_id',
        datetime_col='purchase_date',
        monetary_value_col='revenue',
        observation_period_end=transactions_df['purchase_date'].max()
    )

    summary_df = summary_df.merge(purchase_dates, on="customer_id", how="left")
#transaction_id,visit_id,customer_id,order_id,purchase_date,payment_method,revenue

    # Filter out non-repeaters and invalid monetary values
    summary_df = summary_df[(summary_df['frequency'] > 0) & (summary_df['monetary_value'] > 0)]

    if summary_df.empty:
        raise ValueError("No valid data for CLTV prediction.")

    # Fit BG/NBD model
    bgf = BetaGeoFitter(penalizer_coef=0.01)
    bgf.fit(summary_df['frequency'], summary_df['recency'], summary_df['T'])

    # Fit Gamma-Gamma model
    ggf = GammaGammaFitter(penalizer_coef=0.01)
    ggf.fit(summary_df['frequency'], summary_df['monetary_value'])

    # Predict 3-month (90-day) CLTV
    summary_df['predicted_cltv_3m'] = ggf.customer_lifetime_value(
        bgf,
        summary_df['frequency'],
        summary_df['recency'],
        summary_df['T'],
        summary_df['monetary_value'],
        time=3,        # in months
        freq='D',      # frequency of transaction is daily
        discount_rate=0.01
    )

    summary_df = summary_df.reset_index()
    return summary_df