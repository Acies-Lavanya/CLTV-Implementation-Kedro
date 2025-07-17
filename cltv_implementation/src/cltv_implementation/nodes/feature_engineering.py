import pandas as pd

def calculate_rfm(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure datetime format for purchase_date
    df['purchase_date'] = pd.to_datetime(df['purchase_date'], errors='coerce')
    
    # Define snapshot date as one day after the most recent transaction
    snapshot_date = df['purchase_date'].max() + pd.Timedelta(days=1)

    # Group by customer and compute RFM values
    rfm = df.groupby('customer_id').agg({
        'purchase_date': lambda x: (snapshot_date - x.max()).days,  # Recency
        'transaction_id': 'count',                                  # Frequency
        'revenue': 'sum'                                            # Monetary
    }).rename(columns={
        'purchase_date': 'recency',
        'transaction_id': 'frequency',
        'revenue': 'monetary'
    }).reset_index()

    return rfm
