import pandas as pd

def calculate_rfm(df: pd.DataFrame) -> pd.DataFrame:
    df['purchase_date'] = pd.to_datetime(df['purchase_date'], errors='coerce')
    snapshot_date = df['purchase_date'].max() + pd.Timedelta(days=1)

    rfm = df.groupby('customer_id').agg({
        'purchase_date': lambda x: (snapshot_date - x.max()).days,  # Recency
        'transaction_id': 'count', 
        'revenue': 'sum'         
    }).rename(columns={
        'purchase_date': 'recency',
        'invoice_no': 'frequency',
        'amount': 'monetary'
    }).reset_index()

    return rfm
