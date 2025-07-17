# feature_engineering.py

import pandas as pd
import numpy as np

def assign_score(value, thresholds, reverse=False):
    if reverse:
        if value <= thresholds[0]: return 5
        elif value <= thresholds[1]: return 4
        elif value <= thresholds[2]: return 3
        elif value <= thresholds[3]: return 2
        else: return 1
    else:
        if value <= thresholds[0]: return 1
        elif value <= thresholds[1]: return 2
        elif value <= thresholds[2]: return 3
        elif value <= thresholds[3]: return 4
        else: return 5

def assign_segment(row):
    r = row['r_score']
    fm = row['fm_score']
    if (r == 5 and fm == 5) or (r == 5 and fm == 4) or (r == 4 and fm == 5):
        return 'Champions'
    elif (r == 5 and fm == 3) or (r == 4 and fm == 4) or (r == 3 and fm == 5) or (r == 3 and fm == 4):
        return 'Loyal Customers'
    elif (r == 5 and fm == 2) or (r == 4 and fm == 2) or (r == 3 and fm == 3) or (r == 4 and fm == 3):
        return 'Potential Loyalists'
    elif r == 5 and fm == 1:
        return 'Recent Customers'
    elif (r == 4 and fm == 1) or (r == 3 and fm == 1):
        return 'Promising'
    elif (r == 3 and fm == 2) or (r == 2 and fm == 3) or (r == 2 and fm == 2):
        return 'Customers Needing Attention'
    elif r == 2 and fm == 1:
        return 'About to Sleep'
    elif (r == 2 and fm == 5) or (r == 2 and fm == 4) or (r == 1 and fm == 3):
        return 'At Risk'
    elif (r == 1 and fm == 5) or (r == 1 and fm == 4):
        return "Can't Lose Them"
    elif r == 1 and fm == 2:
        return 'Hibernating'
    elif r == 1 and fm == 1:
        return 'Lost'
    else:
        return 'Unclassified'

def calculate_rfm(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure correct datetime format
    df['purchase_date'] = pd.to_datetime(df['purchase_date'], errors='coerce')

    # Set snapshot date
    today = df['purchase_date'].max() + pd.Timedelta(days=1)

    # Aggregate base RFM metrics
    customer_df = df.groupby('customer_id').agg(
        Recency=('purchase_date', lambda x: (today - x.max()).days),
        Frequency=('transaction_id', 'nunique'),
        Monetary=('revenue', 'sum'),
        LastPurchaseDate=('purchase_date', 'max'),
        FirstPurchaseDate=('purchase_date', 'min')
    ).reset_index()

    # Calculate percentiles
    monetary_percentiles = np.percentile(customer_df['Monetary'], q=[20, 40, 60, 80])
    frequency_percentiles = np.percentile(customer_df['Frequency'], q=[20, 40, 60, 80])
    recency_percentiles = np.percentile(customer_df['Recency'], q=[20, 40, 60, 80])

    # Assign R, F, M scores
    customer_df['m_score'] = customer_df['Monetary'].apply(lambda x: assign_score(x, monetary_percentiles))
    customer_df['f_score'] = customer_df['Frequency'].apply(lambda x: assign_score(x, frequency_percentiles))
    customer_df['r_score'] = customer_df['Recency'].apply(lambda x: assign_score(x, recency_percentiles, reverse=True))

    # Combined FM score
    customer_df['fm_score'] = ((customer_df['f_score'] + customer_df['m_score']) / 2).round().astype(int)

    # Segment assignment
    customer_df['rfm_segment'] = customer_df.apply(assign_segment, axis=1)

    return customer_df[[
        'customer_id', 'Recency', 'Frequency', 'Monetary',
        'r_score', 'f_score', 'm_score', 'fm_score', 'rfm_segment'
    ]]
