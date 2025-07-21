import pandas as pd
from typing import Optional
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
    
def merge_user_level(transactional=None, customer=None, behavioral=None, selected_tables: list = None) -> Optional[pd.DataFrame]:
    # Transactional is mandatory
    if transactional is None or transactional.empty:
        raise ValueError("❌ Transactional data is required and is missing or empty.")

    # Ensure purchase_date is datetime
    
    customer_df = transactional.copy()
    
    monetary_percentiles = np.percentile(customer_df['monetary_value'], q=[20, 40, 60, 80])
    frequency_percentiles = np.percentile(customer_df['frequency'], q=[20, 40, 60, 80])
    recency_percentiles = np.percentile(customer_df['recency'], q=[20, 40, 60, 80])

    customer_df['m_score'] = customer_df['monetary_value'].apply(lambda x: assign_score(x, monetary_percentiles))
    customer_df['f_score'] = customer_df['frequency'].apply(lambda x: assign_score(x, frequency_percentiles))
    customer_df['r_score'] = customer_df['recency'].apply(lambda x: assign_score(x, recency_percentiles, reverse=True))

    customer_df['fm_score'] = ((customer_df['f_score'] + customer_df['m_score']) / 2).round().astype(int)

    # Segment assignment
    customer_df['rfm_segment'] = customer_df.apply(assign_segment, axis=1)

    user_df = customer_df.copy()

    # ---- Merge Behavioral (optional) ----


    if "behavioral" in selected_tables and behavioral is not None and not behavioral.empty:
        print("✅ Merging behavioral data")
        behavioral_agg = (
            behavioral.groupby("customer_id")
            .agg(
                total_sessions=("session_id", "nunique"),
                avg_page_views=("page_views", "mean"),
                avg_sponsors_viewed=("sponsored_listing_viewed", "mean"),
                total_session_cost=("session_total_cost", "sum"),
                preferred_device=("device_type", "max")
            )
            .reset_index()
        )
        user_df = user_df.merge(behavioral_agg, on="customer_id", how="left")
    else:
        print("⚠️ Skipping behavioral merge (not provided or empty)")

    # ---- Merge Customer Master (optional) ----
    if "customer" in selected_tables and customer is not None and not customer.empty:
        print("✅ Merging customer data")
        user_df = user_df.merge(customer, on="customer_id", how="left")
    else:
        print("⚠️ Skipping customer merge (not provided or empty)")

    return user_df
