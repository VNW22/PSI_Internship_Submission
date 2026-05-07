import pandas as pd
import numpy as np
import os

def load_data(data_path):
    os.makedirs("output/rejected", exist_ok=True)
    
    def process_source(name, numeric_cols=[]):
        df = pd.read_csv(f"{data_path}/{name}.csv")
        valid_mask = df.notnull().all(axis=1)
        valid = df[valid_mask].copy()
        rejected = df[~valid_mask].copy()
        
        for col in numeric_cols:
            if col in valid.columns:
                valid[col] = pd.to_numeric(valid[col], errors='coerce')
        
        rejected.to_csv(f"output/rejected/{name}.csv", index=False)
        return valid

    cust = process_source("customers")
    items = process_source("order_items", ['quantity', 'unit_price'])
    orders = process_source("orders", ['total_amount', 'discount_pct'])
    returns = process_source("returns", ['refund_amount'])
    
    return cust, items, orders, returns

def clean_and_enrich(cust, items, orders, returns):
    cust = cust.drop_duplicates().copy()
    cust['signup_date'] = pd.to_datetime(cust['signup_date'], errors='coerce')
    cust = cust.dropna(subset=['signup_date'])
    cust['customer_tier'] = cust['customer_tier'].str.lower()

    orders = orders.drop_duplicates().copy()
    orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')
    orders = orders.dropna(subset=['order_date'])
    orders['is_negative_amount'] = orders['total_amount'] < 0

    items = items.drop_duplicates()
    returns['return_date'] = pd.to_datetime(returns['return_date'], errors='coerce')
    returns = returns.dropna(subset=['return_date']).drop_duplicates()

    orphans = items[~items['order_id'].isin(orders['order_id'])]
    os.makedirs("output/orphaned", exist_ok=True)
    orphans.to_csv("output/orphaned/orphaned_items.csv", index=False)

    merged = orders.merge(cust, on="customer_id", how="inner")
    enriched = merged.merge(items, on="order_id", how="inner")
    
    enriched['net_amount'] = enriched['total_amount'] * (1 - enriched['discount_pct'] / 100)
    
    enriched = enriched.sort_values(['country', 'net_amount'], ascending=[True, False])
    enriched['rank'] = enriched.groupby('country')['net_amount'].rank(ascending=False, method='dense')
    
    enriched = enriched.sort_values(['customer_id', 'order_date']).reset_index(drop=True)
    rolling = (
        enriched.groupby('customer_id')
        .rolling('7D', on='order_date')['order_id']
        .count()
        .reset_index()
    )
    enriched['rolling_7d_orders'] = rolling['order_id']
    
    return enriched, returns

def run_analysis(enriched, returns):
    enriched['order_date'] = pd.to_datetime(enriched['order_date'])
    enriched['month'] = enriched['order_date'].dt.to_period('M')
    enriched['revenue'] = enriched['quantity'] * enriched['unit_price']
    
    cat_rev = enriched.groupby(['month', 'category'])['revenue'].sum().reset_index()
    tot_rev = enriched.groupby('month')['revenue'].sum().reset_index().rename(columns={'revenue': 'total_revenue'})
    share = cat_rev.merge(tot_rev, on='month')
    share['share'] = share['revenue'] / share['total_revenue']

    ret_merged = returns.merge(enriched, on="order_id", how="left")
    ret_merged['refund_exceeds_order'] = ret_merged['refund_amount'] > ret_merged['net_amount']
    
    order_count = enriched['order_id'].nunique()
    rates = ret_merged.groupby(['category', 'customer_tier']).size().reset_index(name='count')
    rates['rate'] = rates['count'] / order_count
    
    top_refunds = ret_merged.groupby('customer_id')['refund_amount'].sum().nlargest(10).reset_index()
    
    return share, rates, top_refunds

def save_outputs(enriched, share, rates, top_refunds):
    os.makedirs("output/summaries", exist_ok=True)
    os.makedirs("output/final_data", exist_ok=True)
    
    if enriched['customer_id'].isnull().any():
        raise ValueError("Data Quality Gate: Found NULL customers")

    enriched['year'] = enriched['order_date'].dt.year
    enriched['month_num'] = enriched['order_date'].dt.month
    
    enriched.to_parquet("output/final_data/orders.parquet", partition_cols=['year', 'month_num'], index=False)
    
    share.to_csv("output/summaries/category_share.csv", index=False)
    rates.to_csv("output/summaries/return_rates.csv", index=False)
    top_refunds.to_csv("output/summaries/top_customers.csv", index=False)

def main():
    c, i, o, r = load_data("data")
    en, ret = clean_and_enrich(c, i, o, r)
    s, ra, t = run_analysis(en, ret)
    save_outputs(en, s, ra, t)

if __name__ == "__main__":
    main()
