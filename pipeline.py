import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
os.makedirs("output", exist_ok=True)

def run_pipeline(input_csv, sqlite_db_path, agg_csv_path, chart_path):
    # 1. Ingest
    df = pd.read_csv(input_csv)
    logging.info("Ingested CSV, rows=%d", len(df))

    # 2. Clean & transform
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['order_id','date','region','quantity','price'])
    df['quantity'] = df['quantity'].astype(int)
    df['price'] = df['price'].astype(float)
    df['total_amount'] = df['quantity'] * df['price']
    df['month'] = df['date'].dt.to_period('M').astype(str)

    # 3. Store cleaned data into SQLite
    conn = sqlite3.connect(sqlite_db_path)
    df.to_sql("cleaned_sales", conn, if_exists="replace", index=False)
    conn.close()

    # 4. Aggregate
    agg = df.groupby('region', as_index=False).agg(
        total_sales_amount=pd.NamedAgg(column='total_amount', aggfunc='sum'),
        total_orders=pd.NamedAgg(column='order_id', aggfunc='count'),
        avg_order_value=pd.NamedAgg(column='total_amount', aggfunc='mean')
    ).sort_values('total_sales_amount', ascending=False)
    agg.to_csv(agg_csv_path, index=False)

    # 5. Plot
    plt.figure(figsize=(6,4))
    plt.bar(agg['region'], agg['total_sales_amount'])
    plt.title("Total Sales Amount by Region")
    plt.xlabel("Region")
    plt.ylabel("Total Sales Amount")
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()

if __name__ == "__main__":
    run_pipeline(
        input_csv="data/raw_sales.csv",
        sqlite_db_path="output/sales.db",
        agg_csv_path="output/sales_by_region.csv",
        chart_path="output/sales_by_region.png"
    )
    print("Pipeline finished. Check the output/ folder.")
