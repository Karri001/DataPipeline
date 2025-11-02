import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import os
import logging
from datetime import datetime

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

# Get today's date for versioned folder names
today = datetime.now().strftime("%Y-%m-%d")
daily_output_dir = os.path.join("output", today)
os.makedirs(daily_output_dir, exist_ok=True)

# Define the log file path
log_file_path = os.path.join("output", "pipeline_log.csv")

def run_pipeline(input_csv, sqlite_db_path, agg_csv_path, chart_path):
    try:
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
        logging.info("Data cleaned and transformed. Rows remaining: %d", len(df))

        # 3. Store cleaned data into SQLite
        conn = sqlite3.connect(sqlite_db_path)
        df.to_sql("cleaned_sales", conn, if_exists="replace", index=False)
        conn.close()
        logging.info("Saved cleaned data to database: %s", sqlite_db_path)

        # 4. Aggregate
        agg = df.groupby('region', as_index=False).agg(
            total_sales_amount=pd.NamedAgg(column='total_amount', aggfunc='sum'),
            total_orders=pd.NamedAgg(column='order_id', aggfunc='count'),
            avg_order_value=pd.NamedAgg(column='total_amount', aggfunc='mean')
        ).sort_values('total_sales_amount', ascending=False)
        agg.to_csv(agg_csv_path, index=False)
        logging.info("Aggregated data saved to: %s", agg_csv_path)

        # 5. Plot
        plt.figure(figsize=(6,4))
        plt.bar(agg['region'], agg['total_sales_amount'])
        plt.title("Total Sales Amount by Region")
        plt.xlabel("Region")
        plt.ylabel("Total Sales Amount")
        plt.tight_layout()
        plt.savefig(chart_path)
        plt.close()
        logging.info("Chart saved to: %s", chart_path)

        # 6. Log run summary
        log_run_summary(df, agg, "success")

    except Exception as e:
        logging.error("Pipeline failed: %s", str(e))
        log_run_summary(pd.DataFrame(), pd.DataFrame(), "failed")

def log_run_summary(df, agg, status):
    """Append one line of metadata about this run to pipeline_log.csv."""
    total_sales = df['total_amount'].sum() if not df.empty else 0
    total_orders = len(df)
    unique_regions = df['region'].nunique() if not df.empty else 0

    log_entry = pd.DataFrame([{
        "run_date": today,
        "rows_processed": total_orders,
        "total_sales": round(total_sales, 2),
        "unique_regions": unique_regions,
        "status": status
    }])

    # If log file doesn’t exist, create it with headers; else append
    if not os.path.exists(log_file_path):
        log_entry.to_csv(log_file_path, index=False)
    else:
        log_entry.to_csv(log_file_path, mode='a', header=False, index=False)

    logging.info("Logged pipeline run to: %s", log_file_path)

if __name__ == "__main__":
    run_pipeline(
        input_csv="data/raw_sales.csv",
        sqlite_db_path=os.path.join(daily_output_dir, f"sales_{today}.db"),
        agg_csv_path=os.path.join(daily_output_dir, f"sales_by_region_{today}.csv"),
        chart_path=os.path.join(daily_output_dir, f"sales_by_region_{today}.png")
    )
    print(f"✅ Pipeline finished for {today}. Check {daily_output_dir} and pipeline_log.csv")
