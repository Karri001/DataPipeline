import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt

st.set_page_config(page_title="Sales Pipeline Dashboard", layout="wide")

st.title("📊 Data Pipeline Dashboard")
st.markdown("This live dashboard is powered by your daily pipeline outputs.")

# Paths
log_file = "output/pipeline_log.csv"
output_dir = "output"

# ---- Load pipeline log ----
if os.path.exists(log_file):
    log_df = pd.read_csv(log_file)
    st.subheader("📈 Pipeline Run Summary")
    st.dataframe(log_df.tail(10), use_container_width=True)

    # Plot total sales over time
    fig, ax = plt.subplots(figsize=(6,3))
    ax.plot(log_df["run_date"], log_df["total_sales"], marker="o", linewidth=2)
    ax.set_title("Total Sales Trend")
    ax.set_xlabel("Run Date")
    ax.set_ylabel("Total Sales")
    ax.grid(True, linestyle="--", alpha=0.6)
    st.pyplot(fig)
else:
    st.warning("No pipeline_log.csv found yet. Run the pipeline first.")

# ---- Latest daily summary ----
st.markdown("---")
st.subheader("🗺️ Latest Sales by Region")

# Find latest daily folder
folders = sorted([f for f in os.listdir(output_dir) if os.path.isdir(os.path.join(output_dir, f))])
if folders:
    latest_folder = os.path.join(output_dir, folders[-1])
    st.write(f"Showing data for **{folders[-1]}**")

    # Load regional data
    region_file = os.path.join(latest_folder, f"sales_by_region_{folders[-1]}.csv")
    if os.path.exists(region_file):
        region_df = pd.read_csv(region_file)
        st.dataframe(region_df, use_container_width=True)

        # Plot bar chart
        fig2, ax2 = plt.subplots(figsize=(5,3))
        ax2.bar(region_df["region"], region_df["total_sales_amount"], color="skyblue")
        ax2.set_title("Sales by Region")
        ax2.set_xlabel("Region")
        ax2.set_ylabel("Total Sales")
        st.pyplot(fig2)
    else:
        st.warning("No regional sales file found for latest date.")
else:
    st.warning("No output folders found yet.")
