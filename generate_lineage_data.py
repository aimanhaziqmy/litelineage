import time
import random
from litelineage.core import tracker

print("🚀 Starting Dummy E-Commerce Pipeline...")

# --- PHASE 1: INGESTION ---

@tracker.track(inputs=["api:stripe/orders"], outputs=["raw/orders.json"])
def ingest_orders():
    print("  [1/5] Ingesting orders from Stripe API...")
    time.sleep(0.2) # Simulate work

@tracker.track(inputs=["db:postgres/users"], outputs=["raw/users.csv"])
def ingest_users():
    print("  [2/5] Ingesting user data from Production DB...")
    time.sleep(0.2)

# --- PHASE 2: PROCESSING ---

@tracker.track(inputs=["raw/orders.json"], outputs=["clean/orders.parquet"])
def clean_orders():
    print("  [3/5] Cleaning and converting orders to Parquet...")
    time.sleep(0.3)

@tracker.track(inputs=["raw/users.csv"], outputs=["clean/users.parquet"])
def clean_users():
    print("  [4/5] Normalizing user data...")
    time.sleep(0.3)

# --- PHASE 3: AGGREGATION & REPORTING ---

@tracker.track(
    inputs=["clean/orders.parquet", "clean/users.parquet"], 
    outputs=["gold/daily_sales_report.pdf", "gold/marketing_dashboard.csv"]
)
def generate_daily_reports():
    print("  [5/5] Joining data and generating executive reports...")
    time.sleep(0.5)

# --- RUN THE PIPELINE ---

if __name__ == "__main__":
    # Simulate a run for "yesterday"
    ingest_orders()
    ingest_users()
    clean_orders()
    clean_users()
    generate_daily_reports()
    
    print("\n✅ Pipeline Finished! Metadata has been saved to 'lineage.db'.")