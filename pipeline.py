import pandas as pd
import sqlite3
from pathlib import Path

# -----------------------
# Paths
# -----------------------
DATA_PATH = "data/raw_sales_data.csv"
DB_PATH = "database/sales.db"
REPORT_PATH = "reports/summary_report.txt"

# Create folders if they don't exist
Path("database").mkdir(exist_ok=True)
Path("reports").mkdir(exist_ok=True)

# -----------------------
# STEP 1: Load Raw Data
# -----------------------
df = pd.read_csv(DATA_PATH)

# -----------------------
# STEP 2: Data Cleaning
# -----------------------
df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

# Fill missing price with 0 instead of dropping everything
df["price"] = df["price"].fillna(0)

# Drop rows where date is missing
df = df.dropna(subset=["order_date"])

# Type conversions
df["quantity"] = df["quantity"].astype(int)
df["price"] = df["price"].astype(float)

# Create calculated column
df["total_amount"] = df["quantity"] * df["price"]

# Debug check (optional but useful)
print("Rows after cleaning:", len(df))
print(df.head())

# -----------------------
# STEP 3: Store in SQL
# -----------------------
conn = sqlite3.connect(DB_PATH)
df.to_sql("sales", conn, if_exists="replace", index=False)

# -----------------------
# STEP 4: Analysis Using SQL
# -----------------------
query_total_sales = """
SELECT ROUND(SUM(total_amount), 2) AS total_revenue
FROM sales;
"""

query_top_product = """
SELECT product, SUM(quantity) AS total_sold
FROM sales
GROUP BY product
ORDER BY total_sold DESC
LIMIT 1;
"""

query_top_customer = """
SELECT customer_id, ROUND(SUM(total_amount), 2) AS spend
FROM sales
GROUP BY customer_id
ORDER BY spend DESC
LIMIT 1;
"""

total_sales = pd.read_sql(query_total_sales, conn)
top_product = pd.read_sql(query_top_product, conn)
top_customer = pd.read_sql(query_top_customer, conn)

conn.close()

# -----------------------
# STEP 5: Generate Report (SAFE)
# -----------------------
total_revenue = total_sales.iloc[0, 0] if not total_sales.empty else 0

with open(REPORT_PATH, "w", encoding="utf-8") as f:
    f.write("SALES DATA SUMMARY REPORT\n")
    f.write("=========================\n\n")

    f.write(f"Total Revenue: INR {total_revenue}\n\n")

    f.write("Top Selling Product:\n")
    if not top_product.empty:
        f.write(
            f"{top_product.iloc[0]['product']} "
            f"({top_product.iloc[0]['total_sold']} units)\n\n"
        )
    else:
        f.write("No data available\n\n")

    f.write("Top Customer by Spend:\n")
    if not top_customer.empty:
        f.write(
            f"{top_customer.iloc[0]['customer_id']} "
            f"(INR {top_customer.iloc[0]['spend']})\n"
        )
    else:
        f.write("No data available\n")

print(" Pipeline executed successfully.")
print(" Report generated at:", REPORT_PATH)
print(" Database stored at:", DB_PATH)
