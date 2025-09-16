# load_from_kagglehub_to_postgres.py
import kagglehub
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Numeric, Date, Text
import os

# Download dataset
path = kagglehub.dataset_download("cameronseamons/electronic-sales-sep2023-sep2024")
print("Downloaded to:", path)

# Find CSV file
csv_files = [f for f in os.listdir(path) if f.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError("No CSV found in KaggleHub dataset folder")
CSV_PATH = os.path.join(path, csv_files[0])
print("Using file:", CSV_PATH)

# Read CSV
df = pd.read_csv(
    CSV_PATH,
    parse_dates=["Purchase Date"],
    dayfirst=False
)

# Normalize column names
rename_map = {
    "Customer ID": "customer_id",
    "Age": "age",
    "Gender": "gender",
    "Loyalty Member": "loyalty_member",
    "Product Type": "product_type",
    "SKU": "sku",
    "Rating": "rating",
    "Order Status": "order_status",
    "Payment Method": "payment_method",
    "Total Price": "total_price",
    "Unit Price": "unit_price",
    "Quantity": "quantity",
    "Purchase Date": "purchase_date",
    "Shipping Type": "shipping_type",
    "Add-ons Purchased": "add_ons_purchased",
    "Add-on Total": "add_on_total",
}
df = df.rename(columns=rename_map)

# Coerce numeric types
for col in ["age", "rating", "quantity"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

for col in ["total_price", "unit_price", "add_on_total"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Connect to Postgres
CONN_STR = "postgresql://admin:admin@localhost:5432/electronic_rfm"
engine = create_engine(CONN_STR)

TABLE_NAME = "sales_2023_2024"   
SCHEMA_NAME = "raw"

# Check if table exists → truncate if yes
with engine.begin() as conn:
    table_exists = conn.execute(text(f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{SCHEMA_NAME}'
            AND table_name   = '{TABLE_NAME}'
        )
    """)).scalar()

    if table_exists:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA_NAME}.{TABLE_NAME}"))
    else:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))

# Type mapping
dtype_map = {
    "customer_id": Integer(),
    "age": Integer(),
    "gender": Text(),
    "loyalty_member": Text(),
    "product_type": Text(),
    "sku": Text(),
    "rating": Integer(),
    "order_status": Text(),
    "payment_method": Text(),
    "total_price": Numeric(),
    "unit_price": Numeric(),
    "quantity": Integer(),
    "purchase_date": Date(),
    "shipping_type": Text(),
    "add_ons_purchased": Text(),
    "add_on_total": Numeric(),
}

# Load data
df.to_sql(
    TABLE_NAME,
    engine,
    schema=SCHEMA_NAME,
    if_exists="append",  # safe since we truncated earlier
    index=False,
    dtype=dtype_map
)

# Helpful indexes
with engine.begin() as conn:
    conn.execute(text(f"""
        CREATE INDEX IF NOT EXISTS idx_sales_purchase_date 
        ON {SCHEMA_NAME}.{TABLE_NAME} (purchase_date);

        CREATE INDEX IF NOT EXISTS idx_sales_customer_id  
        ON {SCHEMA_NAME}.{TABLE_NAME} (customer_id);
    """))

print(f"Loaded KaggleHub CSV → Postgres table {SCHEMA_NAME}.{TABLE_NAME}")