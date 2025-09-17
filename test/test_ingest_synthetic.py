# test_ingest_synthetic.py
import pandas as pd
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect
import sys

# DB connection
engine = create_engine(
    "postgresql+psycopg2://admin:admin@localhost:5432/electronic_rfm"
)

def generate_monthly_sales(year, month, n=200, existing_ids=None):
    """
    Generate synthetic sales data for a given year and month.
    50% old customers, 50% new customers.
    """
    if existing_ids is None:
        existing_ids = list(range(1, 5001))  # simulate "old" customers

    new_ids = range(max(existing_ids) + 1, max(existing_ids) + n + 1)

    rows = []
    days_in_month = (datetime(year, month % 12 + 1, 1) - timedelta(days=1)).day

    for i in range(n):
        if i < n // 2:
            customer_id = random.choice(existing_ids)  # old customer
        else:
            customer_id = random.choice(new_ids)      # new customer

        purchase_date = datetime(year, month, 1) + timedelta(days=random.randint(0, days_in_month - 1))
        product_type = random.choice(["Smartphone", "Laptop", "Tablet", "Accessories"])
        unit_price = round(random.uniform(50, 1500), 2)
        quantity = random.randint(1, 3)
        total_price = round(unit_price * quantity, 2)

        rows.append({
            "customer_id": customer_id,
            "age": random.randint(18, 70),
            "gender": random.choice(["Male", "Female"]),
            "loyalty_member": random.choice(["Yes", "No"]),
            "product_type": product_type,
            "sku": f"{product_type[:3].upper()}-{random.randint(1000,9999)}",
            "rating": random.randint(1, 5),
            "order_status": random.choice(["Completed", "Cancelled"]),
            "payment_method": random.choice(["Cash", "Credit Card", "Paypal"]),
            "total_price": total_price,
            "unit_price": unit_price,
            "quantity": quantity,
            "purchase_date": purchase_date.strftime("%Y-%m-%d"),
            "shipping_type": random.choice(["Standard", "Overnight", "Express"]),
            "add_ons_purchased": random.choice(["None", "Accessories", "Extended Warranty"]),
            "add_on_total": round(random.uniform(0, 200), 2)
        })

    return pd.DataFrame(rows)


if __name__ == "__main__":
    # Read args from CLI (year, month)
    if len(sys.argv) != 3:
        print("Usage: python test_ingest_synthetic.py <year> <month>")
        sys.exit(1)

    year = int(sys.argv[1])
    month = int(sys.argv[2])

    print(f"📅 Generating synthetic sales data for {year}-{month:02d}...")

    # Generate new unseen monthly dataset
    df_new = generate_monthly_sales(year, month, n=200)

    # Match DB column order
    inspector = inspect(engine)
    db_columns = [col['name'] for col in inspector.get_columns("sales_2023_2024", schema="raw")]
    df_new = df_new[db_columns]

    # Append new rows
    df_new.to_sql(
        "sales_2023_2024",
        engine,
        schema="raw",
        if_exists="append",
        index=False
    )

    print(f"✅ Inserted {len(df_new)} rows into raw.sales_2023_2024 for {year}-{month:02d}")
