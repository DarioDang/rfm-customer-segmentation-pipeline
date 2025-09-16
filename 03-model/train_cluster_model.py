# train_cluster_model.py
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans


# DB connection
DB_CONFIG = {
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432,
    "database": "electronic_rfm"
}
engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# Load modelling features
model_name = "public_model.modelling_features"
print(f"Loading {model_name} from Postgres...")
model_df = pd.read_sql(f"SELECT * FROM {model_name}", engine)

X = model_df.drop(columns="customer_id")
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)


# KMeans Training
k = 3
kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto")
clusters = kmeans.fit_predict(X_scaled)
model_df["cluster"] = clusters
customer_clusters = model_df[["customer_id", "cluster"]]

# SAVE TO POSTGRES
schema_name = "public_model"
table_name = "customer_clusters"

with engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
    # Truncate if table already exists
    table_exists = conn.execute(text(f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            AND table_name = '{table_name}'
        )
    """)).scalar()
    if table_exists:
        conn.execute(text(f"TRUNCATE TABLE {schema_name}.{table_name}"))

customer_clusters.to_sql(
    table_name,
    engine,
    schema=schema_name,
    if_exists="append",
    index=False
)

print(f"Saved {len(customer_clusters)} rows into {schema_name}.{table_name}")