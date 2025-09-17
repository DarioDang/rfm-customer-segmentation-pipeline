from prefect import flow, task 
from prefect_dbt.cli.commands import DbtCoreOperation
import subprocess

# ---------- TASKS ----------
@task
def ingest():
    subprocess.run(["python", "01-ingest/ingest-data.py"], check=True)

@task
def dbt_run(select: str):
    result = DbtCoreOperation(
        commands=[f"dbt run --select {select}"],   # 👈 fix: one string
        project_dir="electronic_rfm_dbt"
    ).run()
    return result

@task
def train_and_logged_to_postgres():
    # Train model + save artifact + log to MLflow
    result_train = subprocess.run(
        ["python", "03-model/train_cluster_model.py"],
        check=True,
        capture_output=True,
        text=True
    )

# ---------- FLOW ----------
@flow(name="monthly_customer_segmentation")
def pipeline():
    # 1. Ingest
    ingest()
    
    # 2. DBT Preprocessing
    dbt_run("stg_sale_data")
    dbt_run("int_customer_rfm")
    dbt_run("mart_rfm")
    dbt_run("mart_rfm_features")
    dbt_run("modelling_features")
    
    # 3. Train Clustering Model
    train_and_logged_to_postgres()

    
    # 4. DBT Analysis
    dbt_run("customer_clustered_information")
    dbt_run("dim_customer_clustered")
    dbt_run("fact_cluster_summary")
    dbt_run("fact_customer_snapshot")


# ---------- ENTRY POINT ----------
if __name__ == "__main__":
    pipeline()
