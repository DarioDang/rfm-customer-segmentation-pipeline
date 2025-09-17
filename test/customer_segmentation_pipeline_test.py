from prefect import flow, task 
from prefect_dbt.cli.commands import DbtCoreOperation
import subprocess

@task
def dbt_run(select: str):
    result = DbtCoreOperation(
        commands=[f"dbt run --select {select}"],
        project_dir="../electronic_rfm_dbt"
    ).run()
    return result

@task
def train_and_logged_to_postgres():
    subprocess.run(
        ["python", "../03-model/train_cluster_model.py"],
        check=True,
        capture_output=True,
        text=True
    )

@flow(name="monthly_customer_segmentation_test")
def pipeline_test():
    # No ingest() here, since synthetic script already inserts new rows
    
    dbt_run("stg_sale_data")
    dbt_run("int_customer_rfm")
    dbt_run("mart_rfm")
    dbt_run("mart_rfm_features")
    dbt_run("modelling_features")
    
    train_and_logged_to_postgres()

    dbt_run("customer_clustered_information")
    dbt_run("dim_customer_clustered")
    dbt_run("fact_cluster_summary")
    dbt_run("fact_customer_snapshot")


if __name__ == "__main__":
    pipeline_test()