from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner":            "saurabh",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="payflow_dwh_pipeline",
    default_args=default_args,
    description="PayFlow DWH — Bronze to Gold pipeline",
    schedule_interval="0 2 * * *",  # runs daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["payflow", "dwh", "delta"],
) as dag:

    #Task 1: Bronze Layer
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command="cd /opt/airflow && python src/ingestion/bronze_loader.py",
    )

    #Task 2: Silver Layer
    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command="cd /opt/airflow && python src/silver/silver_transform.py",
    )

    #Task 3: SCD1 Dimensions
    load_scd1 = BashOperator(
        task_id="load_scd1_dimensions",
        bash_command="cd /opt/airflow && python src/gold/dim_merchant_scd1.py",
    )

    #Task 4: SCD2 Dimensions
    load_scd2 = BashOperator(
        task_id="load_scd2_dimensions",
        bash_command="cd /opt/airflow && python src/gold/dim_customer_scd2.py",
    )

    #Task 5: Fact Table
    load_fact = BashOperator(
        task_id="load_fact_transaction",
        bash_command="cd /opt/airflow && python src/gold/fact_loader.py",
    )

    #Task 6: Analytics
    run_analytics = BashOperator(
        task_id="run_analytics",
        bash_command="cd /opt/airflow && python sql/analytics/business_queries.py",
    )