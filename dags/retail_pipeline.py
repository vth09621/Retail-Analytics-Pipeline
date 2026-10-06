from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/Users/avinashsingh/retail-data-engineering"
PYTHON = f"{PROJECT_DIR}/.venv/bin/python"
DBT = f"{PROJECT_DIR}/.venv/bin/dbt"


default_args = {
    "owner": "retail-data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="retail_pipeline",
    description="Retail data engineering pipeline: S3 → PySpark → DuckDB → dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["retail", "pyspark", "duckdb", "dbt"],
) as dag:

    raw_to_bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} pyspark/ingest_retail.py"
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} pyspark/bronze_to_silver.py"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} pyspark/silver_to_gold.py"
        ),
    )

    gold_to_duckdb = BashOperator(
        task_id="gold_to_duckdb",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} warehouse/load_gold.py"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT_DIR}/retail_dbt && "
            f"{DBT} build"
        ),
    )


    raw_to_bronze >> bronze_to_silver >> silver_to_gold >> gold_to_duckdb >> dbt_build
