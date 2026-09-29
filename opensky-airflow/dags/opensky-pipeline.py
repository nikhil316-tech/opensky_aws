from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
}


with DAG(
    dag_id="opensky_pipeline",
    default_args=default_args,
    description="OpenSky micro batch Bronze-Silver-Gold pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["opensky", "glue", "iceberg"],
) as dag:

    # ----------------------------------------
    # Bronze
    # ----------------------------------------

    bronze = GlueJobOperator(
        task_id="opensky_bronze",
        job_name="opensky_bronze",
        wait_for_completion=True,
    )

    # ----------------------------------------
    # Silver
    # ----------------------------------------

    silver = GlueJobOperator(
        task_id="opensky_silver_iceberg",
        job_name="opensky_silver_iceberg",
        wait_for_completion=True,
    )

    # ----------------------------------------
    # Gold
    # ----------------------------------------

    gold = GlueJobOperator(
        task_id="opensky_gold_scd2",
        job_name="opensky_gold_scd2",
        wait_for_completion=True,
    )

    # ----------------------------------------
    # Pipeline dependency
    # ----------------------------------------

    bronze >> silver >> gold