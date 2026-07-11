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
    description="OpenSky micro batch pipeline",
    schedule=None,   # every minute
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["opensky", "glue", "iceberg"],
) as dag:


    silver = GlueJobOperator(
        task_id="opensky_silver_iceberg",
        job_name="opensky_silver_iceberg",
        wait_for_completion=True,
    )

    dim_country = GlueJobOperator(
        task_id="opensky_gold_dim_country",
        job_name="opensky_gold_dim_country",
        wait_for_completion=True,
    )

    dim_aircraft = GlueJobOperator(
        task_id="opensky_gold_dim_aircraft",
        job_name="opensky_gold_dim_aircraft",
        wait_for_completion=True,
    )

    dim_flight = GlueJobOperator(
        task_id="opensky_gold_dim_flight",
        job_name="opensky_gold_dim_flight",
        wait_for_completion=True,
    )

    fact = GlueJobOperator(
        task_id="opensky_gold_fact_aircraft",
        job_name="opensky_gold_fact_aircraft",
        wait_for_completion=True,
    )

    silver >> dim_country >> dim_aircraft >> dim_flight >> fact