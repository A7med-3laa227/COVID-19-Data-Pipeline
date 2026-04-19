"""
COVID-19 ETL Pipeline DAG
Runs the full Extract → Transform → Load pipeline once per day.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os

# Make the pipeline module importable inside the container
sys.path.insert(0, "/opt/airflow/dags/pipeline")

# ── Import the ETL functions ──────────────────────────────────────────────────
from explore_data import extraction, transformation, load
from validate_data import validate


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_extraction(**context):
    df = extraction()
    if df is None:
        raise ValueError("Extraction returned None — check the source URL.")
    # Push to XCom so downstream tasks can access it
    context["ti"].xcom_push(key="raw_df", value=df.to_json())


def run_transformation(**context):
    import pandas as pd
    raw_json = context["ti"].xcom_pull(key="raw_df", task_ids="extract")
    df = pd.read_json(raw_json)
    # Restore datetime type lost during JSON serialisation
    df["Last_Update"] = pd.to_datetime(df["Last_Update"], unit="ms")
    dim_date, dim_location, fact = transformation(df)
    context["ti"].xcom_push(key="dim_date",    value=dim_date.to_json())
    context["ti"].xcom_push(key="dim_location", value=dim_location.to_json())
    context["ti"].xcom_push(key="fact",         value=fact.to_json())


def run_validation(**context):
    """Run data completeness & schema checks before loading."""
    import pandas as pd
    ti = context["ti"]
    dim_date     = pd.read_json(ti.xcom_pull(key="dim_date",     task_ids="transform"))
    dim_location = pd.read_json(ti.xcom_pull(key="dim_location", task_ids="transform"))
    fact         = pd.read_json(ti.xcom_pull(key="fact",         task_ids="transform"))

    # Restore full_date as a DATE string (not epoch int)
    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"], unit="ms").dt.strftime("%Y-%m-%d")

    # Raises ValueError on any check failure → Airflow marks task as failed
    validate(dim_date, dim_location, fact)


def run_load(**context):
    import pandas as pd
    ti = context["ti"]
    dim_date    = pd.read_json(ti.xcom_pull(key="dim_date",    task_ids="transform"))
    dim_location = pd.read_json(ti.xcom_pull(key="dim_location", task_ids="transform"))
    fact        = pd.read_json(ti.xcom_pull(key="fact",        task_ids="transform"))

    # Restore full_date as a DATE string (not epoch int)
    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"], unit="ms").dt.strftime("%Y-%m-%d")

    load(dim_date, dim_location, fact)


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="covid_etl_pipeline",
    description="Download, transform and load daily COVID-19 data into PostgreSQL",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["covid", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extraction,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transformation,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=run_validation,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    # Pipeline order: Extract → Transform → Validate → Load
    extract_task >> transform_task >> validate_task >> load_task
