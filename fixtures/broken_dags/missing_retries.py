"""Broken on purpose (DE-1803): tasks that call external systems give up after one failure."""

import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="broken_missing_retries",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0},
) as dag:

    @task
    def extract_orders_api() -> None:
        """Calls the storefront API."""

    @task
    def ingest_3pl_drops() -> None:
        """Reads the partner's file share."""

    [extract_orders_api(), ingest_3pl_drops()]
