"""Broken on purpose (DE-1803): no explicit catchup. Whether switching this DAG on runs every day since
start_date depends on a default that differs between Airflow versions and installations."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="broken_implicit_catchup",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
) as dag:

    @task
    def extract_orders_api() -> None:
        """Calls the storefront API."""

    extract_orders_api()
