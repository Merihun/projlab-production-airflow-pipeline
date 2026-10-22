"""Broken on purpose (DE-1803): the two tasks wait for each other, so neither can ever run."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="broken_cycle",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
) as dag:

    @task
    def load() -> None:
        """Loads the day's files."""

    @task
    def check() -> None:
        """Checks what was loaded."""

    loaded, checked = load(), check()
    loaded >> checked >> loaded
