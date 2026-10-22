"""Broken on purpose (DE-1803): a network call at import time. The scheduler parses DAG files every few
seconds, so this hits the storefront API thousands of times a day and stops the DAG loading when it is down."""

import json
import os
import urllib.request
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

API_URL = os.environ.get("STOREFRONT_API_URL", "http://storefront:8000")

with urllib.request.urlopen(f"{API_URL}/health", timeout=5) as response:  # the top-level call
    API_STATUS = json.load(response)["status"]

with DAG(
    dag_id="broken_top_level_http_call",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
) as dag:

    @task
    def extract_orders_api() -> str:
        return API_STATUS

    extract_orders_api()
