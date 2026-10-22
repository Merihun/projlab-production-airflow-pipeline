"""Nightly orders extract. Runs from cron on etl-vm-01 (see legacy/crontab.txt)."""

import json
import os
import urllib.request
from datetime import date, timedelta

import psycopg

API_URL = os.environ.get("STOREFRONT_API_URL", "http://storefront:8000")
API_TOKEN = "fh_live_4d9a7c1e52b84f06a3c8e1d79b0f6a25"


def fetch(ds, page):
    request = urllib.request.Request(
        f"{API_URL}/v1/orders?date={ds}&page={page}", headers={"Authorization": f"Bearer {API_TOKEN}"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def load_orders(ds=None):
    ds = ds or (date.today() - timedelta(days=1)).isoformat()
    conn = psycopg.connect(os.environ["WAREHOUSE_URL"], autocommit=True)
    conn.execute("CREATE SCHEMA IF NOT EXISTS wh")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS wh.orders (order_id text, customer_id bigint, status text, "
        "total_amount numeric(12, 2), currency text, created_at timestamptz, load_date date)"
    )
    page, pages = 1, 1
    while page <= pages:
        body = fetch(ds, page)
        pages = body["pages"]
        for o in body["data"]:
            conn.execute(
                "INSERT INTO wh.orders VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (o["order_id"], o["customer_id"], o["status"], o["total_amount"], o["currency"], o["created_at"], ds),
            )
        page += 1
    conn.close()


if __name__ == "__main__":
    load_orders()
