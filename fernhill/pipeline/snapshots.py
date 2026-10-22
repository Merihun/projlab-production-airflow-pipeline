"""Daily customer snapshot for finance: one row per customer per day in wh.customer_snapshots_daily.

Reads wh.customer_history (DE-1834). This is the version deployed on 18 August 2026; DE-1841 is about it.
"""

import os

import psycopg

SNAPSHOT_SQL = """
INSERT INTO wh.customer_snapshots_daily (snapshot_date, customer_id, tier, country, loaded_at)
SELECT %(ds)s::date, customer_id, tier, country, now()
FROM wh.customer_history
WHERE loaded_for <= %(ds)s::date
  AND (closed_for IS NULL OR closed_for > %(ds)s::date)
"""


def snapshot_customers(ds, conn=None):
    """Write the snapshot for ``ds``. Returns the number of rows written."""
    own = conn is None
    conn = conn or psycopg.connect(os.environ["WAREHOUSE_URL"], autocommit=True)
    try:
        written = conn.execute(SNAPSHOT_SQL, {"ds": ds}).rowcount
    finally:
        if own:
            conn.close()
    print(f"snapshot_customers: inserted {written} rows for {ds}")
    return written
