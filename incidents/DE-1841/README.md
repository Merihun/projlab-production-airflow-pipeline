# DE-1841: what is attached

| File | What it is |
|------|------------|
| `scheduler-2026-08-19.log` | The scheduler log for 19 August 2026, trimmed to `fernhill_nightly`. Times are UTC. |
| `deploy.diff` | The change deployed on the afternoon of 18 August. `fernhill/pipeline/snapshots.py` in this repository is the code after that change. |
| `data/incident/customer_snapshots_daily.csv` | `wh.customer_snapshots_daily` as it stood when the incident was raised (12 to 18 August). It is generated with the rest of the practice data. |

Load the table into the warehouse to investigate it and to try your cleanup:

    docker compose exec workspace python -m fernhill_source load-incident

That drops and recreates `wh.customer_snapshots_daily`, so you can run it again whenever you want the
incident's state back.

`snapshot_customers` is a task that was added to `fernhill_nightly` after `customers_incremental`. It reads
`wh.customer_history`, which you build in DE-1834.
