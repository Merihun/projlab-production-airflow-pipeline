# Production Airflow Pipeline: what this starter gives you

This folder is your ProjLabs lab for the DE-1800 epic. It holds the world around the pipeline (the source
systems, the warehouse, the code you inherit and the evidence for the incident) and none of the pipeline:
the Airflow stack, the DAGs, the tests and the documents are the tickets. Everything is open source and runs
on your own computer.

| Provided | What it is |
|----------|------------|
| `docker-compose.yml` | The lab: `workspace` (Python 3.12, pytest, ruff, psycopg), `storefront` (the orders API) and `warehouse` (PostgreSQL 16). DE-1802 adds your Airflow services to this file. |
| `fernhill_source/` | The source systems replayed for 1 July to 31 August 2026. You do not change it. |
| `data/` | The 3PL drops, customer extracts and the DE-1841 table export. Generated when the lab starts; not in git. |
| `golden/` | The expected results DE-1814 and DE-1822 name. |
| `fixtures/broken_dags/` | Four unsafe DAGs your integrity tests must reject (DE-1803). |
| `fernhill/pipeline/`, `legacy/crontab.txt` | The code and schedule from the old VM, as you inherit them. |
| `incidents/DE-1841/` | The scheduler log and deploy diff attached to the incident. |
| `tests/test_environment.py` | Proves the lab is up. |
| `tests/harness/`, `dbt/` | Three provided tests and the dbt project (see the end of this file). |

## Where things are

Inside the `workspace` service these are set for you. Give your Airflow services the same values.

| Variable | Value |
|----------|-------|
| `STOREFRONT_API_URL` | `http://storefront:8000` |
| `WAREHOUSE_URL` | `postgresql://fernhill:fernhill@warehouse:5432/warehouse` |

Run `docker compose` on your own machine (or in the Codespace's terminal), not inside the workspace. Run
Python tools through the workspace: `docker compose exec workspace python -m pytest -q`.

The warehouse is PostgreSQL, a separate database server from the Airflow metadata database you add in
DE-1802. Several tasks write to it at the same time, which is why it is not a single-file database.

## The source systems

**Orders API** (`storefront`). `GET /v1/orders?date=2026-08-14&page=1` with `Authorization: Bearer <token>`
returns `{"date", "page", "pages", "total", "data": [...]}`, at most 100 orders a page. Each order has
`order_id`, `customer_id`, `status`, `total_amount`, `currency` and `created_at` (UTC). A date has only the
orders created on it, and asking twice gives the same answer.

- It serves two requests at a time. A third gets `429` with `Retry-After` (DE-1814).
- `GET /_lab/stats` shows the most requests it served at once and how many it refused: evidence for your
  backfill.
- `STOREFRONT_OUTAGE_DATES` in `.env` makes those dates answer `503`, to replay the outage (DE-1821).
- The token it accepts is the one hard-coded in `fernhill/pipeline/orders.py`, until you set
  `STOREFRONT_API_TOKEN` in `.env`. Then only your value works and the old one is refused: that is what
  rotating it means here (DE-1826). Your first commit puts the old token in your git history, as the ticket
  says.

**3PL drops** (`data/3pl/fulfilment_<yyyymmdd>_<a|b|c>.csv`). One file per warehouse per day; there is no
`c` file on Sundays. Columns: `event_id`, `order_id`, `event_type`, `carrier`, `warehouse_id`, `event_ts`.
Every row in these files is valid. One whole file is not: `fulfilment_20260727_b.csv` (DE-1822).

**Customer extracts** (`data/customers/customers_<yyyymmdd>.csv`). The first day is the full customer base.
Every later day holds the customers created or changed that day, and a few sent again unchanged. Columns:
`customer_id`, `email`, `full_name`, `tier`, `country`, `updated_at`; from 13 August also
`marketing_opt_in` (DE-1834). A customer appears at most once in a file.

    docker compose exec workspace python -m fernhill_source generate --force   put data/ back as it was
    docker compose exec workspace python -m fernhill_source orders 2026-08-14  how many orders a date has
    docker compose exec workspace python -m fernhill_source load-incident      DE-1841's table, into the warehouse

## The expected results

- `golden/july_daily_counts.csv`: orders per `load_date` for 1 to 30 July.
- `golden/day27_files.csv`: rows per `source_file` in `wh.fulfilment_events` for 27 July. The malformed file
  is not in it.

## Airflow 3: four things that differ from older tutorials

- The **DAG processor** is a service of its own, and tasks talk to Airflow through the **API server**
  (which is also the web UI; there is no separate webserver). Every Airflow service needs the API server's
  address and the same JWT secret.
- A plain cron string no longer gives a run "yesterday". Use `CronDataIntervalTimetable` so that the run for
  a day starts after that day, and take the day from the run's **logical date in London time**: in summer
  the logical date in UTC is still the evening before.
- Backfills are `airflow backfill create --dag-id ... --from-date ... --to-date ...` and run through the
  scheduler. The DAG must be unpaused, days that already ran are skipped unless you say otherwise, and
  `--to-date` has to reach the start time of the last day's run.
- Import from `airflow.sdk` (`dag`, `task`, `Connection`). Tasks cannot query the metadata database.

On Linux and in a Codespace, files in this folder belong to your user, and the Airflow image runs as its own
user (id 50000), so a task that moves a file out of `data/` or creates `dead_letter/` is refused. Run the
Airflow services as your user: `user: "${AIRFLOW_UID:-50000}:0"` and `HOME: /home/airflow` (so Python still
finds the image's packages) in the Compose file, and `AIRFLOW_UID=<output of id -u>` in `.env`. Docker Desktop on macOS and Windows
needs nothing.

The replay ends on 31 August 2026. When you unpause the DAG, Airflow also starts one scheduled run for
today's real date, which has nothing to load and fails: a free rehearsal of your retries and your alert.
Pause the DAG again when your backfill is done.

## Provided tests and tools

| Provided | Use |
|----------|-----|
| `tests/harness/test_stack.py` | DE-1802. Inside the running stack: `docker compose exec -T scheduler pytest -q tests/harness/test_stack.py`. |
| `tests/harness/test_alerting.py` | DE-1821. Calls your `on_failure_callback` as Airflow does after the last retry and stands in for the webhook. |
| `tests/harness/test_deadline.py` | DE-1825. Runs your `fernhill_sla_monitor` for a morning with no data and expects one alert. Needs the metadata database, so run it in the stack. |
| `dbt/` | The dbt project `dbt_build` runs: three models over the `wh` schema, built into `analytics`. `dbt/requirements.txt` pins dbt; install it in its own virtual environment in your image, not with the Airflow constraints. |
| `POST /_lab/alerts` on the storefront service | A stand-in for the on-call webhook. Point your `alert_webhook` connection at `http://storefront:8000/_lab/alerts`; `GET` shows what arrived, `DELETE` empties it. |

The harness tests import Airflow and your project, so they run in your Airflow image, not in the workspace.
Do not change `tests/harness/`. Your own tests need pytest in that image too.
