"""DE-1825: when the data is not ready by 05:00 London, an alert goes out through the normal webhook.

The test runs your ``fernhill_sla_monitor`` DAG for the morning of 2 September 2026. The replay ends on
31 August, so the nightly run for 1 September can never have finished: the monitor must alert, however it
decides that the data is late. It needs the Airflow metadata database, so run it in the stack:

    docker compose run --rm -T <your Airflow service> pytest -q tests/harness/test_deadline.py
"""

import os
import subprocess

MORNING = "2026-09-02T05:00:00+01:00"


def test_monitor_dag_runs_at_five_london_time(nightly):
    from airflow.models import DagBag

    bag = DagBag(dag_folder=os.environ.get("DAGS_FOLDER", "dags"))
    assert "fernhill_sla_monitor" in bag.dags, f"no DAG fernhill_sla_monitor; found {sorted(bag.dags)}"
    monitor = bag.dags["fernhill_sla_monitor"]
    assert monitor.timezone.name == "Europe/London"
    timetable = monitor.timetable
    schedule = " ".join(str(getattr(timetable, name, "")) for name in ("expression", "_expression", "summary")) + repr(timetable)
    assert "0 5 * * *" in schedule, f"the monitor's schedule is {schedule}, not the cron expression 0 5 * * *"


def test_late_data_alerts_through_the_webhook(webhook):
    done = subprocess.run(
        ["airflow", "dags", "test", "fernhill_sla_monitor", MORNING],
        capture_output=True, text=True, timeout=300, check=False, env=dict(os.environ),
    )  # fmt: skip
    assert done.returncode == 0, f"the monitor run itself failed:\n{(done.stdout + done.stderr)[-3000:]}"
    assert len(webhook) == 1, f"expected exactly 1 alert for a morning with no data, got {len(webhook)}: {webhook}"
