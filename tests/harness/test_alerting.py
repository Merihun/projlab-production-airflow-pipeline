"""DE-1821: when an external task has failed for the last time, one alert goes out with the four fields.

The test does what Airflow does after the last retry of ``extract_orders_api`` on the day of the outage: it
calls the task's ``on_failure_callback`` with the run's context. Your callback must log
``PIPELINE_TASK_FAILED`` and post to the webhook of the ``alert_webhook`` connection.

    pytest -q tests/harness/test_alerting.py        (in your Airflow image; part of your tests job)
"""

import json
import logging
from types import SimpleNamespace

import pytest

EXTERNAL = ("extract_orders_api", "ingest_3pl_drops", "customers_incremental")
RUN_ID = "scheduled__2026-08-12T00:00:00+00:00"
FIELDS = {
    "task_id": "extract_orders_api",
    "run_id": RUN_ID,
    "try_number": 4,
    "log_url": "http://localhost:8080/dags/fernhill_nightly/runs/" + RUN_ID + "/tasks/extract_orders_api?try_number=4",
}


def callbacks(task, name: str) -> list:
    value = getattr(task, name, None)
    if value is None:
        return []
    return list(value) if isinstance(value, (list, tuple)) else [value]


@pytest.fixture
def failed_context(nightly):
    """What Airflow hands a failure callback, for the fourth and last try of the extract on 12 August."""
    task = nightly.get_task("extract_orders_api")
    ti = SimpleNamespace(dag_id="fernhill_nightly", task=task, map_index=-1, state="failed", **FIELDS)
    dag_run = SimpleNamespace(dag_id="fernhill_nightly", run_id=RUN_ID)
    return {
        "ti": ti,
        "task_instance": ti,
        "task": task,
        "dag": nightly,
        "dag_run": dag_run,
        "run_id": RUN_ID,
        "ds": "2026-08-12",
        "exception": RuntimeError("503 Server Error: storefront API unavailable"),
    }


def test_external_tasks_alert_on_failure_and_not_on_retry(nightly):
    for task_id in EXTERNAL:
        task = nightly.get_task(task_id)
        assert callbacks(task, "on_failure_callback"), f"{task_id} has no on_failure_callback"
        assert not callbacks(task, "on_retry_callback"), f"{task_id} would alert on every retry, not once at the end"


def test_one_alert_reaches_the_webhook_with_the_four_fields(nightly, failed_context, webhook):
    for callback in callbacks(nightly.get_task("extract_orders_api"), "on_failure_callback"):
        callback(failed_context)
    assert len(webhook) == 1, f"expected exactly 1 request to the webhook, got {len(webhook)}"
    body = webhook[0]
    assert isinstance(body, dict), f"the alert must be a JSON object, got {body!r}"
    assert {k: body.get(k) for k in FIELDS} == FIELDS


def test_structured_log_line(nightly, failed_context, webhook, caplog):
    with caplog.at_level(logging.INFO):
        for callback in callbacks(nightly.get_task("extract_orders_api"), "on_failure_callback"):
            callback(failed_context)
    lines = [r.getMessage() for r in caplog.records if "PIPELINE_TASK_FAILED" in r.getMessage()]
    assert len(lines) == 1, f"expected one PIPELINE_TASK_FAILED log line, got {len(lines)}"
    logged = json.loads(lines[0][lines[0].index("{") :])
    assert {k: logged.get(k) for k in FIELDS} == FIELDS
