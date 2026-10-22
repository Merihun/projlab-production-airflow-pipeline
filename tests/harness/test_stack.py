"""DE-1802: is the stack up? Run it inside the running stack, from your integration job:

    docker compose exec -T scheduler pytest -q tests/harness/test_stack.py
"""

import json
import subprocess


def airflow(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["airflow", *args], capture_output=True, text=True, timeout=180, check=False)


def test_scheduler_is_alive():
    done = airflow("jobs", "check", "--job-type", "SchedulerJob")
    assert done.returncode == 0, f"no live scheduler in the metadata database:\n{done.stdout}{done.stderr}"


def test_dag_processor_is_alive():
    done = airflow("jobs", "check", "--job-type", "DagProcessorJob")
    assert done.returncode == 0, f"no live DAG processor: in Airflow 3 it is a service of its own:\n{done.stdout}{done.stderr}"


def json_output(text: str) -> list:
    """The JSON the command printed, without the log lines Airflow may print around it."""
    for line in reversed(text.splitlines()):
        if line.startswith("["):
            try:
                return json.loads(line)
            except ValueError:
                continue
    raise AssertionError(f"no JSON in the command's output:\n{text[-2000:]}")


def test_dags_load_without_import_errors():
    errors = json_output(airflow("dags", "list-import-errors", "--output", "json").stdout)
    assert errors == [], f"DAG import errors: {errors}"
    listed = airflow("dags", "list", "--output", "json")
    assert listed.returncode == 0, listed.stderr
    assert json_output(listed.stdout), "the stack has no DAGs: is your dags folder mounted or copied in?"
