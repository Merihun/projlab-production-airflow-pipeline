# Broken DAGs (DE-1803)

Four DAG files, each unsafe in exactly one way. Your DAG integrity tests must fail on every one of them:

    DAGS_FOLDER=fixtures/broken_dags pytest tests/test_dag_integrity.py -q

| File | What is wrong with it |
|------|-----------------------|
| `top_level_http_call.py` | Calls the storefront API while the file is being imported. |
| `missing_retries.py` | Its tasks talk to external systems and have `retries=0`. |
| `cycle.py` | `load >> check >> load`. |
| `implicit_catchup.py` | Never says `catchup=`, so it inherits whatever the installation's default is. |

Do not edit or delete them, and do not put this folder where the scheduler looks for DAGs.
