"""Fixtures for the provided harness. Run these tests where Airflow is installed and your project is
importable: in your Airflow image (see PROJECT.md). Do not change this folder."""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar

import pytest


class _Webhook(BaseHTTPRequestHandler):
    received: ClassVar[list] = []

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0") or 0))
        try:
            body = json.loads(raw)
        except ValueError:
            body = raw.decode(errors="replace")
        type(self).received.append(body)
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


@pytest.fixture
def webhook(monkeypatch):
    """A stand-in for the alert webhook. The ``alert_webhook`` connection points at it for the test; the
    fixture is the list of JSON bodies it received."""
    _Webhook.received = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Webhook)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/alerts"
    monkeypatch.setenv("AIRFLOW_CONN_ALERT_WEBHOOK", json.dumps({"conn_type": "http", "host": url}))
    yield _Webhook.received
    server.shutdown()
    server.server_close()


@pytest.fixture(scope="session")
def nightly():
    """Your ``fernhill_nightly`` DAG, loaded from ``dags/`` (or ``DAGS_FOLDER``)."""
    from airflow.models import DagBag

    bag = DagBag(dag_folder=os.environ.get("DAGS_FOLDER", "dags"))
    assert bag.import_errors == {}, f"DAG files failed to import: {bag.import_errors}"
    assert "fernhill_nightly" in bag.dags, f"no DAG fernhill_nightly; found {sorted(bag.dags)}"
    return bag.dags["fernhill_nightly"]
