"""The storefront orders API, replayed. Standard library only.

    GET /health                                   no token needed
    GET /v1/orders?date=2026-08-14&page=1         Authorization: Bearer <token>
    GET /_lab/stats                               how hard the API was hit (for your own evidence)
    POST /_lab/alerts                             a stand-in for the on-call alert webhook: keeps what it is sent
    GET /_lab/alerts                              what was sent, oldest first; DELETE empties it

It behaves like the real one in the ways the tickets care about:

* a page holds at most 100 orders, and the answer says how many pages the date has;
* it serves two requests at a time. A third one arriving while two are being served gets 429 with a
  ``Retry-After`` header (DE-1814);
* the dates in ``STOREFRONT_OUTAGE_DATES`` answer 503, to replay the outage of 12 August (DE-1821);
* the token is the one in ``STOREFRONT_API_TOKEN``. Until you set that, the old token in the legacy code
  still works; once you set it, the old one is refused (DE-1826).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import time
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar
from urllib.parse import parse_qs, urlparse

from . import data

MAX_IN_FLIGHT = 2
SERVICE_TIME = 0.05  # seconds a request takes, so that concurrent callers really overlap
# SHA-256 of the value the legacy code carries, so that it is not repeated here (a secret scan must come back clean
# once you have removed it from the legacy code).
_LEGACY_SHA256 = "e37a2aa23e8d95b9e3c1fd5f5f1bcf6074ee4a64da08cd633092ef05cc16da19"


class Stats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.in_flight = 0
        self.max_in_flight = 0
        self.served = 0
        self.rejected_429 = 0
        self.rejected_401 = 0
        self.outage_503 = 0

    def as_dict(self) -> dict[str, int]:
        with self.lock:
            return {k: v for k, v in vars(self).items() if isinstance(v, int)}


def token_ok(presented: str) -> bool:
    configured = os.environ.get("STOREFRONT_API_TOKEN", "")
    if configured:
        return hmac.compare_digest(presented, configured)
    return hmac.compare_digest(hashlib.sha256(presented.encode()).hexdigest(), _LEGACY_SHA256)


def outage_dates() -> set[str]:
    return {d.strip() for d in os.environ.get("STOREFRONT_OUTAGE_DATES", "").split(",") if d.strip()}


def orders_page(day: date, page: int) -> dict[str, object]:
    rows = data.orders(day)
    pages = -(-len(rows) // data.PAGE_SIZE)
    return {
        "date": day.isoformat(),
        "page": page,
        "pages": pages,
        "total": len(rows),
        "data": rows[(page - 1) * data.PAGE_SIZE : page * data.PAGE_SIZE],
    }


class Handler(BaseHTTPRequestHandler):
    stats = Stats()
    alerts: ClassVar[list[object]] = []
    server_version = "FernhillStorefront/1"

    def log_message(self, fmt: str, *args: object) -> None:  # one short line per request
        print(f"{self.log_date_time_string()} {fmt % args}", flush=True)

    def _send(self, status: int, body: dict[str, object], headers: dict[str, str] | None = None) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/health":
            return self._send(200, {"status": "ok"})
        if url.path == "/_lab/stats":
            return self._send(200, dict(self.stats.as_dict()))
        if url.path == "/_lab/alerts":
            return self._send(200, {"alerts": list(self.alerts)})
        if url.path != "/v1/orders":
            return self._send(404, {"error": "not found"})
        scheme, _, presented = self.headers.get("Authorization", "").partition(" ")
        if scheme != "Bearer" or not token_ok(presented.strip()):
            with self.stats.lock:
                self.stats.rejected_401 += 1
            return self._send(401, {"error": "missing, wrong or revoked token"})
        query = parse_qs(url.query)
        try:
            day = date.fromisoformat(query.get("date", [""])[0])
            page = int(query.get("page", ["1"])[0])
        except ValueError:
            return self._send(400, {"error": "date must be YYYY-MM-DD and page a number"})
        if not data.in_range(day):
            return self._send(404, {"error": f"no orders for {day}: the replay covers {data.START} to {data.END}"})
        if page < 1:
            return self._send(400, {"error": "page starts at 1"})
        if day.isoformat() in outage_dates():
            with self.stats.lock:
                self.stats.outage_503 += 1
            return self._send(503, {"error": "storefront API unavailable"})
        with self.stats.lock:
            if self.stats.in_flight >= MAX_IN_FLIGHT:
                self.stats.rejected_429 += 1
                busy = True
            else:
                busy = False
                self.stats.in_flight += 1
                self.stats.max_in_flight = max(self.stats.max_in_flight, self.stats.in_flight)
        if busy:
            return self._send(429, {"error": "too many concurrent requests (limit 2)"}, {"Retry-After": "1"})
        try:
            time.sleep(SERVICE_TIME)
            body = orders_page(day, page)
        finally:
            with self.stats.lock:
                self.stats.in_flight -= 1
                self.stats.served += 1
        return self._send(200, body)


    def do_POST(self) -> None:
        if urlparse(self.path).path != "/_lab/alerts":
            return self._send(404, {"error": "not found"})
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0") or 0))
        try:
            self.alerts.append(json.loads(raw or b"null"))
        except ValueError:
            return self._send(400, {"error": "the alert body must be JSON"})
        return self._send(202, {"received": len(self.alerts)})

    def do_DELETE(self) -> None:
        if urlparse(self.path).path != "/_lab/alerts":
            return self._send(404, {"error": "not found"})
        self.alerts.clear()
        return self._send(200, {"alerts": []})


def serve(host: str = "0.0.0.0", port: int = 8000) -> None:  # reachable from the other containers
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Storefront API replay on http://{host}:{port} ({data.START} to {data.END})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
