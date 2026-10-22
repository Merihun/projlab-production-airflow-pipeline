"""The replay data. Everything is derived from fixed seeds, so every copy of the project has the same data
and the files in ``golden/`` hold for all of them. Standard library only."""

from __future__ import annotations

import csv
import random
from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path

START = date(2026, 7, 1)
END = date(2026, 8, 31)
PAGE_SIZE = 100

# The planned faults. The tickets refer to them by date.
MALFORMED_FILE = "fulfilment_20260727_b.csv"  # DE-1822: exported with the wrong delimiter
DRIFT_DAY = date(2026, 8, 13)  # DE-1834: first customer extract with marketing_opt_in
INCIDENT_DAYS = (date(2026, 8, 16), date(2026, 8, 17), date(2026, 8, 18))  # DE-1841: loaded twice
INCIDENT_FIRST_DAY = date(2026, 8, 12)  # the export attached to DE-1841 starts here

ORDER_COLUMNS = ("order_id", "customer_id", "status", "total_amount", "currency", "created_at")
EVENT_COLUMNS = ("event_id", "order_id", "event_type", "carrier", "warehouse_id", "event_ts")
CUSTOMER_COLUMNS = ("customer_id", "email", "full_name", "tier", "country", "updated_at")
SNAPSHOT_COLUMNS = ("snapshot_date", "customer_id", "tier", "country", "loaded_at")

_FIRST_CUSTOMER = 10001
_BASE_CUSTOMERS = 2000
_SHOWCASE = 10482  # the customer in DE-1834's expected result: silver on 2 August, gold on 14 August
_TIERS = ("bronze", "silver", "gold")
_COUNTRIES = ("GB", "GB", "GB", "IE", "FR", "DE", "NL", "ES")
_STATUSES = ("placed", "placed", "paid", "paid", "paid", "shipped", "cancelled")
_CARRIERS = ("royal-mail", "dpd", "evri", "dhl")
_WAREHOUSES = {"a": "LHR1", "b": "MAN2", "c": "EDI3"}
_GIVEN = ("Ada", "Ben", "Chloe", "Dev", "Elin", "Farid", "Grace", "Hugo", "Isla", "Jamal", "Kira", "Leo")
_FAMILY = ("Abbott", "Bose", "Clarke", "Dlamini", "Evans", "Fraser", "Gupta", "Hart", "Ito", "Jones", "Khan")


def days(start: date = START, end: date = END) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def in_range(day: date) -> bool:
    return START <= day <= END


def _rng(kind: str, day: date | None = None) -> random.Random:
    return random.Random(f"fernhill-nightly-v1:{kind}:{day.isoformat() if day else ''}")


def _moment(rng: random.Random, day: date) -> datetime:
    return datetime.combine(day, time(), UTC) + timedelta(seconds=rng.randrange(86400))


# ---------------------------------------------------------------- customers


def _customer(customer_id: int, rng: random.Random, updated_at: datetime) -> dict[str, str]:
    given, family = rng.choice(_GIVEN), rng.choice(_FAMILY)
    return {
        "customer_id": str(customer_id),
        "email": f"{given}.{family}.{customer_id}@example.com".lower(),
        "full_name": f"{given} {family}",
        "tier": rng.choices(_TIERS, weights=(70, 22, 8))[0],
        "country": rng.choice(_COUNTRIES),
        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


def _opt_in(customer_id: int) -> str:
    return "true" if random.Random(f"fernhill-nightly-v1:opt-in:{customer_id}").random() < 0.4 else "false"


@lru_cache(maxsize=1)
def _customer_days() -> dict[date, tuple[list[dict[str, str]], dict[int, dict[str, str]]]]:
    """For every day: the rows of that day's extract, and every customer as they stood at the end of it."""
    out: dict[date, tuple[list[dict[str, str]], dict[int, dict[str, str]]]] = {}
    state: dict[int, dict[str, str]] = {}
    base = _rng("customers-base")
    for customer_id in range(_FIRST_CUSTOMER, _FIRST_CUSTOMER + _BASE_CUSTOMERS):
        state[customer_id] = _customer(customer_id, base, _moment(base, date(2026, 6, 1 + base.randrange(30))))
    state[_SHOWCASE]["tier"] = "bronze"
    out[START] = ([dict(row) for row in state.values()], {k: dict(v) for k, v in state.items()})
    next_id = _FIRST_CUSTOMER + _BASE_CUSTOMERS
    for day in days()[1:]:
        rng = _rng("customers", day)
        rows: dict[int, dict[str, str]] = {}
        existing = [c for c in state if c != _SHOWCASE]
        # A handful of rows are sent again unchanged: same values, same updated_at.
        for customer_id in rng.sample(existing, rng.randint(3, 6)):
            rows[customer_id] = dict(state[customer_id])
        for customer_id in rng.sample(existing, rng.randint(25, 45)):
            if customer_id in rows:
                continue
            row = dict(state[customer_id])
            field = rng.choice(("tier", "tier", "country", "email"))
            if field == "tier":
                row["tier"] = rng.choice([t for t in _TIERS if t != row["tier"]])
            elif field == "country":
                row["country"] = rng.choice([c for c in _COUNTRIES if c != row["country"]])
            else:
                name, _, domain = row["email"].partition("@")
                row["email"] = f"{name}.{day:%m%d}@{domain}"
            row["updated_at"] = _moment(rng, day).strftime("%Y-%m-%d %H:%M:%S")
            rows[customer_id] = state[customer_id] = row
        for _ in range(rng.randint(8, 20)):
            rows[next_id] = state[next_id] = _customer(next_id, rng, _moment(rng, day))
            next_id += 1
        for when, tier in ((datetime(2026, 8, 2, 9, 14, tzinfo=UTC), "silver"), (datetime(2026, 8, 14, 16, 2, tzinfo=UTC), "gold")):
            if when.date() == day:
                row = {**state[_SHOWCASE], "tier": tier, "updated_at": when.strftime("%Y-%m-%d %H:%M:%S")}
                rows[_SHOWCASE] = state[_SHOWCASE] = row
        out[day] = ([rows[k] for k in sorted(rows)], {k: dict(v) for k, v in state.items()})
    return out


def customers(day: date) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    """``(columns, rows)`` of the CRM extract for one day: the customers created or changed that day, plus a
    few sent again unchanged. The first day is the full customer base. From ``DRIFT_DAY`` the extract has one
    more column, ``marketing_opt_in``."""
    rows = [dict(row) for row in _customer_days()[day][0]]
    if day < DRIFT_DAY:
        return CUSTOMER_COLUMNS, rows
    for row in rows:
        row["marketing_opt_in"] = _opt_in(int(row["customer_id"]))
    return (*CUSTOMER_COLUMNS, "marketing_opt_in"), rows


def customers_as_of(day: date) -> dict[int, dict[str, str]]:
    """Every customer as they stood at the end of ``day``."""
    return _customer_days()[day][1]


# ---------------------------------------------------------------- orders


@lru_cache(maxsize=128)
def orders(day: date) -> list[dict[str, str]]:
    """The storefront's orders created on one day (UTC), oldest first."""
    rng = _rng("orders", day)
    known = sorted(customers_as_of(day))
    count = 230 + (60 if day.weekday() >= 5 else 0) + rng.randint(0, 90)
    moments = sorted(_moment(rng, day) for _ in range(count))
    return [
        {
            "order_id": f"FH-{day:%Y%m%d}-{i:04d}",
            "customer_id": str(rng.choice(known)),
            "status": rng.choice(_STATUSES),
            "total_amount": f"{rng.uniform(6, 240):.2f}",
            "currency": "GBP",
            "created_at": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for i, moment in enumerate(moments, start=1)
    ]


# ---------------------------------------------------------------- 3PL drops


def drop_names(day: date) -> list[str]:
    """The files the 3PL partner drops for one day: one per warehouse, and EDI3 is closed on Sundays."""
    letters = "ab" if day.weekday() == 6 else "abc"
    return [f"fulfilment_{day:%Y%m%d}_{letter}.csv" for letter in letters]


@lru_cache(maxsize=512)
def drop_rows(name: str) -> list[dict[str, str]]:
    """The fulfilment events in one drop file. They are about orders from the two days before."""
    stamp, letter = name.removesuffix(".csv").split("_")[1:]
    day = datetime.strptime(stamp, "%Y%m%d").replace(tzinfo=UTC).date()
    rng = _rng(f"drop-{letter}", day)
    recent = [o["order_id"] for back in (1, 2) if in_range(day - timedelta(days=back)) for o in orders(day - timedelta(days=back))]
    recent = recent or [o["order_id"] for o in orders(day)]
    return [
        {
            "event_id": f"EV-{stamp}-{letter.upper()}-{i:04d}",
            "order_id": rng.choice(recent),
            "event_type": rng.choice(("picked", "packed", "shipped", "delivered")),
            "carrier": rng.choice(_CARRIERS),
            "warehouse_id": _WAREHOUSES[letter],
            "event_ts": _moment(rng, day).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for i in range(1, rng.randint(140, 260) + 1)
    ]


# ---------------------------------------------------------------- DE-1841


def snapshot_rows() -> list[dict[str, str]]:
    """``wh.customer_snapshots_daily`` as it stood when DE-1841 was raised: one row per customer per day from
    12 to 18 August, and the three days the rerun on the morning of 19 August loaded a second time."""
    rows = []
    for day in days(INCIDENT_FIRST_DAY, INCIDENT_DAYS[-1]):
        nightly = datetime.combine(day + timedelta(days=1), time(0, 6, 41), UTC)
        loads = [nightly]
        if day in INCIDENT_DAYS:
            loads.append(datetime(2026, 8, 19, 8, 12 + INCIDENT_DAYS.index(day), 7, tzinfo=UTC))
        for loaded_at in loads:
            for customer_id, c in sorted(customers_as_of(day).items()):
                rows.append(
                    {
                        "snapshot_date": day.isoformat(),
                        "customer_id": str(customer_id),
                        "tier": c["tier"],
                        "country": c["country"],
                        "loaded_at": loaded_at.strftime("%Y-%m-%d %H:%M:%S+00"),
                    }
                )
    return rows


# ---------------------------------------------------------------- files


def _write(path: Path, columns: tuple[str, ...], rows: list[dict[str, str]], delimiter: str = ",") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, delimiter=delimiter, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def generate(root: Path) -> dict[str, int]:
    """Write the file sources under ``root``: ``3pl/``, ``customers/`` and ``incident/``. Orders are not files:
    they come from the API."""
    counts = {"3pl": 0, "customers": 0, "incident": 1}
    for day in days():
        for name in drop_names(day):
            # The partner's export job used the wrong delimiter for this one file, so it has a single column.
            _write(root / "3pl" / name, EVENT_COLUMNS, drop_rows(name), ";" if name == MALFORMED_FILE else ",")
            counts["3pl"] += 1
        columns, rows = customers(day)
        _write(root / "customers" / f"customers_{day:%Y%m%d}.csv", columns, rows)
        counts["customers"] += 1
    _write(root / "incident" / "customer_snapshots_daily.csv", SNAPSHOT_COLUMNS, snapshot_rows())
    return counts


def golden() -> dict[str, list[tuple[str, int]]]:
    """The expected results in ``golden/``, worked out from the data."""
    day27 = date(2026, 7, 27)
    return {
        "july_daily_counts.csv": [(d.isoformat(), len(orders(d))) for d in days(date(2026, 7, 1), date(2026, 7, 30))],
        "day27_files.csv": [(n, len(drop_rows(n))) for n in drop_names(day27) if n != MALFORMED_FILE],
    }


GOLDEN_HEADERS = {"july_daily_counts.csv": ("load_date", "n"), "day27_files.csv": ("source_file", "n")}
