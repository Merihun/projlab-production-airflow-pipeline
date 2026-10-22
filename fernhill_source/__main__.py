"""python -m fernhill_source <command>

    generate [--force]     write the 3PL drops, customer extracts and the DE-1841 table export under data/
    serve [--port 8000]    run the storefront orders API (the lab starts this for you as the service storefront)
    orders 2026-08-14      how many orders the API has for a date
    golden [--check]       write (or compare) golden/ from the data
    load-incident          load data/incident/customer_snapshots_daily.csv into wh.customer_snapshots_daily
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
from datetime import date
from pathlib import Path

from . import data

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("FERNHILL_DATA_DIR", ROOT / "data"))


def generate(force: bool) -> int:
    if (DATA / "customers").is_dir() and not force:
        print(f"{DATA} already has the files. Use --force to write them again (this restores moved files).")
        return 0
    counts = data.generate(DATA)
    print(f"Wrote {counts['3pl']} 3PL files, {counts['customers']} customer extracts and the DE-1841 export to {DATA}")
    return 0


def golden(check: bool) -> int:
    wrong = 0
    for name, rows in data.golden().items():
        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(data.GOLDEN_HEADERS[name])
        writer.writerows(rows)
        path = ROOT / "golden" / name
        if not check:
            path.parent.mkdir(exist_ok=True)
            path.write_text(out.getvalue(), encoding="utf-8")
            print(f"wrote {path}")
        elif not path.is_file() or path.read_text(encoding="utf-8") != out.getvalue():
            print(f"{path} does not match the data")
            wrong += 1
    return 1 if wrong else 0


def load_incident() -> int:
    try:
        import psycopg
    except ImportError:
        print("This command needs psycopg. Run it inside the workspace: docker compose exec workspace ...")
        return 2
    source = DATA / "incident" / "customer_snapshots_daily.csv"
    if not source.is_file():
        print(f"{source} is missing. Run: python -m fernhill_source generate --force")
        return 2
    with psycopg.connect(os.environ["WAREHOUSE_URL"]) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS wh")
        cur.execute("DROP TABLE IF EXISTS wh.customer_snapshots_daily")
        cur.execute(
            "CREATE TABLE wh.customer_snapshots_daily (snapshot_date date NOT NULL, customer_id bigint NOT NULL, "
            "tier text, country text, loaded_at timestamptz NOT NULL)"
        )
        with (
            source.open(encoding="utf-8") as handle,
            cur.copy("COPY wh.customer_snapshots_daily FROM STDIN WITH (FORMAT csv, HEADER true)") as copy,
        ):
            for line in handle:
                copy.write(line)
        cur.execute("SELECT COUNT(*) FROM wh.customer_snapshots_daily")
        print(f"wh.customer_snapshots_daily: {cur.fetchone()[0]} rows, as it stood when DE-1841 was raised")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m fernhill_source", description=__doc__.split("\n\n")[1])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("generate")
    p.add_argument("--force", action="store_true")
    p = sub.add_parser("serve")
    p.add_argument("--port", type=int, default=8000)
    p = sub.add_parser("orders")
    p.add_argument("date", type=date.fromisoformat)
    p = sub.add_parser("golden")
    p.add_argument("--check", action="store_true")
    sub.add_parser("load-incident")
    args = parser.parse_args(argv)
    if args.command == "generate":
        return generate(args.force)
    if args.command == "serve":
        from .api import serve

        generate(force=False)
        serve(port=args.port)
        return 0
    if args.command == "orders":
        if not data.in_range(args.date):
            print(f"The replay covers {data.START} to {data.END}.")
            return 2
        print(len(data.orders(args.date)))
        return 0
    if args.command == "golden":
        return golden(args.check)
    return load_incident()


if __name__ == "__main__":
    sys.exit(main())
