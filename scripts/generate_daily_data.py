#!/usr/bin/env python3
"""
Generates the raw daily input batch (orders + events) for one or more
logical dates, so the self_healing_pipeline DAG can be run for a new date.

    python scripts/generate_daily_data.py --date 2026-09-28
    python scripts/generate_daily_data.py --date tomorrow
    python scripts/generate_daily_data.py --start-date 2026-09-23 --end-date 2026-09-27

Writes exactly the files ingestion expects for logical date D:
    data/raw/orders/orders_D.csv
    data/raw/events/events_D.jsonl

Data-generation assumptions:
  - Rows come from the same generators as scripts/data/generate_data.py
    (same status/event-type weights, quantity 1-5, order_total = price * qty),
    with every timestamp inside day D (so the freshness check passes).
  - customers.csv / products.csv are static dimension tables and are NOT
    regenerated: every customer_id / product_id is drawn from the existing
    files, so foreign keys are always valid.
  - order_id / event_id are global primary keys in Postgres (loads upsert on
    them), so a reused ID would overwrite another date's row. Order IDs
    therefore continue the ORD sequence after the highest existing raw
    order_id; event IDs are UUIDs.
  - Deterministic: each date is seeded from (--seed, D). Regenerating the
    same dates in the same order on the same dataset reproduces identical
    files (order ID ranges depend on which batches already exist).
  - Existing batch files are never overwritten unless --force is given.
    With --force the replacement still takes new, globally unique order IDs.
    Staging is not touched: if data/staging/stg_orders_D.csv already exists,
    ingestion keeps using it (delete it to re-stage from the new raw file).

"today" / "tomorrow" are resolved in UTC, matching Airflow's timezone.
"""
import argparse
import csv
import os
import random
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
import generate_data as gen  # noqa: E402

DEFAULT_RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
ORDER_FIELDS = ["order_id", "customer_id", "product_id", "order_ts", "quantity", "order_total", "status"]


def parse_date(value: str) -> datetime:
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    if value == "today":
        return today
    if value == "tomorrow":
        return today + timedelta(days=1)
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date '{value}' (expected YYYY-MM-DD, 'today' or 'tomorrow')")


def _read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _max_order_id(orders_dir):
    highest = 0
    if not os.path.isdir(orders_dir):
        return highest
    for name in os.listdir(orders_dir):
        if not name.endswith(".csv"):
            continue
        for row in _read_csv(os.path.join(orders_dir, name)):
            m = re.fullmatch(r"ORD(\d+)", row["order_id"])
            if m:
                highest = max(highest, int(m.group(1)))
    return highest


def generate_for_date(raw_dir, day, orders_per_day=300, events_per_day=1200, seed=42, force=False):
    """Writes the orders/events batch for `day` under raw_dir. Returns {dataset: (path, rows_written or None)}."""
    customers = _read_csv(os.path.join(raw_dir, "customers.csv"))
    products = _read_csv(os.path.join(raw_dir, "products.csv"))
    for p in products:
        p["price"] = float(p["price"])

    day_str = day.strftime("%Y-%m-%d")
    orders_path = os.path.join(raw_dir, "orders", f"orders_{day_str}.csv")
    events_path = os.path.join(raw_dir, "events", f"events_{day_str}.jsonl")
    rng = random.Random(f"{seed}-{day_str}")
    # generate_data uses uuid4() for event IDs; seed it for reproducibility.
    gen.uuid.uuid4 = lambda: uuid.UUID(int=rng.getrandbits(128), version=4)

    result = {}
    if os.path.exists(orders_path) and not force:
        result["orders"] = (orders_path, None)
    else:
        next_id = _max_order_id(os.path.join(raw_dir, "orders")) + 1
        orders, _ = gen.gen_orders_for_day(day, next_id, customers, products, orders_per_day, rng)
        gen.write_csv(orders_path, orders, ORDER_FIELDS)
        result["orders"] = (orders_path, len(orders))

    if os.path.exists(events_path) and not force:
        result["events"] = (events_path, None)
    else:
        events = gen.gen_events_for_day(day, customers, events_per_day, rng)
        gen.write_jsonl(events_path, events)
        result["events"] = (events_path, len(events))
    return result


def main():
    ap = argparse.ArgumentParser(description="Generate raw daily orders/events batches for new logical dates.")
    ap.add_argument("--date", type=parse_date, help="YYYY-MM-DD, 'today' or 'tomorrow' (UTC)")
    ap.add_argument("--start-date", type=parse_date)
    ap.add_argument("--end-date", type=parse_date)
    ap.add_argument("--raw", default=DEFAULT_RAW_DIR, help="raw data directory (default: data/raw)")
    ap.add_argument("--orders-per-day", type=int, default=300)
    ap.add_argument("--events-per-day", type=int, default=1200)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--force", action="store_true", help="overwrite batch files that already exist for the date(s)")
    args = ap.parse_args()

    if args.date and (args.start_date or args.end_date):
        ap.error("use either --date or --start-date/--end-date, not both")
    if args.date:
        start = end = args.date
    elif args.start_date and args.end_date:
        start, end = args.start_date, args.end_date
    else:
        ap.error("--date or both --start-date and --end-date are required")

    day = start
    while day <= end:
        day_str = day.strftime("%Y-%m-%d")
        for dataset, (path, rows) in generate_for_date(
            args.raw, day, args.orders_per_day, args.events_per_day, args.seed, args.force
        ).items():
            if rows is None:
                print(f"[{day_str}] {dataset}: {path} already exists - left untouched")
            else:
                print(f"[{day_str}] {dataset}: wrote {rows} rows to {path}")
        day += timedelta(days=1)


if __name__ == "__main__":
    main()
