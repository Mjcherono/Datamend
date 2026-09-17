"""Build the synthetic raw warehouse Datamend runs against.

Deterministic: the same seed always produces the same tables, so a chaos run can
be compared against a known-good baseline without re-deriving one each time.

The shape is a small telco-style order book: customers in four Irish regions,
orders over a trading year, and line items underneath them. It is deliberately
clean. Every defect in the demo is injected later by the chaos module, never
baked in here.
"""

from __future__ import annotations

import csv
import random
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb

DEFAULT_DB = Path("data/datamend.duckdb")
DEFAULT_SEED = 20261002

REGIONS = [
    ("IE-DUB", "Dublin", 0.38),
    ("IE-COR", "Cork", 0.22),
    ("IE-GAL", "Galway", 0.21),
    ("IE-LIM", "Limerick", 0.19),
]

PRODUCTS = [
    ("BB-500", "Broadband 500Mb", 49.99),
    ("BB-1G", "Broadband 1Gb", 69.99),
    ("MOB-SIM", "SIM Only", 19.99),
    ("MOB-BUN", "Mobile Bundle", 39.99),
    ("TV-BASE", "TV Base", 29.99),
    ("TV-SPRT", "TV Sport", 24.99),
    ("VOIP-BIZ", "Business Voice", 89.99),
]

START = date(2025, 10, 1)
DAYS = 365


@dataclass
class WarehouseSpec:
    """Size of the generated warehouse.

    Sized so gross revenue sits near EUR 12m. A join fan-out on a minority of
    customers then overstates revenue by a couple of million, which is a
    proportion a reviewer will find plausible.
    """

    customers: int = 20_000
    orders: int = 130_000
    seed: int = DEFAULT_SEED


def _customers(rng: random.Random, spec: WarehouseSpec) -> list[tuple]:
    codes = [r[0] for r in REGIONS]
    weights = [r[2] for r in REGIONS]
    rows = []
    for i in range(1, spec.customers + 1):
        signup = START - timedelta(days=rng.randint(0, 1_400))
        rows.append(
            (
                i,
                f"CUST{i:06d}",
                rng.choices(codes, weights=weights, k=1)[0],
                rng.choice(["consumer", "consumer", "consumer", "business"]),
                signup,
                "active" if rng.random() > 0.08 else "churned",
            )
        )
    return rows


def _orders(rng: random.Random, spec: WarehouseSpec) -> list[tuple]:
    rows = []
    for i in range(1, spec.orders + 1):
        placed = START + timedelta(days=rng.randint(0, DAYS - 1))
        rows.append(
            (
                i,
                f"ORD{i:07d}",
                rng.randint(1, spec.customers),
                placed,
                rng.choices(["complete", "complete", "complete", "cancelled"], k=1)[0],
                rng.choice(["web", "web", "retail", "callcentre"]),
            )
        )
    return rows


def _order_items(rng: random.Random, orders: list[tuple]) -> list[tuple]:
    rows = []
    item_id = 1
    for order in orders:
        for _ in range(rng.choices([1, 2, 3, 4], weights=[0.5, 0.3, 0.15, 0.05], k=1)[0]):
            sku, _name, price = rng.choice(PRODUCTS)
            qty = rng.choices([1, 1, 1, 2, 3], k=1)[0]
            rows.append((item_id, order[0], sku, qty, round(price, 2)))
            item_id += 1
    return rows


def _insert(con: duckdb.DuckDBPyConnection, table: str, rows: list[tuple], width: int) -> None:
    """Load ``rows`` into an existing table via DuckDB's CSV reader.

    Row-at-a-time INSERT costs minutes at this row count, and one huge multi-row
    VALUES just moves the cost into the parser. Staging through a CSV lets DuckDB
    load in parallel and cast to the table's declared types on the way in, which
    matters because the warehouse is rebuilt on every chaos reset.
    """
    if not rows:
        return
    if len({len(r) for r in rows}) != 1 or len(rows[0]) != width:
        raise ValueError(f"{table}: rows do not all have {width} columns")
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "load.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerows(rows)
        con.execute(
            f"COPY {table} FROM ? (FORMAT CSV, HEADER false, DATEFORMAT '%Y-%m-%d')",
            [str(path)],
        )


def build(db_path: Path = DEFAULT_DB, spec: WarehouseSpec | None = None) -> Path:
    """Create or replace the raw schema at ``db_path`` and return the path."""
    spec = spec or WarehouseSpec()
    rng = random.Random(spec.seed)

    customers = _customers(rng, spec)
    orders = _orders(rng, spec)
    items = _order_items(rng, orders)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("DROP TABLE IF EXISTS raw.regions")
        con.execute(
            "CREATE TABLE raw.regions (region_code VARCHAR, region_name VARCHAR, country VARCHAR)"
        )
        _insert(con, "raw.regions", [(c, n, "IE") for c, n, _w in REGIONS], 3)

        con.execute("DROP TABLE IF EXISTS raw.customers")
        con.execute(
            """
            CREATE TABLE raw.customers (
                customer_id BIGINT,
                customer_ref VARCHAR,
                region_code VARCHAR,
                segment VARCHAR,
                signup_date DATE,
                status VARCHAR
            )
            """
        )
        _insert(con, "raw.customers", customers, 6)

        con.execute("DROP TABLE IF EXISTS raw.orders")
        con.execute(
            """
            CREATE TABLE raw.orders (
                order_id BIGINT,
                order_ref VARCHAR,
                customer_id BIGINT,
                order_date DATE,
                status VARCHAR,
                channel VARCHAR
            )
            """
        )
        _insert(con, "raw.orders", orders, 6)

        con.execute("DROP TABLE IF EXISTS raw.order_items")
        con.execute(
            """
            CREATE TABLE raw.order_items (
                order_item_id BIGINT,
                order_id BIGINT,
                sku VARCHAR,
                quantity INTEGER,
                unit_price DECIMAL(10, 2)
            )
            """
        )
        _insert(con, "raw.order_items", items, 5)
    finally:
        con.close()
    return db_path


def summarise(db_path: Path = DEFAULT_DB) -> dict[str, object]:
    """Row counts and headline revenue, used to prove a rebuild is reproducible."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        out: dict[str, object] = {}
        for table in ("regions", "customers", "orders", "order_items"):
            out[table] = con.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
        out["gross_revenue"] = float(
            con.execute(
                """
                SELECT sum(i.quantity * i.unit_price)
                FROM raw.order_items i
                JOIN raw.orders o USING (order_id)
                WHERE o.status = 'complete'
                """
            ).fetchone()[0]
        )
        return out
    finally:
        con.close()


if __name__ == "__main__":
    path = build()
    for key, value in summarise(path).items():
        print(f"{key:>15}: {value:,.2f}" if isinstance(value, float) else f"{key:>15}: {value:,}")
