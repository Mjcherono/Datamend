"""Build the insurance warehouse the erasure demo runs against.

Shaped like a small Irish general insurer: policyholders, the policies they
hold and the claims made against them. Deterministic, so a subject erased in one
run is the same subject in the next.

The important structural choice is the landing zone. `landing.*` is an
append-only record of what arrived from the source systems, and `raw.*` is the
operational copy the warehouse serves from. Erasure hits the operational copy.
The landing zone is what brings people back, exactly as it does in production
when nobody wires deletion through to the place the data actually came from.
"""

from __future__ import annotations

import csv
import random
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb

DEFAULT_DB = Path("data/insurance.duckdb")
DEFAULT_SEED = 20261002

FIRST_NAMES = [
    "Aoife", "Cian", "Saoirse", "Oisin", "Niamh", "Fionn", "Roisin", "Cillian",
    "Sinead", "Darragh", "Maeve", "Eoin", "Clodagh", "Tadhg", "Orla", "Ruairi",
    "Caoimhe", "Padraig", "Aisling", "Diarmuid", "Grainne", "Lorcan",
]
LAST_NAMES = [
    "Byrne", "Murphy", "Kelly", "OSullivan", "Walsh", "Smith", "OBrien", "Ryan",
    "OConnor", "ONeill", "Reilly", "Doyle", "McCarthy", "Gallagher", "Doherty",
    "Kennedy", "Lynch", "Murray", "Quinn", "Moore",
]
COUNTIES = [
    ("Dublin", "D", 0.31), ("Cork", "T12", 0.14), ("Galway", "H91", 0.09),
    ("Limerick", "V94", 0.07), ("Kildare", "W91", 0.07), ("Meath", "C15", 0.06),
    ("Wicklow", "A98", 0.05), ("Donegal", "F92", 0.05), ("Kerry", "V92", 0.05),
    ("Mayo", "F23", 0.05), ("Wexford", "Y35", 0.06),
]
PRODUCTS = [
    ("MOTOR-CMP", "Motor Comprehensive", 780.0),
    ("MOTOR-TPFT", "Motor Third Party Fire and Theft", 540.0),
    ("HOME-BLD", "Home Buildings", 320.0),
    ("HOME-CNT", "Home Contents", 210.0),
    ("TRAVEL-ANN", "Travel Annual", 95.0),
    ("HEALTH-STD", "Health Standard", 1450.0),
]
CLAIM_TYPES = ["collision", "theft", "storm", "water damage", "medical", "windscreen"]

TODAY = date(2026, 10, 1)


@dataclass
class WarehouseSpec:
    """Size of the generated book.

    Sized so a single erased policyholder is a needle worth finding rather than
    an obvious outlier, while still building in a few seconds.
    """

    policyholders: int = 25_000
    seed: int = DEFAULT_SEED


def _load(con: duckdb.DuckDBPyConnection, table: str, rows: list[tuple], width: int) -> None:
    """Bulk load via DuckDB's CSV reader.

    Row-at-a-time INSERT costs minutes at this size and one huge VALUES just
    moves the cost into the parser.
    """
    if not rows:
        return
    if len(rows[0]) != width:
        raise ValueError(f"{table}: expected {width} columns, got {len(rows[0])}")
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "load.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerows(rows)
        con.execute(
            f"COPY {table} FROM ? (FORMAT CSV, HEADER false, DATEFORMAT '%Y-%m-%d')",
            [str(path)],
        )


def _people(rng: random.Random, spec: WarehouseSpec) -> list[tuple]:
    counties = [c[0] for c in COUNTIES]
    weights = [c[2] for c in COUNTIES]
    prefixes = {c[0]: c[1] for c in COUNTIES}
    rows = []
    for i in range(1, spec.policyholders + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        county = rng.choices(counties, weights=weights, k=1)[0]
        born = date(rng.randint(1945, 2006), rng.randint(1, 12), rng.randint(1, 28))
        rows.append(
            (
                i,
                f"PH{i:07d}",
                first,
                last,
                f"{first.lower()}.{last.lower()}{i}@example.ie",
                born,
                f"{rng.randint(1, 180)} {rng.choice(['Main St','Church Rd','The Grove','Park Ave'])}",
                county,
                f"{prefixes[county]}{rng.randint(10, 99)} {rng.choice('ABCDEFHKNPRTVWXY')}{rng.randint(100, 999)}",
                f"08{rng.randint(3, 9)}{rng.randint(1000000, 9999999)}",
            )
        )
    return rows


def _policies(rng: random.Random, people: list[tuple]) -> list[tuple]:
    rows, policy_id = [], 1
    for person in people:
        for _ in range(rng.choices([1, 1, 2, 3], k=1)[0]):
            code, _name, base = rng.choice(PRODUCTS)
            start = TODAY - timedelta(days=rng.randint(30, 2_900))
            years = rng.randint(1, 6)
            end = start + timedelta(days=365 * years)
            # A lapsed policy is the retention trigger: nobody should still be
            # holding this person's data years after cover ended.
            status = "active" if end > TODAY else "lapsed"
            rows.append(
                (
                    policy_id,
                    f"POL{policy_id:08d}",
                    person[0],
                    code,
                    start,
                    end,
                    status,
                    round(base * rng.uniform(0.75, 1.45), 2),
                )
            )
            policy_id += 1
    return rows


def _claims(rng: random.Random, policies: list[tuple]) -> list[tuple]:
    rows, claim_id = [], 1
    for policy in policies:
        if rng.random() > 0.22:
            continue
        for _ in range(rng.choices([1, 1, 2], k=1)[0]):
            start, end = policy[4], policy[5]
            span = max((min(end, TODAY) - start).days, 1)
            rows.append(
                (
                    claim_id,
                    f"CLM{claim_id:08d}",
                    policy[0],
                    start + timedelta(days=rng.randint(0, span)),
                    rng.choice(CLAIM_TYPES),
                    round(rng.lognormvariate(7.0, 0.9), 2),
                    rng.choices(["settled", "settled", "settled", "repudiated", "open"], k=1)[0],
                )
            )
            claim_id += 1
    return rows


SCHEMA = {
    "policyholders": """
        person_id BIGINT, person_ref VARCHAR, first_name VARCHAR, last_name VARCHAR,
        email VARCHAR, date_of_birth DATE, address_line VARCHAR, county VARCHAR,
        eircode VARCHAR, phone VARCHAR
    """,
    "policies": """
        policy_id BIGINT, policy_ref VARCHAR, person_id BIGINT, product_code VARCHAR,
        cover_start DATE, cover_end DATE, status VARCHAR, annual_premium DECIMAL(10, 2)
    """,
    "claims": """
        claim_id BIGINT, claim_ref VARCHAR, policy_id BIGINT, claim_date DATE,
        claim_type VARCHAR, amount DECIMAL(12, 2), status VARCHAR
    """,
}


def build(db_path: Path = DEFAULT_DB, spec: WarehouseSpec | None = None) -> Path:
    """Create the landing zone and the operational copy it feeds."""
    spec = spec or WarehouseSpec()
    rng = random.Random(spec.seed)
    people = _people(rng, spec)
    policies = _policies(rng, people)
    claims = _claims(rng, policies)
    payloads = {"policyholders": people, "policies": policies, "claims": claims}
    widths = {"policyholders": 10, "policies": 8, "claims": 7}

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        for schema in ("landing", "raw"):
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for table, columns in SCHEMA.items():
            for schema in ("landing", "raw"):
                con.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
                con.execute(f"CREATE TABLE {schema}.{table} ({columns})")
            _load(con, f"landing.{table}", payloads[table], widths[table])
            # The operational copy starts as a faithful copy of what landed.
            con.execute(f"INSERT INTO raw.{table} SELECT * FROM landing.{table}")
    finally:
        con.close()
    return db_path


def summarise(db_path: Path = DEFAULT_DB) -> dict[str, object]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        out: dict[str, object] = {}
        for table in SCHEMA:
            out[f"landing.{table}"] = con.execute(
                f"SELECT count(*) FROM landing.{table}"
            ).fetchone()[0]
            out[f"raw.{table}"] = con.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
        out["gross_written_premium"] = float(
            con.execute("SELECT sum(annual_premium) FROM raw.policies").fetchone()[0]
        )
        out["lapsed_policies"] = con.execute(
            "SELECT count(*) FROM raw.policies WHERE status = 'lapsed'"
        ).fetchone()[0]
        return out
    finally:
        con.close()


if __name__ == "__main__":
    path = build()
    for key, value in summarise(path).items():
        print(f"{key:>26}: {value:,.2f}" if isinstance(value, float) else f"{key:>26}: {value:,}")
