"""The attack catalogue.

Every attack here is something that has happened to a real warehouse: a dimension
loaded twice, a filter that quietly narrowed, a source column that started
arriving null. None of them raise an error. Each one is expected to leave the dbt
run green, which is recorded on the attack as ``stays_green`` so the demo can
assert it rather than hope for it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import duckdb


@dataclass(frozen=True)
class Attack:
    """One way to corrupt the warehouse without tripping an error."""

    key: str
    name: str
    summary: str
    #: What a reviewer would have to have anticipated to catch this with a test.
    why_missed: str
    apply: Callable[[duckdb.DuckDBPyConnection, float], dict]
    #: Whether the dbt run is expected to succeed despite the corruption.
    stays_green: bool = True
    #: Attacks Datamend should refuse to auto-patch, because a fix would mask the cause.
    refuse_autofix: bool = False
    default_intensity: float = 0.18


def _fan_out(con: duckdb.DuckDBPyConnection, intensity: float) -> dict:
    """Load a slice of the customer dimension twice.

    The classic bad SCD merge. Every order for an affected customer joins to two
    dimension rows, so its revenue counts twice.
    """
    pct = max(1, min(99, round(intensity * 100)))
    before = con.execute("select count(*) from raw.customers").fetchone()[0]
    con.execute(
        "insert into raw.customers select * from raw.customers "
        "where abs(hash(customer_id)) % 100 < ?",
        [pct],
    )
    after = con.execute("select count(*) from raw.customers").fetchone()[0]
    return {
        "table": "raw.customers",
        "rows_added": after - before,
        "customers_duplicated": after - before,
        "detail": f"{after - before:,} duplicate customer rows inserted ({pct}% of the dimension)",
    }


def _silent_row_drop(con: duckdb.DuckDBPyConnection, intensity: float) -> dict:
    """Quietly drop one region's orders, as a narrowed upstream filter would.

    Revenue falls. Nothing errors, and a row-count test with a loose threshold
    will not notice a single region going missing.
    """
    region = con.execute(
        "select region_code from raw.customers group by 1 order by count(*) desc limit 1 offset 2"
    ).fetchone()[0]
    before = con.execute("select count(*) from raw.orders").fetchone()[0]
    con.execute(
        "delete from raw.orders where customer_id in "
        "(select customer_id from raw.customers where region_code = ?)",
        [region],
    )
    after = con.execute("select count(*) from raw.orders").fetchone()[0]
    return {
        "table": "raw.orders",
        "rows_removed": before - after,
        "region_dropped": region,
        "detail": f"{before - after:,} orders for region {region} removed",
    }


def _null_injection(con: duckdb.DuckDBPyConnection, intensity: float) -> dict:
    """Blank out unit_price on a slice of order lines.

    An upstream contract break. Coalescing the nulls to zero would make the
    pipeline look healthy while understating revenue, which is why Datamend is
    expected to refuse to patch this one.
    """
    pct = max(1, min(99, round(intensity * 100)))
    con.execute(
        "update raw.order_items set unit_price = null "
        "where abs(hash(order_item_id)) % 100 < ?",
        [pct],
    )
    affected = con.execute(
        "select count(*) from raw.order_items where unit_price is null"
    ).fetchone()[0]
    return {
        "table": "raw.order_items",
        "rows_nulled": affected,
        "column": "unit_price",
        "detail": f"unit_price set to null on {affected:,} order lines",
    }


def _unit_price_drift(con: duckdb.DuckDBPyConnection, intensity: float) -> dict:
    """Shift prices on one SKU, as a currency or units mix-up would.

    Row counts, null rates and key cardinality all stay exactly as they were.
    Only the money moves, so only a reconciliation total catches it.
    """
    sku = con.execute(
        "select sku from raw.order_items group by 1 order by count(*) desc limit 1"
    ).fetchone()[0]
    factor = 1 + max(0.01, intensity)
    con.execute(
        "update raw.order_items set unit_price = round(unit_price * ?, 2) where sku = ?",
        [factor, sku],
    )
    affected = con.execute(
        "select count(*) from raw.order_items where sku = ?", [sku]
    ).fetchone()[0]
    return {
        "table": "raw.order_items",
        "rows_changed": affected,
        "sku": sku,
        "factor": factor,
        "detail": f"unit_price on {sku} scaled by {factor:.2f} across {affected:,} lines",
    }


ATTACKS: dict[str, Attack] = {
    a.key: a
    for a in [
        Attack(
            key="fan_out",
            name="Join fan-out",
            summary="A slice of the customer dimension is loaded twice, so orders join to two rows and revenue counts twice.",
            why_missed="Row counts go up, not down, and no test asserts that the customer dimension is unique. Every model succeeds.",
            apply=_fan_out,
        ),
        Attack(
            key="silent_row_drop",
            name="Silent row drop",
            summary="An upstream filter narrows and one region's orders stop arriving.",
            why_missed="Nothing errors. Revenue simply comes in lower, and a percentage-threshold volume test will not flag one region.",
            apply=_silent_row_drop,
        ),
        Attack(
            key="null_injection",
            name="Upstream contract break",
            summary="unit_price starts arriving null on a slice of order lines.",
            why_missed="Sums ignore nulls, so revenue quietly understates rather than failing.",
            apply=_null_injection,
            refuse_autofix=True,
        ),
        Attack(
            key="unit_price_drift",
            name="Unit price drift",
            summary="Prices on the highest-volume SKU shift, as a currency or units mix-up would.",
            why_missed="Row counts, null rates and key counts are all unchanged. Only a reconciliation total moves.",
            apply=_unit_price_drift,
        ),
    ]
}


@dataclass
class AttackResult:
    """What an attack did, for the report and the agent's prompt."""

    attack: Attack
    intensity: float
    effect: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "key": self.attack.key,
            "name": self.attack.name,
            "summary": self.attack.summary,
            "why_missed": self.attack.why_missed,
            "refuse_autofix": self.attack.refuse_autofix,
            "intensity": self.intensity,
            "effect": self.effect,
        }


def run_attack(db_path: Path, key: str, intensity: float | None = None) -> AttackResult:
    """Apply attack ``key`` to the warehouse at ``db_path``."""
    if key not in ATTACKS:
        raise KeyError(f"unknown attack {key!r}; known: {', '.join(sorted(ATTACKS))}")
    attack = ATTACKS[key]
    level = attack.default_intensity if intensity is None else intensity
    con = duckdb.connect(str(db_path))
    try:
        effect = attack.apply(con, level)
    finally:
        con.close()
    return AttackResult(attack=attack, intensity=level, effect=effect)
