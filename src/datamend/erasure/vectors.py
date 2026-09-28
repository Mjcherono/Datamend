"""Erasure, and the ways it comes undone.

Each vector is a normal, sanctioned operation that a data team runs without
thinking about it. None of them are attacks and none of them fail. That is the
whole problem: the pipeline is behaving exactly as designed, and the person who
asked to be forgotten is back.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb

from datamend.erasure.presence import Subject

#: Operational tables an erasure request clears. The landing zone is deliberately
#: absent, which is the most common real configuration and the reason erasure
#: usually fails to hold.
ERASABLE = (
    ("raw.claims", "policy_id in (select policy_id from raw.policies where person_id = ?)"),
    ("raw.policies", "person_id = ?"),
    ("raw.policyholders", "person_id = ?"),
)


def erase(db_path: Path, subject: Subject) -> list[str]:
    """Clear a subject from the operational tables.

    Returns the relations actually changed. Child rows go first so the delete
    can still resolve them through their parent.
    """
    cleared: list[str] = []
    con = duckdb.connect(str(db_path))
    try:
        for relation, predicate in ERASABLE:
            before = con.execute(f"select count(*) from {relation}").fetchone()[0]
            con.execute(f"delete from {relation} where {predicate}", [subject.person_id])
            after = con.execute(f"select count(*) from {relation}").fetchone()[0]
            if after != before:
                cleared.append(relation)
    finally:
        con.close()
    return cleared


def resync_from_landing(db_path: Path, tables: tuple[str, ...] = ()) -> dict[str, int]:
    """Refresh the operational tables from the landing zone.

    The nightly source sync. It is not a bug and nobody reviews it, and it
    reinstates every subject the landing zone still holds.
    """
    targets = tables or ("policyholders", "policies", "claims")
    restored: dict[str, int] = {}
    con = duckdb.connect(str(db_path))
    try:
        for table in targets:
            before = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
            con.execute(f"delete from raw.{table}")
            con.execute(f"insert into raw.{table} select * from landing.{table}")
            after = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
            restored[f"raw.{table}"] = after - before
    finally:
        con.close()
    return restored


def restore_backup(db_path: Path, snapshot: Path) -> dict[str, int]:
    """Reinstate the operational tables from a snapshot taken before the erasure.

    A disaster recovery drill, or a restore after an unrelated incident. The
    EDPB named backup handling among the recurring failures for a reason.
    """
    restored: dict[str, int] = {}
    con = duckdb.connect(str(db_path))
    try:
        con.execute("attach ? as snap (read_only)", [str(snapshot)])
        for table in ("policyholders", "policies", "claims"):
            before = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
            con.execute(f"delete from raw.{table}")
            con.execute(f"insert into raw.{table} select * from snap.raw.{table}")
            after = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
            restored[f"raw.{table}"] = after - before
        con.execute("detach snap")
    finally:
        con.close()
    return restored


@dataclass(frozen=True)
class Vector:
    """One way an erased subject returns."""

    key: str
    name: str
    summary: str
    #: Why no alert fires and no test fails when this happens.
    why_missed: str
    apply: Callable[..., object]


VECTORS: dict[str, Vector] = {
    v.key: v
    for v in [
        Vector(
            key="landing_resync",
            name="Nightly source sync",
            summary="The operational tables are refreshed from the landing zone, which was never cleared.",
            why_missed="The sync is working correctly. Row counts go up, which no volume test treats as a failure.",
            apply=resync_from_landing,
        ),
        Vector(
            key="backup_restore",
            name="Backup restore",
            summary="A restore reinstates the state of the warehouse from before the erasure ran.",
            why_missed="Restores are reviewed for completeness, never for who they bring back.",
            apply=restore_backup,
        ),
    ]
}
