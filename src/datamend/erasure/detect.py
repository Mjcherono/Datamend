"""Catch an erased subject who has come back.

The register says who must be absent. The sweep says who is present. A finding is
the intersection, which is a question nobody currently asks because no green
pipeline has any reason to raise it.

Presence in the landing zone is expected and is not a finding on its own. The
landing zone is an append-only record of what the source systems sent, and
clearing it is a separate decision with its own consequences. What matters is
whether the subject is visible to the business again.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from datamend.erasure.presence import Appearance, PresenceReport, find_subjects
from datamend.erasure.register import ErasureRecord, ErasureRegister

SERVING_PREFIXES = ("raw.", "main_staging.", "main_marts.")


@dataclass
class Resurrection:
    """An erased subject found back in the serving layer."""

    record: ErasureRecord
    appearances: list[Appearance] = field(default_factory=list)
    likely_vector: str = "unknown"
    reasoning: str = ""

    @property
    def relations(self) -> list[str]:
        return sorted({a.relation for a in self.appearances})

    @property
    def rows(self) -> int:
        return sum(a.rows for a in self.appearances)

    def as_dict(self) -> dict:
        return {
            "person_id": self.record.person_id,
            "request_ref": self.record.request_ref,
            "erased_at": self.record.erased_at,
            "relations": self.relations,
            "rows": self.rows,
            "likely_vector": self.likely_vector,
            "reasoning": self.reasoning,
            "appearances": [
                {
                    "relation": a.relation,
                    "column": a.column,
                    "rows": a.rows,
                    "matched_on": a.matched_on,
                }
                for a in self.appearances
            ],
        }


def _serving(report: PresenceReport) -> list[Appearance]:
    return [
        a
        for a in report.appearances
        if a.conclusive and a.relation.startswith(SERVING_PREFIXES)
    ]


def _operational_matches_landing(db_path: Path) -> bool:
    """Whether the operational copy is currently a faithful copy of the landing zone.

    Computed once per detection run. Asking it per finding means a full scan of
    both tables for every subject, which is what made an earlier version take
    thirty-five seconds to explain five hundred findings.
    """
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(
            """
            select count(*) = 0 from (
                select * from landing.policyholders
                except select * from raw.policyholders
            )
            """
        ).fetchone()[0]
    finally:
        con.close()


def _classify(appearances: list[Appearance], resynced: bool) -> tuple[str, str]:
    """Work out how the subject got back, from what is on the table now.

    Evidence only. Nothing here consults a list of known vectors by name, because
    the vector that matters is the one nobody anticipated.
    """
    relations = {a.relation for a in appearances}
    in_operational = any(r.startswith("raw.") for r in relations)
    in_derived = any(r.startswith(("main_staging.", "main_marts.")) for r in relations)

    if in_operational:
        if resynced:
            return (
                "source_resync",
                (
                    "The operational copy matches the landing zone exactly, so it was "
                    "refreshed from a source that still holds the subject. The sync did "
                    "what it was built to do."
                ),
            )
        return (
            "operational_reinstatement",
            (
                "The subject is back in the operational tables but those tables do not "
                "match the landing zone, so something other than a full resync put them "
                "there. A restore or a partial reload are the usual causes."
            ),
        )
    if in_derived:
        return (
            "stale_derived",
            (
                "The subject is absent from the operational tables but still present "
                "downstream, so a derived model has not been rebuilt since the erasure. "
                "Nothing will fix this until that model next runs."
            ),
        )
    return ("unknown", "Present in the serving layer by a route the evidence does not explain.")


def detect_resurrections(
    db_path: Path,
    register: ErasureRegister | None = None,
) -> list[Resurrection]:
    """Every erased subject currently visible to the business."""
    register = register or ErasureRegister.load()
    if not len(register):
        return []
    reports = find_subjects(db_path, register.subjects())
    findings: list[Resurrection] = []
    resynced: bool | None = None
    for person_id, report in reports.items():
        appearances = _serving(report)
        if not appearances:
            continue
        if resynced is None:
            resynced = _operational_matches_landing(db_path)
        vector, reasoning = _classify(appearances, resynced)
        findings.append(
            Resurrection(
                record=register.records[person_id],
                appearances=appearances,
                likely_vector=vector,
                reasoning=reasoning,
            )
        )
    findings.sort(key=lambda f: (-f.rows, f.record.person_id))
    return findings
