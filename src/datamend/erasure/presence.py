"""Find every place a person exists in the warehouse.

This is the primitive the whole product rests on. Proving someone was deleted is
not a matter of checking the table you deleted them from. It means sweeping
everywhere they could be and saying, with the query that proves it, that they
are not there.

Three ways a subject is found, and the difference between them matters:

- **By key**, wherever a `person_id` column exists. Conclusive.
- **By strong identifier**, a value unique to one person such as an email
  address. Conclusive.
- **By weak identifier**, a value people share. An eircode is an address and
  households share one; phone numbers get shared and reassigned. In this
  warehouse 43 eircodes and 4 phone numbers belong to more than one person, so a
  match on those is corroborating evidence and never proof.

Telling a DPO that someone is back when it was actually their neighbour is worse
than saying nothing, so weak matches never raise a finding on their own.
"""

from __future__ import annotations

import csv
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

#: Schemas that hold warehouse content. `landing` is included deliberately: it is
#: usually the reason an erasure did not hold.
SCHEMAS = ("landing", "raw", "main_staging", "main_marts")

KEY_COLUMN = "person_id"

CONFIRMED = "confirmed"
POSSIBLE = "possible"


@dataclass(frozen=True)
class Subject:
    """A person, and the values that identify them.

    ``strong`` holds values unique to this person. ``weak`` holds values they may
    share with others.
    """

    person_id: int
    strong: tuple[str, ...] = ()
    weak: tuple[str, ...] = ()
    label: str = ""

    def describe(self) -> str:
        return self.label or f"person {self.person_id}"

    @property
    def identifiers(self) -> tuple[str, ...]:
        return self.strong + self.weak


@dataclass(frozen=True)
class Appearance:
    """One place a subject was found, with the reason it counted."""

    relation: str
    rows: int
    matched_on: str
    column: str | None = None
    confidence: str = CONFIRMED

    @property
    def conclusive(self) -> bool:
        return self.confidence == CONFIRMED

    def __str__(self) -> str:
        where = f"{self.relation}.{self.column}" if self.column else self.relation
        qualifier = "" if self.conclusive else " [possible]"
        return f"{where}  {self.rows} row(s)  via {self.matched_on}{qualifier}"


@dataclass
class PresenceReport:
    """Everywhere a subject was found in one sweep."""

    subject: Subject
    appearances: list[Appearance] = field(default_factory=list)

    @property
    def confirmed(self) -> list[Appearance]:
        return [a for a in self.appearances if a.conclusive]

    @property
    def present(self) -> bool:
        """Present only when something conclusive says so."""
        return bool(self.confirmed)

    @property
    def total_rows(self) -> int:
        return sum(a.rows for a in self.confirmed)

    def relations(self) -> list[str]:
        return sorted({a.relation for a in self.confirmed})


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _columns(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str, str]]:
    placeholders = ", ".join("?" * len(SCHEMAS))
    return con.execute(
        f"""
        select table_schema, table_name, column_name, data_type
        from information_schema.columns
        where table_schema in ({placeholders})
        order by table_schema, table_name, ordinal_position
        """,
        list(SCHEMAS),
    ).fetchall()


def subject_from_id(db_path: Path, person_id: int) -> Subject:
    """Build a subject from the landing zone, which outlives every erasure."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            """
            select first_name, last_name, email, eircode, phone
            from landing.policyholders where person_id = ?
            """,
            [person_id],
        ).fetchone()
    finally:
        con.close()
    if row is None:
        raise KeyError(f"no policyholder {person_id} in the landing zone")
    first, last, email, eircode, phone = row
    return Subject(
        person_id=person_id,
        strong=tuple(v for v in (email,) if v),
        weak=tuple(v for v in (eircode, phone) if v),
        label=f"{first} {last} (person {person_id})",
    )


def _stage(con: duckdb.DuckDBPyConnection, subjects: list[Subject]) -> bool:
    """Stage the register into temp tables so relations can be joined, not scanned.

    A sweep driven by `IN (...)` costs a full scan per subject once the register
    is any size. Joining against a staged set keeps one pass per relation however
    many subjects are on it.
    """
    con.execute("create temp table _subjects (person_id bigint)")
    con.execute("create temp table _ids (value varchar, person_id bigint, strong boolean)")
    rows = [(v, s.person_id, True) for s in subjects for v in s.strong]
    rows += [(v, s.person_id, False) for s in subjects for v in s.weak]
    with tempfile.TemporaryDirectory() as tmp:
        # Staged through CSV rather than row-at-a-time INSERT. Inserting the
        # register one row at a time made sweep cost scale with register size,
        # which is exactly what joining was supposed to avoid.
        ids_file = Path(tmp) / "subjects.csv"
        with ids_file.open("w", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerows([(s.person_id,) for s in subjects])
        con.execute(
            "COPY _subjects FROM ? (FORMAT CSV, HEADER false)", [str(ids_file)]
        )
        if rows:
            values_file = Path(tmp) / "identifiers.csv"
            with values_file.open("w", newline="", encoding="utf-8") as handle:
                csv.writer(handle).writerows(rows)
            con.execute(
                "COPY _ids FROM ? (FORMAT CSV, HEADER false)", [str(values_file)]
            )
    # A value shared by two people identifies neither, whatever it was declared as.
    con.execute(
        """
        create temp table _lookup as
        select value, any_value(person_id) as person_id,
               bool_and(strong) and count(distinct person_id) = 1 as strong
        from _ids group by value
        """
    )
    return bool(rows)


def find_subjects(db_path: Path, subjects: list[Subject]) -> dict[int, PresenceReport]:
    """Sweep for many subjects at once.

    One pass per relation and per text column, regardless of register size.
    """
    reports = {s.person_id: PresenceReport(subject=s) for s in subjects}
    if not subjects:
        return reports

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        has_identifiers = _stage(con, subjects)
        by_table: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for schema, table, column, data_type in _columns(con):
            by_table.setdefault((schema, table), []).append((column, data_type))

        for (schema, table), columns in by_table.items():
            relation = f"{schema}.{table}"
            names = [c for c, _t in columns]
            if KEY_COLUMN in names:
                rows = con.execute(
                    f"""
                    select r.{_quote(KEY_COLUMN)}, count(*)
                    from {relation} r
                    join _subjects s on s.person_id = r.{_quote(KEY_COLUMN)}
                    group by 1
                    """
                ).fetchall()
                for person_id, count in rows:
                    reports[person_id].appearances.append(
                        Appearance(relation, count, "person_id", KEY_COLUMN, CONFIRMED)
                    )
            if not has_identifiers:
                continue
            for column, data_type in columns:
                if data_type != "VARCHAR":
                    continue
                rows = con.execute(
                    f"""
                    select l.person_id, l.strong, count(*)
                    from {relation} r
                    join _lookup l on l.value = r.{_quote(column)}
                    group by 1, 2
                    """
                ).fetchall()
                for person_id, strong, count in rows:
                    reports[person_id].appearances.append(
                        Appearance(
                            relation,
                            count,
                            "identifier" if strong else "shared identifier",
                            column,
                            CONFIRMED if strong else POSSIBLE,
                        )
                    )
    finally:
        con.close()
    for report in reports.values():
        report.appearances.sort(key=lambda a: (a.relation, a.column or ""))
    return reports


def find_subject(db_path: Path, subject: Subject) -> PresenceReport:
    """Sweep the warehouse for one subject."""
    return find_subjects(db_path, [subject])[subject.person_id]
