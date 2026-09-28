"""Find every place a person exists in the warehouse.

This is the primitive the whole product rests on. Proving someone was deleted is
not a matter of checking the table you deleted them from, it means sweeping
everywhere they could be and saying, with the query that proves it, that they
are not there.

Two ways a subject is found. By key, wherever a person_id column exists, and by
identifier, wherever a distinctive value of theirs appears in a text column.
The second matters because derived tables routinely drop the id and keep the
email, which leaves the person perfectly identifiable and invisible to any check
that only looks at keys.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb

#: Schemas that hold warehouse content. `landing` is included deliberately: it is
#: usually the reason an erasure did not hold.
SCHEMAS = ("landing", "raw", "main_staging", "main_marts")

KEY_COLUMN = "person_id"


@dataclass(frozen=True)
class Subject:
    """A person, and the values that identify them."""

    person_id: int
    identifiers: tuple[str, ...] = ()
    label: str = ""

    def describe(self) -> str:
        return self.label or f"person {self.person_id}"


@dataclass(frozen=True)
class Appearance:
    """One place a subject was found, with the reason it counted."""

    relation: str
    rows: int
    matched_on: str
    column: str | None = None

    def __str__(self) -> str:
        where = f"{self.relation}.{self.column}" if self.column else self.relation
        return f"{where}  {self.rows} row(s)  via {self.matched_on}"


@dataclass
class PresenceReport:
    """Everywhere a subject was found in one sweep."""

    subject: Subject
    appearances: list[Appearance] = field(default_factory=list)

    @property
    def present(self) -> bool:
        return bool(self.appearances)

    @property
    def total_rows(self) -> int:
        return sum(a.rows for a in self.appearances)

    def relations(self) -> list[str]:
        return sorted({a.relation for a in self.appearances})


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _columns(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str]]:
    """Every (schema, table, column) in the schemas worth sweeping."""
    placeholders = ", ".join("?" * len(SCHEMAS))
    return con.execute(
        f"""
        select table_schema, table_name, column_name
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
        identifiers=tuple(v for v in (email, eircode, phone) if v),
        label=f"{first} {last} (person {person_id})",
    )


def find_subject(db_path: Path, subject: Subject) -> PresenceReport:
    """Sweep the warehouse for one subject.

    Text columns are matched on exact identifier values rather than a substring
    search, so a sweep stays cheap enough to run on every pipeline execution
    rather than as an occasional audit.
    """
    report = PresenceReport(subject=subject)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        by_table: dict[tuple[str, str], list[str]] = {}
        for schema, table, column in _columns(con):
            by_table.setdefault((schema, table), []).append(column)

        for (schema, table), columns in by_table.items():
            relation = f"{schema}.{table}"
            if KEY_COLUMN in columns:
                rows = con.execute(
                    f"select count(*) from {relation} where {_quote(KEY_COLUMN)} = ?",
                    [subject.person_id],
                ).fetchone()[0]
                if rows:
                    report.appearances.append(
                        Appearance(relation, rows, "person_id", KEY_COLUMN)
                    )
            if not subject.identifiers:
                continue
            text_columns = con.execute(
                """
                select column_name from information_schema.columns
                where table_schema = ? and table_name = ? and data_type = 'VARCHAR'
                """,
                [schema, table],
            ).fetchall()
            for (column,) in text_columns:
                placeholders = ", ".join("?" * len(subject.identifiers))
                rows = con.execute(
                    f"select count(*) from {relation} where {_quote(column)} in ({placeholders})",
                    list(subject.identifiers),
                ).fetchone()[0]
                if rows:
                    report.appearances.append(
                        Appearance(relation, rows, "identifier", column)
                    )
    finally:
        con.close()
    report.appearances.sort(key=lambda a: (a.relation, a.column or ""))
    return report
