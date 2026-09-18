"""Measurements taken over the mart layer, to be diffed against last known-good.

Four families, because no single one catches everything. Measured against the
attack catalogue: a fan-out moves row counts, a contract break moves null rates,
and a price drift moves neither. Only a reconciliation total sees that last one,
and a probe set without it is blind to a whole class of corruption.

Probes are declared per relation, never per attack. The detector must work on a
corruption nobody anticipated, so nothing here may key on which attack ran.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import duckdb
import yaml

DEFAULT_CONFIG = Path("probes.yml")


class ProbeKind(str, Enum):
    """The four probe families."""

    ROW_COUNT = "row_count"
    NULL_RATE = "null_rate"
    DISTINCT_COUNT = "distinct_count"
    RECONCILIATION = "reconciliation"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@dataclass(frozen=True)
class Probe:
    """One measurement. ``key`` is stable across runs so values can be diffed."""

    key: str
    kind: ProbeKind
    relation: str
    column: str | None = None
    expression: str | None = None

    def select(self) -> str:
        """The SELECT fragment that computes this probe against its relation."""
        alias = _quote(self.key)
        if self.kind is ProbeKind.ROW_COUNT:
            return f"count(*)::DOUBLE as {alias}"
        if self.kind is ProbeKind.NULL_RATE:
            col = _quote(self.column or "")
            # An empty relation has no null rate, rather than a misleading zero.
            return (
                f"(case when count(*) = 0 then null else "
                f"count(*) filter (where {col} is null)::DOUBLE / count(*) end) as {alias}"
            )
        if self.kind is ProbeKind.DISTINCT_COUNT:
            col = _quote(self.column or "")
            return f"count(distinct {col})::DOUBLE as {alias}"
        return f"({self.expression})::DOUBLE as {alias}"


@dataclass
class ProbeSet:
    """Every probe to run, grouped by the relation it reads."""

    probes: list[Probe] = field(default_factory=list)

    def by_relation(self) -> dict[str, list[Probe]]:
        grouped: dict[str, list[Probe]] = {}
        for probe in self.probes:
            grouped.setdefault(probe.relation, []).append(probe)
        return grouped

    def __len__(self) -> int:
        return len(self.probes)


@dataclass
class ProbeValues:
    """Probe results from one run, keyed by probe key."""

    values: dict[str, float | None] = field(default_factory=dict)
    probes: dict[str, Probe] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "values": self.values,
            "probes": {
                key: {
                    "kind": probe.kind.value,
                    "relation": probe.relation,
                    "column": probe.column,
                    "expression": probe.expression,
                }
                for key, probe in self.probes.items()
            },
        }

    @classmethod
    def from_dict(cls, payload: dict) -> ProbeValues:
        probes = {
            key: Probe(
                key=key,
                kind=ProbeKind(meta["kind"]),
                relation=meta["relation"],
                column=meta.get("column"),
                expression=meta.get("expression"),
            )
            for key, meta in payload.get("probes", {}).items()
        }
        return cls(values=dict(payload.get("values", {})), probes=probes)


def load_config(path: Path = DEFAULT_CONFIG) -> dict:
    """Read the probe declaration."""
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    schema, _, table = relation.rpartition(".")
    rows = con.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = ? and table_name = ?
        order by ordinal_position
        """,
        [schema, table],
    ).fetchall()
    return [r[0] for r in rows]


def build_probes(db_path: Path, config: dict) -> ProbeSet:
    """Expand the declaration into concrete probes against the live schema.

    Row counts and null rates come from the relation's actual columns, so a new
    column is covered without anyone remembering to list it.
    """
    probes: list[Probe] = []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        for entry in config.get("relations", []):
            relation = entry["name"]
            available = _columns(con, relation)
            if not available:
                raise ValueError(f"relation {relation} not found; run dbt build first")

            probes.append(
                Probe(key=f"{relation}::row_count", kind=ProbeKind.ROW_COUNT, relation=relation)
            )
            for column in available:
                probes.append(
                    Probe(
                        key=f"{relation}::null_rate::{column}",
                        kind=ProbeKind.NULL_RATE,
                        relation=relation,
                        column=column,
                    )
                )
            for column in entry.get("keys", []):
                if column not in available:
                    raise ValueError(f"{relation}: declared key {column!r} is not a column")
                probes.append(
                    Probe(
                        key=f"{relation}::distinct::{column}",
                        kind=ProbeKind.DISTINCT_COUNT,
                        relation=relation,
                        column=column,
                    )
                )
            for name, expression in (entry.get("reconciliations") or {}).items():
                probes.append(
                    Probe(
                        key=f"{relation}::recon::{name}",
                        kind=ProbeKind.RECONCILIATION,
                        relation=relation,
                        expression=expression,
                    )
                )
    finally:
        con.close()
    return ProbeSet(probes=probes)


def run_probes(db_path: Path, probe_set: ProbeSet) -> ProbeValues:
    """Execute every probe.

    One query per relation rather than one per probe: the mart layer is scanned a
    handful of times instead of a few hundred, which keeps a full sweep inside a
    couple of seconds.
    """
    out = ProbeValues()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        for relation, probes in probe_set.by_relation().items():
            sql = "select " + ", ".join(p.select() for p in probes) + f" from {relation}"
            row = con.execute(sql).fetchone()
            for probe, value in zip(probes, row or []):
                out.values[probe.key] = None if value is None else float(value)
                out.probes[probe.key] = probe
    finally:
        con.close()
    return out


def write_values(path: Path, values: ProbeValues) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values.as_dict(), indent=2, sort_keys=True), encoding="utf-8")


@dataclass(frozen=True)
class ProbeDelta:
    """One probe's movement between a baseline and an observed run."""

    probe: Probe
    baseline: float | None
    observed: float | None
    #: None when a relative change is undefined, e.g. a baseline of zero.
    relative: float | None

    @property
    def absolute(self) -> float | None:
        if self.baseline is None or self.observed is None:
            return None
        return self.observed - self.baseline


def diff_values(
    baseline: ProbeValues,
    observed: ProbeValues,
    rel_tol: float = 1e-9,
) -> list[ProbeDelta]:
    """Probes that moved, largest relative movement first.

    Mechanical comparison only. Deciding what a movement means belongs to the
    detector, not here.
    """
    moved: list[ProbeDelta] = []
    for key, before in baseline.values.items():
        if key not in observed.values:
            continue
        after = observed.values[key]
        if before is None and after is None:
            continue
        if before is not None and after is not None:
            if before == after:
                continue
            scale = abs(before)
            if scale > 0 and abs(after - before) / scale <= rel_tol:
                continue
            relative = (after - before) / scale if scale > 0 else None
        else:
            relative = None
        probe = observed.probes.get(key) or baseline.probes[key]
        moved.append(ProbeDelta(probe=probe, baseline=before, observed=after, relative=relative))
    moved.sort(key=lambda d: abs(d.relative) if d.relative is not None else float("inf"), reverse=True)
    return moved
