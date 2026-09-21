"""Unit tests for the probe framework.

These build their own tiny warehouse rather than leaning on the generated one, so
they stay fast and do not need a dbt run.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from datamend.detect.probes import (
    Probe,
    ProbeKind,
    build_probes,
    diff_values,
    run_probes,
)

CONFIG = {
    "relations": [
        {
            "name": "m.t",
            "keys": ["id"],
            "reconciliations": {"total": "sum(amount)"},
        }
    ]
}


@pytest.fixture()
def db(tmp_path: Path) -> Path:
    path = tmp_path / "t.duckdb"
    con = duckdb.connect(str(path))
    con.execute("create schema m")
    con.execute("create table m.t (id bigint, label varchar, amount double)")
    con.execute("insert into m.t values (1,'a',10.0), (2,'b',20.0), (3,null,30.0)")
    con.close()
    return path


def test_probes_cover_every_column_and_declared_key(db: Path) -> None:
    probe_set = build_probes(db, CONFIG)
    kinds = [p.kind for p in probe_set.probes]
    assert kinds.count(ProbeKind.ROW_COUNT) == 1
    # One null-rate probe per column, derived from the schema rather than listed.
    assert kinds.count(ProbeKind.NULL_RATE) == 3
    assert kinds.count(ProbeKind.DISTINCT_COUNT) == 1
    assert kinds.count(ProbeKind.RECONCILIATION) == 1


def test_probe_values(db: Path) -> None:
    values = run_probes(db, build_probes(db, CONFIG)).values
    assert values["m.t::row_count"] == 3
    assert values["m.t::distinct::id"] == 3
    assert values["m.t::recon::total"] == 60.0
    assert values["m.t::null_rate::label"] == pytest.approx(1 / 3)
    assert values["m.t::null_rate::id"] == 0.0


def test_unknown_relation_is_an_error(db: Path) -> None:
    with pytest.raises(ValueError, match="not found"):
        build_probes(db, {"relations": [{"name": "m.missing"}]})


def test_declared_key_must_exist(db: Path) -> None:
    with pytest.raises(ValueError, match="declared key"):
        build_probes(db, {"relations": [{"name": "m.t", "keys": ["nope"]}]})


def test_identical_runs_produce_no_deltas(db: Path) -> None:
    probe_set = build_probes(db, CONFIG)
    before = run_probes(db, probe_set)
    after = run_probes(db, probe_set)
    assert diff_values(before, after) == []


def test_diff_reports_movement_and_orders_by_relative_size(db: Path) -> None:
    probe_set = build_probes(db, CONFIG)
    before = run_probes(db, probe_set)
    con = duckdb.connect(str(db))
    con.execute("insert into m.t values (4,'c',1000.0)")
    con.close()
    deltas = diff_values(before, run_probes(db, probe_set))
    moved = {d.probe.key: d for d in deltas}
    assert moved["m.t::row_count"].absolute == 1
    assert moved["m.t::recon::total"].absolute == pytest.approx(1000.0)
    # The reconciliation total moved far more in relative terms, so it leads.
    assert deltas[0].probe.key == "m.t::recon::total"


def test_duplicate_rows_break_the_key_invariant(db: Path) -> None:
    """The fan-out signal: row count rises while the distinct key count does not."""
    probe_set = build_probes(db, CONFIG)
    con = duckdb.connect(str(db))
    con.execute("insert into m.t select * from m.t")
    con.close()
    values = run_probes(db, probe_set).values
    assert values["m.t::row_count"] == 6
    assert values["m.t::distinct::id"] == 3


def test_null_rate_of_empty_relation_is_null_not_zero(tmp_path: Path) -> None:
    path = tmp_path / "e.duckdb"
    con = duckdb.connect(str(path))
    con.execute("create schema m")
    con.execute("create table m.t (id bigint)")
    con.close()
    values = run_probes(path, build_probes(path, {"relations": [{"name": "m.t"}]})).values
    assert values["m.t::row_count"] == 0
    assert values["m.t::null_rate::id"] is None


def test_column_names_are_quoted(tmp_path: Path) -> None:
    """A column needing quoting must not break the generated SQL."""
    path = tmp_path / "q.duckdb"
    con = duckdb.connect(str(path))
    con.execute("create schema m")
    con.execute('create table m.t ("order date" date, "select" bigint)')
    con.execute("insert into m.t values ('2026-01-01', 1)")
    con.close()
    config = {"relations": [{"name": "m.t", "keys": ["select"]}]}
    values = run_probes(path, build_probes(path, config)).values
    assert values["m.t::row_count"] == 1
    assert values['m.t::distinct::select'] == 1


def test_select_fragment_quotes_the_alias() -> None:
    probe = Probe(key="a::b::c", kind=ProbeKind.ROW_COUNT, relation="m.t")
    assert probe.select() == 'count(*)::DOUBLE as "a::b::c"'
