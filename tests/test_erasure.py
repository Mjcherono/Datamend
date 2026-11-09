"""Tests for the presence sweep and resurrection detection.

The sharpest one here is the shared-identifier case. An eircode is an address and
households share it, so matching on one and calling it the same person produces a
false accusation. For a tool whose output a DPO acts on, that is worse than
saying nothing.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from datamend.erasure.detect import detect_resurrections
from datamend.erasure.presence import Subject, find_subject, find_subjects
from datamend.erasure.register import ErasureRegister


def _warehouse(path: Path, people: list[tuple], mart: list[tuple] | None = None) -> None:
    con = duckdb.connect(str(path))
    con.execute("create schema landing")
    con.execute("create schema raw")
    con.execute("create schema main_marts")
    ddl = "(person_id bigint, first_name varchar, last_name varchar, email varchar, eircode varchar, phone varchar)"
    for schema in ("landing", "raw"):
        con.execute(f"create table {schema}.policyholders {ddl}")
        con.executemany(f"insert into {schema}.policyholders values (?,?,?,?,?,?)", people)
    con.execute("create table main_marts.dim_policyholder (person_id bigint, email varchar)")
    for row in mart if mart is not None else [(p[0], p[3]) for p in people]:
        con.execute("insert into main_marts.dim_policyholder values (?, ?)", list(row))
    con.close()


PEOPLE = [
    (1, "Aoife", "Byrne", "aoife@example.ie", "D02 X111", "0871111111"),
    # Same household as person 1: shares the eircode, different person.
    (2, "Cian", "Byrne", "cian@example.ie", "D02 X111", "0872222222"),
    (3, "Niamh", "Walsh", "niamh@example.ie", "T12 Y222", "0873333333"),
]


@pytest.fixture()
def db(tmp_path: Path) -> Path:
    path = tmp_path / "w.duckdb"
    _warehouse(path, PEOPLE)
    return path


def _subject(person: tuple) -> Subject:
    return Subject(
        person_id=person[0],
        strong=(person[3],),
        weak=(person[4], person[5]),
        label=f"{person[1]} {person[2]}",
    )


def test_key_and_strong_identifier_both_confirm(db: Path) -> None:
    report = find_subject(db, _subject(PEOPLE[2]))
    assert report.present
    reasons = {a.matched_on for a in report.confirmed}
    assert reasons == {"person_id", "identifier"}


def test_shared_eircode_does_not_confirm_the_wrong_person(db: Path) -> None:
    """Person 1 erased, person 2 still present, sharing an eircode."""
    con = duckdb.connect(str(db))
    for relation in ("raw.policyholders", "main_marts.dim_policyholder"):
        con.execute(f"delete from {relation} where person_id = 1")
    con.close()

    report = find_subject(db, _subject(PEOPLE[0]))
    serving = [a for a in report.confirmed if not a.relation.startswith("landing.")]
    assert serving == [], f"person 1 should be absent, found {serving}"

    # The household match is still reported, flagged as possible rather than proof.
    possible = [a for a in report.appearances if not a.conclusive]
    assert any(a.column == "eircode" for a in possible)


def test_landing_presence_alone_is_not_a_resurrection(db: Path, tmp_path: Path) -> None:
    con = duckdb.connect(str(db))
    for relation in ("raw.policyholders", "main_marts.dim_policyholder"):
        con.execute(f"delete from {relation} where person_id = 3")
    con.close()
    register = ErasureRegister(path=tmp_path / "reg.json")
    register.record(_subject(PEOPLE[2]), request_ref="DSR-3", relations_cleared=["raw.policyholders"])
    assert detect_resurrections(db, register) == []


def test_subject_back_in_the_serving_layer_is_a_finding(db: Path, tmp_path: Path) -> None:
    register = ErasureRegister(path=tmp_path / "reg.json")
    register.record(_subject(PEOPLE[2]), request_ref="DSR-3", relations_cleared=["raw.policyholders"])
    findings = detect_resurrections(db, register)
    assert len(findings) == 1
    assert findings[0].record.person_id == 3
    assert "raw.policyholders" in findings[0].relations


def test_stale_derived_model_is_classified_as_such(db: Path, tmp_path: Path) -> None:
    """Gone from the operational tables, still downstream: the model has not rebuilt."""
    con = duckdb.connect(str(db))
    con.execute("delete from raw.policyholders where person_id = 3")
    con.close()
    register = ErasureRegister(path=tmp_path / "reg.json")
    register.record(_subject(PEOPLE[2]), request_ref="DSR-3", relations_cleared=["raw.policyholders"])
    findings = detect_resurrections(db, register)
    assert len(findings) == 1
    assert findings[0].likely_vector == "stale_derived"


def test_source_resync_is_classified_as_such(db: Path, tmp_path: Path) -> None:
    """Operational copy matches the landing zone exactly, so it was refreshed."""
    register = ErasureRegister(path=tmp_path / "reg.json")
    register.record(_subject(PEOPLE[2]), request_ref="DSR-3", relations_cleared=["raw.policyholders"])
    findings = detect_resurrections(db, register)
    assert findings[0].likely_vector == "source_resync"


def test_empty_register_finds_nothing(db: Path, tmp_path: Path) -> None:
    assert detect_resurrections(db, ErasureRegister(path=tmp_path / "reg.json")) == []


def test_sweep_is_batched_not_per_subject(db: Path) -> None:
    """All subjects come back from one sweep, each attributed correctly."""
    reports = find_subjects(db, [_subject(p) for p in PEOPLE])
    assert set(reports) == {1, 2, 3}
    for person in PEOPLE:
        confirmed = reports[person[0]].confirmed
        assert confirmed, f"person {person[0]} not found"
        # Nobody is attributed rows belonging to somebody else.
        assert all(a.rows == 1 for a in confirmed)


def test_register_round_trip_keeps_identifier_strength(tmp_path: Path) -> None:
    register = ErasureRegister(path=tmp_path / "reg.json")
    register.record(_subject(PEOPLE[0]), request_ref="DSR-1", relations_cleared=[])
    reloaded = ErasureRegister.load(tmp_path / "reg.json")
    subject = reloaded.subjects()[0]
    assert subject.strong == ("aoife@example.ie",)
    assert subject.weak == ("D02 X111", "0871111111")
