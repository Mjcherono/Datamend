"""Tests for baseline capture and promotion.

The behaviour that matters most is the separation: capturing must not promote. If
it did, a corrupt run would silently become the reference everything else is
judged against.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from datamend.detect.baseline import capture, current, load, promote, require_current, versions

CONFIG = """
relations:
  - name: m.t
    keys: [id]
    reconciliations:
      total: "sum(amount)"
"""


@pytest.fixture()
def db(tmp_path: Path) -> Path:
    path = tmp_path / "t.duckdb"
    con = duckdb.connect(str(path))
    con.execute("create schema m")
    con.execute("create table m.t (id bigint, amount double)")
    con.execute("insert into m.t values (1, 10.0), (2, 20.0)")
    con.close()
    return path


@pytest.fixture()
def config(tmp_path: Path) -> Path:
    path = tmp_path / "probes.yml"
    path.write_text(CONFIG, encoding="utf-8")
    return path


def test_capture_does_not_promote(db: Path, config: Path, tmp_path: Path) -> None:
    store = tmp_path / "baselines"
    capture(db, config_path=config, directory=store)
    assert current(store) is None


def test_promote_is_explicit(db: Path, config: Path, tmp_path: Path) -> None:
    store = tmp_path / "baselines"
    captured = capture(db, config_path=config, directory=store)
    promoted = promote(captured.version, directory=store)
    active = current(store)
    assert active is not None
    assert active.version == promoted.version == captured.version


def test_versions_increment(db: Path, config: Path, tmp_path: Path) -> None:
    store = tmp_path / "baselines"
    first = capture(db, config_path=config, directory=store)
    second = capture(db, config_path=config, directory=store)
    assert (first.version, second.version) == (1, 2)
    assert versions(store) == [1, 2]


def test_promoting_an_older_version_is_allowed(db: Path, config: Path, tmp_path: Path) -> None:
    """A bad baseline has to be recoverable by going back to a good one."""
    store = tmp_path / "baselines"
    first = capture(db, config_path=config, directory=store)
    second = capture(db, config_path=config, directory=store)
    promote(second.version, directory=store)
    promote(first.version, directory=store)
    active = current(store)
    assert active is not None and active.version == first.version


def test_round_trip_preserves_probe_values(db: Path, config: Path, tmp_path: Path) -> None:
    store = tmp_path / "baselines"
    captured = capture(db, config_path=config, directory=store)
    reloaded = load(captured.version, directory=store)
    assert reloaded.values.values == captured.values.values
    assert reloaded.values.probes == captured.values.probes


def test_require_current_explains_itself_when_nothing_promoted(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="no baseline promoted"):
        require_current(tmp_path / "baselines")


def test_capture_records_run_metadata(db: Path, config: Path, tmp_path: Path) -> None:
    store = tmp_path / "baselines"
    captured = capture(db, note="clean build", config_path=config, directory=store)
    assert captured.note == "clean build"
    assert captured.captured_at.endswith("+00:00")
    assert captured.values.values["m.t::recon::total"] == 30.0
