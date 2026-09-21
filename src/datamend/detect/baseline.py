"""Known-good probe values, versioned.

Detection is a diff against last known-good, so the known-good has to be
trustworthy. Capture and promotion are separate on purpose: capturing records a
candidate, promoting makes it the thing everything is judged against. If those
were one step, the first corrupt run to go unnoticed would quietly become the new
normal and every later run would be measured against it.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from datamend.detect.probes import ProbeValues, build_probes, load_config, run_probes

BASELINE_DIR = Path("baselines")
CURRENT = "CURRENT.json"


def _git_sha() -> str | None:
    """The commit the baseline was taken at, so a stale one can be spotted."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None if result.returncode == 0 else None


@dataclass
class Baseline:
    """One captured set of known-good probe values."""

    version: int
    captured_at: str
    values: ProbeValues
    note: str = ""
    git_sha: str | None = None
    #: Warehouse snapshot this baseline describes, once #5 lands.
    snapshot: str | None = None

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "captured_at": self.captured_at,
            "note": self.note,
            "git_sha": self.git_sha,
            "snapshot": self.snapshot,
            "probe_values": self.values.as_dict(),
        }

    @classmethod
    def from_dict(cls, payload: dict) -> Baseline:
        return cls(
            version=payload["version"],
            captured_at=payload["captured_at"],
            values=ProbeValues.from_dict(payload["probe_values"]),
            note=payload.get("note", ""),
            git_sha=payload.get("git_sha"),
            snapshot=payload.get("snapshot"),
        )


def _path(version: int, directory: Path = BASELINE_DIR) -> Path:
    return directory / f"v{version:03d}.json"


def versions(directory: Path = BASELINE_DIR) -> list[int]:
    if not directory.exists():
        return []
    out = []
    for path in directory.glob("v*.json"):
        try:
            out.append(int(path.stem.lstrip("v")))
        except ValueError:
            continue
    return sorted(out)


def load(version: int, directory: Path = BASELINE_DIR) -> Baseline:
    path = _path(version, directory)
    if not path.exists():
        raise FileNotFoundError(f"no baseline v{version:03d} in {directory}")
    return Baseline.from_dict(json.loads(path.read_text(encoding="utf-8")))


def capture(
    db_path: Path,
    note: str = "",
    config_path: Path | None = None,
    directory: Path = BASELINE_DIR,
    snapshot: str | None = None,
) -> Baseline:
    """Measure the warehouse and store it as the next version.

    Does not promote. The caller decides whether what it just measured deserves to
    become the reference.
    """
    config = load_config(config_path) if config_path else load_config()
    probe_set = build_probes(db_path, config)
    values = run_probes(db_path, probe_set)

    version = (versions(directory)[-1] + 1) if versions(directory) else 1
    baseline = Baseline(
        version=version,
        captured_at=datetime.now(UTC).isoformat(timespec="seconds"),
        values=values,
        note=note,
        git_sha=_git_sha(),
        snapshot=snapshot,
    )
    directory.mkdir(parents=True, exist_ok=True)
    _path(version, directory).write_text(
        json.dumps(baseline.as_dict(), indent=2, sort_keys=True), encoding="utf-8"
    )
    return baseline


def promote(version: int, directory: Path = BASELINE_DIR) -> Baseline:
    """Make ``version`` the baseline everything is judged against.

    Deliberately a separate, explicit act. Never call this from a detection path.
    """
    baseline = load(version, directory)
    (directory / CURRENT).write_text(
        json.dumps({"version": version}, indent=2), encoding="utf-8"
    )
    return baseline


def current(directory: Path = BASELINE_DIR) -> Baseline | None:
    """The promoted baseline, or None if nothing has been promoted yet."""
    pointer = directory / CURRENT
    if not pointer.exists():
        return None
    version = json.loads(pointer.read_text(encoding="utf-8"))["version"]
    return load(version, directory)


def require_current(directory: Path = BASELINE_DIR) -> Baseline:
    baseline = current(directory)
    if baseline is None:
        raise RuntimeError(
            "no baseline promoted; capture one against a clean build and promote it"
        )
    return baseline
