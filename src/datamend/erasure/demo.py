"""The erasure lifecycle, end to end.

Erase a subject, run the pipeline, confirm they are gone, then run the ordinary
nightly sync and confirm they are back. Nothing here is adversarial. Every step
is an operation a data team runs on purpose.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from datamend.erasure.presence import PresenceReport, find_subject, subject_from_id
from datamend.erasure.register import ErasureRegister
from datamend.erasure.vectors import erase, resync_from_landing

DB = Path("data/insurance.duckdb")
DBT_DIR = Path("dbt")


def run_pipeline() -> bool:
    """Run dbt exactly as the nightly schedule would."""
    result = subprocess.run(
        ["../.venv/bin/dbt", "build"],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", "DBT_PROFILES_DIR": ".", "HOME": str(Path.home())},
        check=False,
    )
    return result.returncode == 0


def _serving_relations(report: PresenceReport) -> list[str]:
    """Where the subject is visible to the business, ignoring the landing zone."""
    return [r for r in report.relations() if not r.startswith("landing.")]


def _show(label: str, report: PresenceReport) -> list[str]:
    serving = _serving_relations(report)
    print(f"\n{label}")
    print(f"  serving layer: {len(serving)} relations, {report.total_rows} rows total")
    for relation in serving:
        print(f"    {relation}")
    if not serving:
        print("    (nothing)")
    return serving


def main(person_id: int = 4471, request_ref: str = "DSR-2026-0417") -> int:
    subject = subject_from_id(DB, person_id)
    print("=" * 74)
    print(f"Erasure request {request_ref}: {subject.describe()}")
    print("=" * 74)

    before = _show("1. Before the request", find_subject(DB, subject))

    cleared = erase(DB, subject)
    if not run_pipeline():
        print("\nPipeline failed during erasure")
        return 1
    register = ErasureRegister.load()
    register.record(subject, request_ref=request_ref, relations_cleared=cleared)
    after_erasure = _show(f"2. After erasure (cleared {', '.join(cleared)}) and a pipeline run",
                          find_subject(DB, subject))

    restored = resync_from_landing(DB)
    green = run_pipeline()
    after_resync = _show("3. After the nightly source sync and the same pipeline run",
                         find_subject(DB, subject))

    print("\n" + "=" * 74)
    print(f"  pipeline exit status on that run : {'success' if green else 'FAILED'}")
    print(f"  rows reinstated by the sync      : {sum(restored.values()):,}")
    print(f"  serving relations before erasure : {len(before)}")
    print(f"  serving relations after erasure  : {len(after_erasure)}")
    print(f"  serving relations now            : {len(after_resync)}")
    print(f"  subject is on the erasure register: {person_id in register.records}")
    print("=" * 74)
    return 0 if after_resync else 1


if __name__ == "__main__":
    raise SystemExit(main())
