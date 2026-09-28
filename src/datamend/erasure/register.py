"""The record of who was erased and when.

Detection is a comparison against this. Without a durable register there is no
question to ask: a subject who is absent today might have been erased last month
or might simply never have existed, and those are very different answers to give
a regulator.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from datamend.erasure.presence import Subject

DEFAULT_PATH = Path("erasure_register.json")


@dataclass
class ErasureRecord:
    """One erasure request, and what it cleared."""

    person_id: int
    request_ref: str
    requested_at: str
    erased_at: str
    strong: list[str] = field(default_factory=list)
    weak: list[str] = field(default_factory=list)
    label: str = ""
    relations_cleared: list[str] = field(default_factory=list)

    def subject(self) -> Subject:
        return Subject(
            person_id=self.person_id,
            strong=tuple(self.strong),
            weak=tuple(self.weak),
            label=self.label,
        )

    def as_dict(self) -> dict:
        return {
            "person_id": self.person_id,
            "request_ref": self.request_ref,
            "requested_at": self.requested_at,
            "erased_at": self.erased_at,
            "strong": self.strong,
            "weak": self.weak,
            "label": self.label,
            "relations_cleared": self.relations_cleared,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> ErasureRecord:
        return cls(**payload)


@dataclass
class ErasureRegister:
    """Every subject the organisation has undertaken to erase."""

    records: dict[int, ErasureRecord] = field(default_factory=dict)
    path: Path = DEFAULT_PATH

    @classmethod
    def load(cls, path: Path = DEFAULT_PATH) -> ErasureRegister:
        if not path.exists():
            return cls(path=path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        records = {
            int(r["person_id"]): ErasureRecord.from_dict(r) for r in payload.get("records", [])
        }
        return cls(records=records, path=path)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"records": [r.as_dict() for r in self.records.values()]}
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def record(
        self,
        subject: Subject,
        request_ref: str,
        relations_cleared: list[str],
        requested_at: str | None = None,
    ) -> ErasureRecord:
        now = datetime.now(UTC).isoformat(timespec="seconds")
        entry = ErasureRecord(
            person_id=subject.person_id,
            request_ref=request_ref,
            requested_at=requested_at or now,
            erased_at=now,
            strong=list(subject.strong),
            weak=list(subject.weak),
            label=subject.label,
            relations_cleared=relations_cleared,
        )
        self.records[subject.person_id] = entry
        self.save()
        return entry

    def subjects(self) -> list[Subject]:
        return [r.subject() for r in self.records.values()]

    def __len__(self) -> int:
        return len(self.records)
