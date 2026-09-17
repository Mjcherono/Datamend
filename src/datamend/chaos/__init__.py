"""Chaos attacks against the warehouse.

Each attack corrupts raw data the way a real upstream change would, then lets the
pipeline run normally. The point is not that the pipeline breaks. The point is
that it does not.
"""

from datamend.chaos.attacks import ATTACKS, Attack, AttackResult, run_attack

__all__ = ["ATTACKS", "Attack", "AttackResult", "run_attack"]
