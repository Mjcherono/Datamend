# Datamend

Datamend is an open-source data engineering agent that watches dbt pipelines and catches the failures a green dashboard hides, such as join fan-out and silent row drops.
When something breaks it proposes a fix and verifies it in a sandbox first, diffing row counts, null rates and reconciliation totals, then waits for a human to approve.
Where a fix would mask an upstream problem rather than solve it, it refuses, quarantines the bad rows and escalates with an incident report.
