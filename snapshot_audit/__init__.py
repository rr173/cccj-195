"""Read-only offline auditor for snapshot-transaction histories.

The auditor imports a recorded history (initial state, complete commit-log
range declaration, per-transaction operation records) and decides one of:

  * ``valid``   - evidence is complete, every recorded read matches the
                  snapshot it claims, and the dependency graph over declared
                  version order / read observations / real-time is acyclic.
                  A topological order plus a replay certificate is emitted.
  * ``invalid`` - a confirmed violation: a read observed a version it could
                  not legally observe (future / aborted / unknown / stale /
                  value mismatch / missing / extra), or the dependency graph
                  contains a cycle.  A concrete, independently re-checked
                  witness (anomaly record or cycle) is emitted.
  * ``unknown`` - evidence is incomplete (undeclared log range, missing
                  operations, unfinished scans, broken version chains, ...)
                  and no confirmed violation exists.  The gaps are listed.

The auditor never schedules transactions, never mutates the source, and
never "repairs" history by retrying.
"""

__version__ = "0.1.0"
