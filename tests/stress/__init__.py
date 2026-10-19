"""The opt-in stress suite (``XRDWMS_STRESS=1``, ``tools/stress.sh``).

A package, so its ``conftest`` and helpers are ``stress.conftest`` and
``stress._load`` - distinct from the main suite's ``conftest`` for pytest and
for mypy, which checks both.
"""
