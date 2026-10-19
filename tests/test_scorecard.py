"""xrdwms's take-over scorecard gate: xrd-devtools' scorecard plugin, run on ``src``.

Every repository of the chain is held to the same twelve measurements against
the same corpus of 39 widely used Python libraries. The gate is found in an
xrd-devtools checkout (a sibling ``../xrd-devtools``, CI's
``.dependencies/xrd-devtools``, or one installed or on ``PYTHONPATH``); where
there is none, it skips with that environmental reason. CONTRIBUTING has the
details, and how CI makes the gate required.
"""

import pytest

plugin = pytest.importorskip(
    "xrddevtools.pytest_plugin",
    reason="environmental: no xrd-devtools checkout (../xrd-devtools, .dependencies/xrd-devtools)",
)

test_xrdwms_is_easier_to_take_over_than_the_industry_median = plugin.scorecard_gate(__file__)
