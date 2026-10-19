"""xrdwms's release metadata: xrd-devtools' release gate, run on this checkout.

One version source (``src/xrdwms/_version.py``), a CHANGELOG led by exactly
that version's section, project metadata and README that name this
repository, and a release workflow that builds through the shared publish
action: the checks every repository of the chain shares
(``xrddevtools.release``). The gate is found in an xrd-devtools checkout (a
sibling ``../xrd-devtools``, CI's ``.dependencies/xrd-devtools``, or one
installed or on ``PYTHONPATH``); where there is none, it skips with that
environmental reason.
"""

import pytest

plugin = pytest.importorskip(
    "xrddevtools.pytest_plugin",
    reason="environmental: no xrd-devtools checkout (../xrd-devtools, .dependencies/xrd-devtools)",
)

test_xrdwms_release_metadata_is_consistent = plugin.release_gate(__file__)
