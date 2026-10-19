"""Fixtures shared by every test module.

Every test is hermetic: no ambient proxy, token, configuration, journal or
ARC job list from the developer's shell leaks in. The credential side - the
environment cleared, ``HOME`` fresh, xrdsec's ``TMP_DIR`` (the ``/tmp`` of
``x509up_u<uid>``) an empty directory, and the ``home``, ``pki`` and
``grid_env`` fixtures - is :mod:`xrdsec.testing.fixtures`, the plugin every
suite on the stack loads; :func:`pytest_xrdsec_hermetic` adds xrdwms's own
inputs. Interop tests are marked ``ambient`` and opt out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from xrdarc import config as arc_config
from xrdarc.testing.fixtures import gate_interop

if TYPE_CHECKING:
    from xrdsec.testing.fixtures import Hermetic

# xrdarc.testing.fixtures: the fake ARC CE (``ce``, ``make_ce``), its
# ``credentials`` and ``make_arc`` (sessions that never retry or sleep), the
# same fixtures xrdarc's own suite uses.
pytest_plugins = ["xrdsec.testing.fixtures", "xrdarc.testing.fixtures"]

#: xrdwms's own inputs (and ARC's and the XDG locations it keeps state in),
#: cleared on top of the credential variables the plugin clears.
_OWN = (
    "XDG_STATE_HOME",
    "XDG_CONFIG_HOME",
    "XRDWMS_CONFIG",
    "XRDWMS_JOURNAL",
    "TMPDIR",
    "FORCE_COLOR",
)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Interop tests need a real CE: ``ambient``, and skipped unless XRDWMS_INTEROP=1."""
    gate_interop(
        items, variable="XRDWMS_INTEROP", reason="needs a real ARC CE; set XRDWMS_INTEROP=1"
    )


def pytest_xrdsec_hermetic(hermetic: Hermetic) -> None:
    """xrdwms's own inputs, on top of xrdsec's: its variables, and no system ARC ``client.conf``."""
    hermetic.unset(*_OWN)
    hermetic.monkeypatch.setattr(
        arc_config, "SYSTEM_CONFIG", hermetic.home / "no-system-client.conf"
    )
