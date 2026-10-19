"""Credentials as the manager sees them: what there is now, and when it runs out.

The classes are :mod:`xrdsec.lifetime`'s - the same objects, re-exported
here because this is where an xrdwms user looks for them - and so is the
expiry and renewal bookkeeping the manager does with them
(:class:`xrdsec.lifetime.CredentialWatch`). A provider answers
:meth:`CredentialProvider.current`, re-reading each time, so a proxy made
afresh with ``xrdarc proxy`` or ``voms-proxy-init`` is noticed; the manager
then warns once before it expires and, when a *new* one appears, has
backends that support it (ARC) renew the credential their running jobs hold.

Acquiring tokens (OIDC device flow, Vault) is xrdsec's job and not here yet.
"""

from __future__ import annotations

from xrdsec.lifetime import BearerToken as BearerToken
from xrdsec.lifetime import Credential as Credential
from xrdsec.lifetime import CredentialProvider as CredentialProvider
from xrdsec.lifetime import ProxyFile as ProxyFile

__all__ = ["BearerToken", "Credential", "CredentialProvider", "ProxyFile"]
