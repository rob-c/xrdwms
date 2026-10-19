"""Configuration: named backends, the journal, the polling policy, from one TOML file.

``$XRDWMS_CONFIG``, else ``$XDG_CONFIG_HOME/xrdwms/config.toml``
(``~/.config``)::

    journal = "~/work/xrdwms.sqlite"
    staging = "davs://se.example.org/user/me/xrdwms"   # where large inputs go

    [poll]
    interval = 60
    max_interval = 600

    [backends.gridlab]
    kind = "arc"
    ce = "arc-ce.arctest.local"
    queue = "main"

A backend can also be named without configuring it, as ``kind:target``:
``arc:ce.example.org`` or ``arc:ce.example.org/short`` (CE and queue), and
``local`` or ``local:/scratch/jobs`` (the job root). The name is what the
journal records, so the same words find the backend again later.
"""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

from .errors import ConfigError
from .manager import PollPolicy

__all__ = ["Config", "config_path", "load_config", "resolve_backend"]


def config_path(environ: Mapping[str, str] | None = None) -> Path:
    """Where the configuration file is (whether or not it exists)."""
    env = os.environ if environ is None else environ
    if env.get("XRDWMS_CONFIG"):
        return Path(env["XRDWMS_CONFIG"])
    base = env.get("XDG_CONFIG_HOME") or os.path.join(Path.home(), ".config")
    return Path(base, "xrdwms", "config.toml")


@dataclass(frozen=True)
class Config:
    """The configuration file, read: backend tables by name, the journal, the policy."""

    backends: dict[str, dict[str, Any]] = field(default_factory=dict)
    journal: str | None = None
    policy: PollPolicy = field(default_factory=PollPolicy)
    staging: str | None = None
    path: str | None = None


def load_config(path: str | os.PathLike[str] | None = None) -> Config:
    """Read the configuration; a missing default file is an empty configuration."""
    chosen = Path(path) if path is not None else config_path()
    if not chosen.is_file():
        if path is not None:
            raise ConfigError(f"No configuration file {chosen}")
        return Config()
    try:
        data = tomllib.loads(chosen.read_text())
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{chosen}: {exc}") from exc
    backends = data.get("backends", {})
    if not isinstance(backends, dict) or not all(isinstance(v, dict) for v in backends.values()):
        raise ConfigError(f"{chosen}: [backends.<name>] must be tables")
    journal = data.get("journal")
    staging = data.get("staging")
    return Config(
        backends={name: dict(table) for name, table in backends.items()},
        journal=os.path.expanduser(journal) if isinstance(journal, str) else None,
        policy=_policy(data.get("poll", {}), chosen),
        staging=staging if isinstance(staging, str) else None,
        path=str(chosen),
    )


def _policy(table: Any, where: Path) -> PollPolicy:
    """The ``[poll]`` table as a :class:`PollPolicy`; an unknown setting is an error.

    A misspelt setting would otherwise be ignored silently and the default
    used, which is exactly the surprise a configuration file must not hold.
    """
    if not isinstance(table, dict):
        raise ConfigError(f"{where}: [poll] must be a table")
    known = {f.name for f in fields(PollPolicy)}
    unknown = sorted(set(table) - known)
    if unknown:
        raise ConfigError(f"{where}: unknown [poll] settings: {', '.join(unknown)}")
    return PollPolicy(**table)


def resolve_backend(name: str, config: Config | None = None) -> dict[str, Any]:
    """The backend table for ``name``: a configured one, or a ``kind:target`` shorthand."""
    if config is not None and name in config.backends:
        return dict(config.backends[name])
    kind, _, target = name.partition(":")
    if kind == "arc" and target:
        ce, _, queue = target.partition("/")
        return {"kind": "arc", "ce": ce, **({"queue": queue} if queue else {})}
    if kind == "local":
        return {"kind": "local", **({"root": target} if target else {})}
    if kind == "fake":
        return {"kind": "fake"}
    configured = ", ".join(sorted(config.backends)) if config and config.backends else "none"
    raise ConfigError(
        f"No backend {name!r}: configured are {configured}; or use arc:<ce>[/<queue>] or local"
    )
