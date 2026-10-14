"""The only module that reads the environment (AD-7).

Configuration is checked once per process, before any SparkSession exists.
Errors name the variable, never a value.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

from pipeline.schemas import DEFAULT_SALT

DATA_ROOT = "DATA_ROOT"
PATIENT_ID_SALT = "PATIENT_ID_SALT"


class ConfigError(RuntimeError):
    """A required environment variable is missing, or a variable is malformed."""


@dataclass(frozen=True)
class Conf:
    data_root: str
    salt: str = field(repr=False)

    def __repr__(self) -> str:
        return f"Conf(data_root={self.data_root!r}, salt='***')"

    __str__ = __repr__


def require_env(name: str, environ: Mapping[str, str] | None = None) -> str:
    """Return a non-empty environment variable or raise ConfigError naming it."""
    env = os.environ if environ is None else environ
    value = env.get(name)
    if value is None or not value.strip():
        raise ConfigError(f"Required environment variable {name} is unset or empty")
    return value


def load_salt(environ: Mapping[str, str] | None = None) -> str:
    """PATIENT_ID_SALT if set, else DEFAULT_SALT. Set-but-blank or padded fails."""
    env = os.environ if environ is None else environ
    salt = env.get(PATIENT_ID_SALT)
    if salt is None:
        return DEFAULT_SALT
    if not salt.strip():
        raise ConfigError(f"Environment variable {PATIENT_ID_SALT} is set but empty")
    if salt != salt.strip():
        # Never strip silently: hashes would differ between environments.
        raise ConfigError(
            f"Environment variable {PATIENT_ID_SALT} has leading or trailing whitespace"
        )
    return salt


def load_conf(environ: Mapping[str, str] | None = None) -> Conf:
    """Build the immutable conf; DATA_ROOT is required, the salt optional."""
    data_root = require_env(DATA_ROOT, environ)
    return Conf(data_root=data_root.strip().rstrip("/"), salt=load_salt(environ))
