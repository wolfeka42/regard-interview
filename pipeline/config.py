"""The only module that reads the environment (AD-7).

Configuration is checked once per process, before any SparkSession exists.
Errors name the missing variable, never a value.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

DATA_ROOT = "DATA_ROOT"
RAW_ROOT = "RAW_ROOT"
PATIENT_KEY_HMAC_SECRET = "PATIENT_KEY_HMAC_SECRET"
REQUIRED_VARS = (DATA_ROOT, RAW_ROOT, PATIENT_KEY_HMAC_SECRET)


class ConfigError(RuntimeError):
    """A required environment variable is missing or empty."""


@dataclass(frozen=True)
class Conf:
    data_root: str
    raw_root: str
    hmac_secret: str = field(repr=False)

    def __repr__(self) -> str:
        return f"Conf(data_root={self.data_root!r}, raw_root={self.raw_root!r}, hmac_secret='***')"

    __str__ = __repr__


def require_env(name: str, environ: Mapping[str, str] | None = None) -> str:
    """Return a non-empty environment variable or raise ConfigError naming it."""
    env = os.environ if environ is None else environ
    value = env.get(name)
    if value is None or not value.strip():
        raise ConfigError(f"Required environment variable {name} is unset or empty")
    return value


def load_conf(environ: Mapping[str, str] | None = None) -> Conf:
    """Build the immutable conf; the key is required in every job, with no default."""
    data_root = require_env(DATA_ROOT, environ)
    raw_root = require_env(RAW_ROOT, environ)
    secret = require_env(PATIENT_KEY_HMAC_SECRET, environ)
    if secret != secret.strip():
        # Never strip silently: keys would differ between environments.
        raise ConfigError(
            f"Environment variable {PATIENT_KEY_HMAC_SECRET} has leading or trailing whitespace"
        )
    return Conf(
        data_root=data_root.rstrip("/"),
        raw_root=raw_root.rstrip("/"),
        hmac_secret=secret,
    )
