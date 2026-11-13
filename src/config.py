"""Configuration handling for the Local Job Queue service."""

from __future__ import annotations

import os

DEFAULT_PORT = 8080
DEFAULT_DATA_DIR = "./data"
DEFAULT_BACKOFF_BASE_MS = 1000
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_LEASE_MS = 30000


class ConfigError(Exception):
    """Raised when the configuration is invalid."""


class Config:
    """Immutable configuration read once at process start."""

    def __init__(
        self,
        port: int,
        data_dir: str,
        backoff_base_ms: int,
        max_attempts: int,
        lease_ms: int,
    ) -> None:
        self.port = port
        self.data_dir = data_dir
        self.backoff_base_ms = backoff_base_ms
        self.max_attempts = max_attempts
        self.lease_ms = lease_ms

    @property
    def db_path(self) -> str:
        return os.path.join(self.data_dir, "jobs.db")


def _int_env(environ: dict[str, str], name: str, default: int, minimum: int) -> int:
    raw = environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw.strip())
    except ValueError:
        raise ConfigError(f"{name} must be an integer (got {raw!r})") from None
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum} (got {value})")
    return value


def load_config(
    environ: dict[str, str],
    port: int | None = None,
    data_dir: str | None = None,
) -> Config:
    """Build a Config from environment variables and CLI overrides."""
    if port is None:
        port = DEFAULT_PORT
    if data_dir is None:
        data_dir = environ.get("DATA_DIR") or DEFAULT_DATA_DIR
    if data_dir == "":
        data_dir = DEFAULT_DATA_DIR

    backoff_base_ms = _int_env(environ, "JOB_BACKOFF_BASE_MS", DEFAULT_BACKOFF_BASE_MS, minimum=1)
    max_attempts = _int_env(environ, "JOB_MAX_ATTEMPTS", DEFAULT_MAX_ATTEMPTS, minimum=1)
    lease_ms = _int_env(environ, "JOB_LEASE_MS", DEFAULT_LEASE_MS, minimum=1)

    return Config(
        port=port,
        data_dir=data_dir,
        backoff_base_ms=backoff_base_ms,
        max_attempts=max_attempts,
        lease_ms=lease_ms,
    )
