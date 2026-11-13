"""Command-line entrypoint for the Local Job Queue service."""

from __future__ import annotations

import os
import sys

from .api import log_event, make_server, start_retry_manager
from .config import ConfigError, load_config
from .store import Store

USAGE = """Usage: ./serve [options]

Options:
  --port N         TCP port to listen on (1..65535, default 8080)
  --data-dir PATH  Directory for the SQLite database (overrides DATA_DIR)
  -h, --help       Show this help message and exit

Environment:
  DATA_DIR             Directory for jobs.db (default: ./data)
  JOB_BACKOFF_BASE_MS  Exponential backoff base in ms (default: 1000)
  JOB_MAX_ATTEMPTS     Default maximum attempts per job (default: 5)
  JOB_LEASE_MS         Default lease duration in ms (default: 30000)
  JOB_CURRENT_TIME_MS  Fixed clock in Unix ms for deterministic tests
"""


class UsageError(Exception):
    """Raised for invalid command-line arguments."""


def parse_args(argv: list[str]) -> dict:
    port: int | None = None
    data_dir: str | None = None
    help_requested = False

    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg in ("-h", "--help"):
            help_requested = True
            index += 1
        elif arg == "--port" or arg.startswith("--port="):
            if arg == "--port":
                index += 1
                if index >= len(argv):
                    raise UsageError("--port requires a value")
                raw = argv[index]
            else:
                raw = arg.split("=", 1)[1]
            try:
                port = int(raw)
            except ValueError:
                raise UsageError(f"--port must be an integer (got {raw!r})") from None
            if not 1 <= port <= 65535:
                raise UsageError(f"--port must be between 1 and 65535 (got {port})")
            index += 1
        elif arg == "--data-dir" or arg.startswith("--data-dir="):
            if arg == "--data-dir":
                index += 1
                if index >= len(argv):
                    raise UsageError("--data-dir requires a value")
                data_dir = argv[index]
            else:
                data_dir = arg.split("=", 1)[1]
            index += 1
        else:
            raise UsageError(f"unrecognized argument: {arg}")

    return {"port": port, "data_dir": data_dir, "help": help_requested}


def main(argv: list[str]) -> int:
    try:
        options = parse_args(argv)
    except UsageError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1

    if options["help"]:
        sys.stdout.write(USAGE)
        return 0

    try:
        config = load_config(
            os.environ,
            port=options["port"],
            data_dir=options["data_dir"],
        )
    except ConfigError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1

    try:
        os.makedirs(config.data_dir, exist_ok=True)
    except OSError as exc:
        sys.stderr.write(f"error: cannot create data directory {config.data_dir!r}: {exc}\n")
        return 1

    if not os.path.isdir(config.data_dir):
        sys.stderr.write(f"error: data directory {config.data_dir!r} is not a directory\n")
        return 1

    try:
        store = Store(
            config.db_path,
            backoff_base_ms=config.backoff_base_ms,
            default_max_attempts=config.max_attempts,
        )
    except Exception as exc:
        sys.stderr.write(f"error: cannot open database in {config.data_dir!r}: {exc}\n")
        return 1

    try:
        server = make_server(
            "0.0.0.0",
            config.port,
            store,
            default_max_attempts=config.max_attempts,
        )
    except OSError as exc:
        sys.stderr.write(f"error: cannot listen on port {config.port}: {exc}\n")
        store.close()
        return 1

    stop_retry, retry_thread = start_retry_manager(store)
    log_event(
        "server_started",
        port=config.port,
        data_dir=config.data_dir,
        backoff_base_ms=config.backoff_base_ms,
        max_attempts=config.max_attempts,
        lease_ms=config.lease_ms,
    )

    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:  # pragma: no cover - interactive use
        pass
    finally:
        stop_retry.set()
        retry_thread.join(timeout=1.0)
        server.server_close()
        store.close()

    return 0
