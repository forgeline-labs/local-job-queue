"""Clock helpers.

Timestamps are stored and emitted as RFC 3339 UTC strings with millisecond
precision.  ``JOB_CURRENT_TIME_MS`` is read on every operation (not cached at
startup) so that a deterministic clock can be injected for tests.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

_TIME_ENV = "JOB_CURRENT_TIME_MS"

# Largest representable millisecond timestamp: 9999-12-31T23:59:59.999Z.
MAX_TIMESTAMP_MS = 253402300799999


def now_ms() -> int:
    """Return the current time in Unix milliseconds.

    When ``JOB_CURRENT_TIME_MS`` is defined it wins over the system clock.
    The variable is intentionally re-read on each call.
    """
    raw = os.environ.get(_TIME_ENV)
    if raw is not None and raw.strip() != "":
        try:
            return int(raw.strip())
        except ValueError:
            # An invalid injected clock is treated as absent rather than
            # crashing request handling.
            pass
    return int(time.time() * 1000)


def format_ms(ms: int) -> str:
    """Format epoch milliseconds as ``YYYY-MM-DDTHH:MM:SS.mmmZ``."""
    seconds, millis = divmod(ms, 1000)
    moment = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}Z"
