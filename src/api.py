"""HTTP interface for the Local Job Queue service."""

from __future__ import annotations

import json
import re
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .clock import MAX_TIMESTAMP_MS, format_ms, now_ms
from .store import JobConflict, JobNotFound, Store

MAX_PAYLOAD_BYTES = 262144
# A valid enqueue body can carry the payload plus the surrounding envelope
# (and, for non-ASCII input, JSON escapes).  Refuse anything substantially
# larger before reading it into memory.
MAX_REQUEST_BODY_BYTES = MAX_PAYLOAD_BYTES * 4
MAX_QUEUE_LENGTH = 128
MAX_ERROR_LENGTH = 1024
DEFAULT_LIMIT = 50
MAX_LIMIT = 200
# SQLite INTEGER values are signed 64-bit; binding a larger Python int raises
# OverflowError, so API inputs that reach SQL must fit in this range.
MAX_SQLITE_INTEGER = 2**63 - 1

_ROUTES = (
    (re.compile(r"^/health$"), {"GET": "health"}),
    (re.compile(r"^/jobs$"), {"GET": "list_jobs", "POST": "enqueue"}),
    (re.compile(r"^/jobs/claim$"), {"POST": "claim"}),
    (re.compile(r"^/jobs/dead-letter$"), {"GET": "dead_letter"}),
    (re.compile(r"^/jobs/([^/]+)$"), {"GET": "get_job"}),
    (re.compile(r"^/jobs/([^/]+)/ack$"), {"POST": "ack"}),
    (re.compile(r"^/jobs/([^/]+)/fail$"), {"POST": "fail"}),
)


def log_event(event: str, **fields: object) -> None:
    """Write a structured JSON log line to stdout."""
    record = {"time": format_ms(now_ms()), "event": event, **fields}
    try:
        sys.stdout.write(json.dumps(record, ensure_ascii=False) + "\n")
        sys.stdout.flush()
    except (BrokenPipeError, ValueError):  # pragma: no cover - defensive
        pass


class ApiError(Exception):
    """An error that maps to the stable JSON error envelope."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def _invalid_json(message: str = "request body must be valid JSON") -> ApiError:
    return ApiError(400, "invalid_json", message)


def clamp_limit(raw: str | None) -> int:
    if raw is None:
        return DEFAULT_LIMIT
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if value <= 0:
        return DEFAULT_LIMIT
    if value > MAX_LIMIT:
        return MAX_LIMIT
    return value


def clamp_offset(raw: str | None) -> int:
    if raw is None:
        return 0
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 0
    return min(max(0, value), MAX_SQLITE_INTEGER)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


class _RequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "LocalJobQueue/0.1.0"
    sys_version = ""

    store: Store
    default_max_attempts: int

    # -- plumbing ------------------------------------------------------
    def log_message(self, fmt: str, *args: object) -> None:
        # Access logging is intentionally disabled: the process emits only the
        # structured startup event (and retry-manager errors) to stdout.  This
        # also prevents a parent process that captures stdout via a pipe from
        # ever blocking request handling.
        return

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._dispatch("PUT")

    def do_PATCH(self) -> None:  # noqa: N802
        self._dispatch("PATCH")

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch("DELETE")

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._dispatch("OPTIONS")

    def do_HEAD(self) -> None:  # noqa: N802
        self._dispatch("HEAD")

    def do_TRACE(self) -> None:  # noqa: N802
        self._dispatch("TRACE")

    # -- request handling ----------------------------------------------
    def _dispatch(self, method: str) -> None:
        try:
            split = urlsplit(self.path)
            path = split.path
            query = parse_qs(split.query, keep_blank_values=True)
            try:
                self._body = self._read_body()
            except ApiError as exc:
                self._send_api_error(exc)
                return

            for pattern, methods in _ROUTES:
                match = pattern.match(path)
                if match is None:
                    continue
                handler_name = methods.get(method)
                if handler_name is None:
                    self._send_error(
                        405,
                        "method_not_allowed",
                        f"method {method} is not allowed for {path}",
                    )
                    return
                getattr(self, "_handle_" + handler_name)(query, match)
                return

            self._send_error(404, "not_found", f"resource {path} not found")
        except ApiError as exc:
            self._send_api_error(exc)
        except JobNotFound:
            self._send_error(404, "not_found", "job not found")
        except JobConflict:
            self._send_error(409, "conflict", "job is not in a claimed state")
        except Exception:  # pragma: no cover - defensive
            self._send_error(500, "internal_error", "unexpected internal error")

    def _read_body(self) -> bytes:
        header = self.headers.get("Content-Length")
        if header is None:
            return b""
        try:
            length = int(header)
        except (TypeError, ValueError):
            self.close_connection = True
            raise _invalid_json("invalid Content-Length header") from None
        if length < 0:
            self.close_connection = True
            raise _invalid_json("invalid Content-Length header")
        if length > MAX_REQUEST_BODY_BYTES:
            # Do not read an oversized body: reading it before validating the
            # payload would let a single request exhaust process memory.  The
            # connection is closed because the body is left unread.
            self.close_connection = True
            raise ApiError(
                413,
                "payload_too_large",
                f"request body must not exceed {MAX_REQUEST_BODY_BYTES} bytes",
            )
        return self.rfile.read(length) if length else b""

    def _has_json_content_type(self) -> bool:
        header = self.headers.get("Content-Type")
        if not header:
            return False
        media_type = header.split(";", 1)[0].strip().lower()
        return media_type == "application/json"

    def _parse_json(self, body: bytes) -> object:
        try:
            return json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise _invalid_json("request body is not valid JSON") from None

    def _require_json_object(self) -> dict:
        if not self._has_json_content_type():
            raise _invalid_json("Content-Type must be application/json")
        if not self._body.strip():
            raise _invalid_json("request body is required")
        parsed = self._parse_json(self._body)
        if not isinstance(parsed, dict):
            raise _invalid_json("request body must be a JSON object")
        return parsed

    def _optional_json_object(self) -> dict:
        if not self._body.strip():
            return {}
        if not self._has_json_content_type():
            raise _invalid_json("Content-Type must be application/json")
        parsed = self._parse_json(self._body)
        if not isinstance(parsed, dict):
            raise _invalid_json("request body must be a JSON object")
        return parsed

    def _validate_ignored_body(self) -> None:
        """Ack ignores body contents but a malformed body is still an error."""
        if not self._body.strip():
            return
        if not self._has_json_content_type():
            raise _invalid_json("Content-Type must be application/json")
        self._parse_json(self._body)

    # -- responses -----------------------------------------------------
    def _send_json(self, status: int, payload: object) -> None:
        try:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except UnicodeEncodeError:
            # Lone surrogates (legal JSON escapes) cannot be encoded directly;
            # escaping them keeps the response valid UTF-8.
            body = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_error(self, status: int, code: str, message: str) -> None:
        self._send_json(
            status,
            {"error": {"code": code, "message": message, "details": {}}},
        )

    def _send_api_error(self, exc: ApiError) -> None:
        self._send_error(exc.status, exc.code, exc.message)

    # -- handlers ------------------------------------------------------
    def _handle_health(self, query: dict, match: re.Match) -> None:
        self._send_json(200, {"status": "ok"})

    def _handle_enqueue(self, query: dict, match: re.Match) -> None:
        body = self._require_json_object()

        queue = body.get("queue")
        if not isinstance(queue, str) or queue == "" or len(queue) > MAX_QUEUE_LENGTH:
            raise ApiError(
                422,
                "invalid_queue",
                "queue must be a non-empty string of at most 128 characters",
            )

        if "payload" not in body or not isinstance(body["payload"], dict):
            raise ApiError(422, "invalid_payload", "payload must be a JSON object")
        payload = body["payload"]

        priority = body.get("priority", 10)
        if not _is_int(priority) or not 0 <= priority <= 100:
            raise ApiError(
                422,
                "invalid_priority",
                "priority must be an integer between 0 and 100",
            )

        max_attempts = body.get("max_attempts", self.default_max_attempts)
        if not _is_int(max_attempts) or not 1 <= max_attempts <= MAX_SQLITE_INTEGER:
            raise ApiError(
                422,
                "invalid_max_attempts",
                "max_attempts must be an integer between 1 and 9223372036854775807",
            )

        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        try:
            payload_size = len(serialized.encode("utf-8"))
        except UnicodeEncodeError:
            # A lone surrogate cannot be represented in UTF-8; measure its
            # ASCII-escaped form instead so it can still be judged and stored.
            payload_size = len(
                json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("ascii")
            )
        if payload_size > MAX_PAYLOAD_BYTES:
            raise ApiError(
                413,
                "payload_too_large",
                "payload must not exceed 262144 bytes when serialized",
            )

        job = self.store.enqueue(queue, priority, payload, max_attempts)
        self._send_json(202, job)

    def _handle_list_jobs(self, query: dict, match: re.Match) -> None:
        queue = query.get("queue", [None])[0]
        status = query.get("status", [None])[0]
        limit = clamp_limit(query.get("limit", [None])[0])
        offset = clamp_offset(query.get("offset", [None])[0])
        jobs, total = self.store.list_jobs(queue, status, limit, offset)
        self._send_json(200, {"jobs": jobs, "total": total})

    def _handle_get_job(self, query: dict, match: re.Match) -> None:
        job = self.store.get(match.group(1))
        self._send_json(200, job)

    def _handle_claim(self, query: dict, match: re.Match) -> None:
        body = self._require_json_object()

        queue = body.get("queue")
        if not isinstance(queue, str) or queue == "" or len(queue) > MAX_QUEUE_LENGTH:
            raise ApiError(
                422,
                "invalid_queue",
                "queue must be a non-empty string of at most 128 characters",
            )

        lease_duration_ms = body.get("lease_duration_ms")
        if (
            not _is_int(lease_duration_ms)
            or lease_duration_ms <= 0
            or now_ms() + lease_duration_ms > MAX_TIMESTAMP_MS
        ):
            raise ApiError(
                422,
                "lease_duration_required",
                "lease_duration_ms must be a positive integer within the supported range",
            )

        job = self.store.claim(queue, lease_duration_ms)
        self._send_json(200, {"job": job})

    def _handle_ack(self, query: dict, match: re.Match) -> None:
        self._validate_ignored_body()
        job = self.store.ack(match.group(1))
        self._send_json(200, job)

    def _handle_fail(self, query: dict, match: re.Match) -> None:
        body = self._optional_json_object()
        error = body.get("error")
        if not isinstance(error, str):
            error = None
        elif len(error) > MAX_ERROR_LENGTH:
            error = error[:MAX_ERROR_LENGTH]
        job = self.store.fail(match.group(1), error)
        self._send_json(200, job)

    def _handle_dead_letter(self, query: dict, match: re.Match) -> None:
        queue = query.get("queue", [None])[0]
        limit = clamp_limit(query.get("limit", [None])[0])
        offset = clamp_offset(query.get("offset", [None])[0])
        jobs, total = self.store.list_dead_letter(queue, limit, offset)
        self._send_json(200, {"jobs": jobs, "total": total})


class Server(ThreadingHTTPServer):
    daemon_threads = True
    # On Windows SO_REUSEADDR lets a second process bind a busy port, which
    # would hide the "port in use" startup failure (AC-17). Use exclusive
    # binding there; keep SO_REUSEADDR on POSIX for fast restarts.
    allow_reuse_address = sys.platform != "win32"

    def server_bind(self) -> None:
        exclusive = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)
        if sys.platform == "win32" and exclusive is not None:
            self.socket.setsockopt(socket.SOL_SOCKET, exclusive, 1)
        super().server_bind()


def make_server(host: str, port: int, store: Store, default_max_attempts: int) -> Server:
    handler = type(
        "RequestHandler",
        (_RequestHandler,),
        {"store": store, "default_max_attempts": default_max_attempts},
    )
    return Server((host, port), handler)


def start_retry_manager(
    store: Store, interval: float = 0.05
) -> tuple[threading.Event, threading.Thread]:
    """Start the background retry/lease sweep loop."""
    stop = threading.Event()

    def loop() -> None:
        while not stop.wait(interval):
            try:
                store.sweep()
            except Exception as exc:  # pragma: no cover - defensive
                log_event("retry_manager_error", error=str(exc))

    thread = threading.Thread(target=loop, name="retry-manager", daemon=True)
    thread.start()
    return stop, thread
