"""SQLite-backed durable job store.

All state transitions are executed under a process-wide lock on a single
connection.  SQLite itself provides durability (WAL) and atomic statements;
the lock serialises the read-then-write transitions so that two concurrent
claim requests can never both receive the same job.
"""

from __future__ import annotations

import json
import secrets
import sqlite3
import threading
from contextlib import contextmanager

from .clock import format_ms, now_ms

STATUSES = ("pending", "claimed", "succeeded", "retrying", "dead_letter")

JOB_FIELDS = (
    "id",
    "queue",
    "priority",
    "payload",
    "status",
    "attempts",
    "max_attempts",
    "next_retry_at",
    "claimed_by",
    "lease_expires_at",
    "error",
    "created_at",
    "updated_at",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    queue           TEXT NOT NULL,
    priority        INTEGER NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL,
    next_retry_at   TEXT,
    claimed_by      TEXT,
    lease_expires_at TEXT,
    error           TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_queue_status ON jobs (queue, status);
CREATE INDEX IF NOT EXISTS idx_jobs_retry ON jobs (next_retry_at, status);
CREATE INDEX IF NOT EXISTS idx_jobs_order ON jobs (priority DESC, created_at ASC);
"""


class JobNotFound(Exception):
    """Raised when a job does not exist."""


class JobConflict(Exception):
    """Raised when an operation is incompatible with the job state."""


class Store:
    """Durable job storage."""

    def __init__(self, db_path: str, backoff_base_ms: int, default_max_attempts: int) -> None:
        self._lock = threading.RLock()
        self._backoff_base_ms = backoff_base_ms
        self._default_max_attempts = default_max_attempts
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            isolation_level=None,
            timeout=5.0,
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(_SCHEMA)
        self.sweep()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # Internal helpers (caller must hold the lock)
    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> dict:
        data = dict(row)
        data["payload"] = json.loads(data["payload"])
        return {field: data[field] for field in JOB_FIELDS}

    def _fetch(self, job_id: str) -> sqlite3.Row | None:
        return self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    @contextmanager
    def _transaction(self):
        """Run a read-modify-write sequence atomically.

        ``BEGIN IMMEDIATE`` also makes the sequence safe if a second process
        happens to open the same database file: SQLite serialises writers.
        """
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.execute("COMMIT")

    def _require(self, job_id: str) -> sqlite3.Row:
        row = self._fetch(job_id)
        if row is None:
            raise JobNotFound(job_id)
        return row

    def _sweep_locked(self, now_iso: str) -> None:
        # Lease expired: the worker never acked.  ``now > lease_expires_at``.
        self._conn.execute(
            """
            UPDATE jobs
               SET status = 'pending',
                   claimed_by = NULL,
                   lease_expires_at = NULL,
                   updated_at = ?
             WHERE status = 'claimed'
               AND lease_expires_at IS NOT NULL
               AND lease_expires_at < ?
            """,
            (now_iso, now_iso),
        )
        # Retry timer elapsed: retrying jobs become claimable again.
        self._conn.execute(
            """
            UPDATE jobs
               SET status = 'pending',
                   next_retry_at = NULL,
                   updated_at = ?
             WHERE status = 'retrying'
               AND next_retry_at IS NOT NULL
               AND next_retry_at <= ?
            """,
            (now_iso, now_iso),
        )

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------
    def sweep(self) -> None:
        """Apply time-based transitions (leases and retry schedules)."""
        with self._lock:
            self._sweep_locked(format_ms(now_ms()))

    def enqueue(self, queue: str, priority: int, payload: dict, max_attempts: int) -> dict:
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            timestamp = format_ms(now_ms())
            for _ in range(5):
                job_id = "job_" + secrets.token_hex(12)
                try:
                    self._conn.execute(
                        """
                        INSERT INTO jobs (
                            id, queue, priority, payload, status, attempts,
                            max_attempts, next_retry_at, claimed_by,
                            lease_expires_at, error, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, 'pending', 0, ?, NULL, NULL,
                                  NULL, NULL, ?, ?)
                        """,
                        (job_id, queue, priority, payload_json, max_attempts, timestamp, timestamp),
                    )
                    break
                except sqlite3.IntegrityError:
                    continue
            else:  # pragma: no cover - astronomically unlikely
                raise RuntimeError("could not generate a unique job id")
            return self._row_to_job(self._require(job_id))

    def get(self, job_id: str) -> dict:
        with self._lock:
            self._sweep_locked(format_ms(now_ms()))
            return self._row_to_job(self._require(job_id))

    def list_jobs(
        self,
        queue: str | None,
        status: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict], int]:
        with self._lock:
            self._sweep_locked(format_ms(now_ms()))
            clauses: list[str] = []
            params: list[object] = []
            if queue is not None:
                clauses.append("queue = ?")
                params.append(queue)
            if status is not None:
                clauses.append("status = ?")
                params.append(status)
            where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
            total = self._conn.execute(f"SELECT COUNT(*) AS n FROM jobs{where}", params).fetchone()[
                "n"
            ]
            rows = self._conn.execute(
                f"""
                SELECT * FROM jobs{where}
                 ORDER BY priority DESC, created_at ASC, rowid ASC
                 LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            ).fetchall()
            return [self._row_to_job(row) for row in rows], int(total)

    def list_dead_letter(
        self, queue: str | None, limit: int, offset: int
    ) -> tuple[list[dict], int]:
        with self._lock:
            self._sweep_locked(format_ms(now_ms()))
            params: list[object] = ["dead_letter"]
            where = " WHERE status = ?"
            if queue is not None:
                where += " AND queue = ?"
                params.append(queue)
            total = self._conn.execute(f"SELECT COUNT(*) AS n FROM jobs{where}", params).fetchone()[
                "n"
            ]
            rows = self._conn.execute(
                f"""
                SELECT * FROM jobs{where}
                 ORDER BY created_at DESC, rowid DESC
                 LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            ).fetchall()
            return [self._row_to_job(row) for row in rows], int(total)

    def claim(self, queue: str, lease_duration_ms: int) -> dict | None:
        with self._lock, self._transaction():
            now = now_ms()
            now_iso = format_ms(now)
            self._sweep_locked(now_iso)
            row = self._conn.execute(
                """
                SELECT * FROM jobs
                 WHERE queue = ?
                   AND (
                        status = 'pending'
                        OR (status = 'retrying'
                            AND next_retry_at IS NOT NULL
                            AND next_retry_at <= ?)
                   )
                 ORDER BY priority DESC, created_at ASC, rowid ASC
                 LIMIT 1
                """,
                (queue, now_iso),
            ).fetchone()
            if row is None:
                return None
            attempts = int(row["attempts"]) + 1
            claimed_by = "claimed_" + secrets.token_hex(6)
            lease_expires_at = format_ms(now + lease_duration_ms)
            self._conn.execute(
                """
                UPDATE jobs
                   SET status = 'claimed',
                       attempts = ?,
                       claimed_by = ?,
                       lease_expires_at = ?,
                       next_retry_at = NULL,
                       updated_at = ?
                 WHERE id = ?
                """,
                (attempts, claimed_by, lease_expires_at, now_iso, row["id"]),
            )
            return self._row_to_job(self._require(row["id"]))

    def ack(self, job_id: str) -> dict:
        with self._lock, self._transaction():
            now_iso = format_ms(now_ms())
            self._sweep_locked(now_iso)
            row = self._require(job_id)
            if row["status"] != "claimed":
                raise JobConflict(job_id)
            self._conn.execute(
                """
                UPDATE jobs
                   SET status = 'succeeded',
                       next_retry_at = NULL,
                       claimed_by = NULL,
                       lease_expires_at = NULL,
                       updated_at = ?
                 WHERE id = ?
                """,
                (now_iso, job_id),
            )
            return self._row_to_job(self._require(job_id))

    def fail(self, job_id: str, error: str | None) -> dict:
        with self._lock, self._transaction():
            now = now_ms()
            now_iso = format_ms(now)
            self._sweep_locked(now_iso)
            row = self._require(job_id)
            if row["status"] != "claimed":
                raise JobConflict(job_id)

            attempts = int(row["attempts"])
            max_attempts = int(row["max_attempts"])
            message = error if error is not None else row["error"]

            if attempts >= max_attempts:
                self._conn.execute(
                    """
                    UPDATE jobs
                       SET status = 'dead_letter',
                           next_retry_at = NULL,
                           claimed_by = NULL,
                           lease_expires_at = NULL,
                           error = ?,
                           updated_at = ?
                     WHERE id = ?
                    """,
                    (message, now_iso, job_id),
                )
            else:
                delay_ms = self._backoff_base_ms * (2 ** (attempts - 1))
                next_retry_at = format_ms(now + delay_ms)
                self._conn.execute(
                    """
                    UPDATE jobs
                       SET status = 'retrying',
                           next_retry_at = ?,
                           claimed_by = NULL,
                           lease_expires_at = NULL,
                           error = ?,
                           updated_at = ?
                     WHERE id = ?
                    """,
                    (next_retry_at, message, now_iso, job_id),
                )
            return self._row_to_job(self._require(job_id))
