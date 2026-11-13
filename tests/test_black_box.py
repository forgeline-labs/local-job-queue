"""Black-box acceptance tests for Local Job Queue v0.1.

These tests only use the public HTTP interface: every test starts the real
``./serve`` executable in a subprocess with its own temporary data directory
and an ephemeral port.  No module from ``src/`` is imported.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVE = os.path.join(ROOT, "serve")

BASE_ENV = {
    "JOB_BACKOFF_BASE_MS": "50",
    "JOB_MAX_ATTEMPTS": "3",
    "JOB_LEASE_MS": "500",
}


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Service:
    """A running ./serve subprocess."""

    def __init__(self, data_dir: str | None = None, env: dict | None = None) -> None:
        self.data_dir = data_dir or tempfile.mkdtemp(prefix="ljq-test-")
        self.env_overrides = dict(env or {})
        self.proc: subprocess.Popen | None = None
        self.port: int | None = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def _env(self) -> dict:
        env = os.environ.copy()
        env.pop("JOB_CURRENT_TIME_MS", None)
        env.update(BASE_ENV)
        env["DATA_DIR"] = self.data_dir
        env.update(self.env_overrides)
        return env

    def start(self) -> None:
        self.port = free_port()
        self.proc = subprocess.Popen(
            [sys.executable, SERVE, "--port", str(self.port), "--data-dir", self.data_dir],
            cwd=ROOT,
            env=self._env(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if self.proc.poll() is not None:
                stderr = self.proc.stderr.read().decode() if self.proc.stderr else ""
                raise RuntimeError(f"server exited early: {stderr}")
            try:
                status, body, _ = self.request("GET", "/health", timeout=0.25)
                if status == 200 and body == {"status": "ok"}:
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.02)
        raise RuntimeError("server did not become healthy within 5 seconds")

    def restart(self, env: dict | None = None) -> None:
        self.stop()
        if env is not None:
            self.env_overrides = dict(env)
        self.start()

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            if sig == getattr(signal, "SIGKILL", None):
                self.proc.kill()
            else:
                self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:  # pragma: no cover - defensive
                self.proc.kill()
                self.proc.wait(timeout=5)
        if self.proc.stderr is not None:
            self.proc.stderr.close()
        self.proc = None

    def cleanup(self) -> None:
        self.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def request(
        self,
        method: str,
        path: str,
        body: object | None = None,
        raw: bytes | None = None,
        content_type: str | None = "application/json",
        headers: dict | None = None,
        timeout: float = 5.0,
    ) -> tuple[int, object, object]:
        if raw is None and body is not None:
            raw = json.dumps(body).encode("utf-8")
        request = urllib.request.Request(self.base_url + path, data=raw, method=method)
        if raw is not None and content_type is not None:
            request.add_header("Content-Type", content_type)
        for name, value in (headers or {}).items():
            request.add_header(name, value)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = response.read()
                parsed = json.loads(payload) if payload else None
                return response.status, parsed, response.headers
        except urllib.error.HTTPError as error:
            payload = error.read()
            parsed = json.loads(payload) if payload else None
            return error.code, parsed, error.headers

    # -- convenience ---------------------------------------------------
    def enqueue(
        self,
        queue: str = "q",
        priority: int = 10,
        payload: dict | None = None,
        max_attempts: int | None = None,
    ) -> dict:
        body: dict = {"queue": queue, "priority": priority, "payload": payload or {}}
        if max_attempts is not None:
            body["max_attempts"] = max_attempts
        status, job, _ = self.request("POST", "/jobs", body=body)
        assert status == 202, (status, job)
        return job

    def claim(self, queue: str = "q", lease_duration_ms: int = 500) -> dict | None:
        status, body, _ = self.request(
            "POST",
            "/jobs/claim",
            body={"queue": queue, "lease_duration_ms": lease_duration_ms},
        )
        assert status == 200, (status, body)
        return body["job"]

    def fail(self, job_id: str, error: str | None = None) -> dict:
        payload = {"error": error} if error is not None else {}
        status, job, _ = self.request("POST", f"/jobs/{job_id}/fail", body=payload)
        assert status == 200, (status, job)
        return job

    def ack(self, job_id: str) -> dict:
        status, job, _ = self.request("POST", f"/jobs/{job_id}/ack")
        assert status == 200, (status, job)
        return job

    def get(self, job_id: str) -> dict:
        status, job, _ = self.request("GET", f"/jobs/{job_id}")
        assert status == 200, (status, job)
        return job

    def wait_for_retry(self, queue: str = "q", timeout: float = 3.0) -> dict | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = self.claim(queue)
            if job is not None:
                return job
            time.sleep(0.02)
        return None


class BlackBoxTest(unittest.TestCase):
    def setUp(self) -> None:
        self.services: list[Service] = []

    def tearDown(self) -> None:
        for service in self.services:
            service.cleanup()

    def start(self, env: dict | None = None, data_dir: str | None = None) -> Service:
        service = Service(data_dir=data_dir, env=env)
        self.services.append(service)
        service.start()
        return service

    # -- AC-1 ----------------------------------------------------------
    def test_ac01_health_and_readiness(self) -> None:
        started = time.time()
        service = self.start()
        elapsed = time.time() - started
        self.assertLess(elapsed, 5.0)
        status, body, headers = service.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "ok"})
        self.assertEqual(headers.get("Content-Type"), "application/json; charset=utf-8")

    # -- AC-2 ----------------------------------------------------------
    def test_ac02_enqueue_and_claim(self) -> None:
        service = self.start()
        job = service.enqueue(queue="email", priority=7, payload={"to": "a@b.c"})
        self.assertRegex(job["id"], r"^job_[0-9a-f]{24}$")
        self.assertEqual(job["queue"], "email")
        self.assertEqual(job["priority"], 7)
        self.assertEqual(job["payload"], {"to": "a@b.c"})
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attempts"], 0)
        self.assertEqual(job["max_attempts"], 3)
        self.assertIsNone(job["next_retry_at"])
        self.assertIsNone(job["claimed_by"])
        self.assertIsNone(job["lease_expires_at"])
        self.assertIsNone(job["error"])

        claimed = service.claim("email")
        assert claimed is not None
        self.assertEqual(claimed["id"], job["id"])
        self.assertEqual(claimed["status"], "claimed")
        self.assertEqual(claimed["attempts"], 1)
        self.assertIsNotNone(claimed["claimed_by"])
        self.assertIsNotNone(claimed["lease_expires_at"])
        self.assertIsNone(claimed["next_retry_at"])

    # -- AC-3 ----------------------------------------------------------
    def test_ac03_ack(self) -> None:
        service = self.start()
        job = service.enqueue()
        service.claim()
        acked = service.ack(job["id"])
        self.assertEqual(acked["status"], "succeeded")
        self.assertIsNone(acked["lease_expires_at"])
        self.assertIsNone(acked["claimed_by"])
        self.assertIsNone(acked["next_retry_at"])
        self.assertEqual(service.get(job["id"])["status"], "succeeded")

    # -- AC-4 ----------------------------------------------------------
    def test_ac04_fail_schedules_retry(self) -> None:
        service = self.start()
        job = service.enqueue(max_attempts=3)
        service.claim()
        failed = service.fail(job["id"])
        self.assertEqual(failed["status"], "retrying")
        self.assertIsNotNone(failed["next_retry_at"])
        self.assertIsNone(failed["claimed_by"])
        self.assertIsNone(failed["lease_expires_at"])

    # -- AC-5 ----------------------------------------------------------
    def test_ac05_retry_becomes_pending(self) -> None:
        service = self.start(env={"JOB_CURRENT_TIME_MS": "1000"})
        job = service.enqueue(max_attempts=3)
        service.claim()
        failed = service.fail(job["id"])
        self.assertEqual(failed["status"], "retrying")
        self.assertEqual(failed["next_retry_at"], "1970-01-01T00:00:01.050Z")

        service.restart(env={"JOB_CURRENT_TIME_MS": "1050"})
        self.assertEqual(service.get(job["id"])["status"], "pending")
        claimed = service.claim()
        assert claimed is not None
        self.assertEqual(claimed["id"], job["id"])
        self.assertEqual(claimed["attempts"], 2)

    # -- AC-6 ----------------------------------------------------------
    def test_ac06_backoff_is_exponential(self) -> None:
        service = self.start(env={"JOB_CURRENT_TIME_MS": "1000"})
        job = service.enqueue(max_attempts=5)

        service.claim()
        first = service.fail(job["id"])
        self.assertEqual(first["next_retry_at"], "1970-01-01T00:00:01.050Z")

        service.restart(env={"JOB_CURRENT_TIME_MS": "1050"})
        service.claim()
        second = service.fail(job["id"])
        self.assertEqual(second["next_retry_at"], "1970-01-01T00:00:01.150Z")

        service.restart(env={"JOB_CURRENT_TIME_MS": "1150"})
        service.claim()
        third = service.fail(job["id"])
        self.assertEqual(third["next_retry_at"], "1970-01-01T00:00:01.350Z")

    # -- AC-7 ----------------------------------------------------------
    def test_ac07_dead_letter(self) -> None:
        service = self.start()
        job = service.enqueue(max_attempts=1)
        service.claim()
        failed = service.fail(job["id"], error="fatal")
        self.assertEqual(failed["status"], "dead_letter")
        self.assertIsNone(failed["next_retry_at"])
        self.assertEqual(failed["error"], "fatal")

        status, body, _ = service.request("GET", "/jobs/dead-letter")
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["jobs"][0]["id"], job["id"])
        self.assertEqual(body["jobs"][0]["status"], "dead_letter")

    # -- AC-8 ----------------------------------------------------------
    def test_ac08_filtering_and_limit(self) -> None:
        service = self.start()
        first = service.enqueue(queue="alpha", priority=50)
        second = service.enqueue(queue="alpha", priority=20)
        third = service.enqueue(queue="beta", priority=10)
        service.claim("alpha")

        status, body, _ = service.request("GET", "/jobs?queue=alpha&status=pending")
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["jobs"][0]["id"], second["id"])

        status, body, _ = service.request("GET", "/jobs?queue=alpha&status=claimed")
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["jobs"][0]["id"], first["id"])

        status, body, _ = service.request("GET", "/jobs?limit=2")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["jobs"]), 2)
        self.assertEqual(body["total"], 3)
        self.assertEqual(body["jobs"][0]["id"], first["id"])

        status, body, _ = service.request("GET", "/jobs?queue=beta")
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["jobs"][0]["id"], third["id"])

    # -- AC-9 ----------------------------------------------------------
    def test_ac09_expired_lease_is_reclaimed(self) -> None:
        service = self.start(env={"JOB_CURRENT_TIME_MS": "1000"})
        job = service.enqueue()
        claimed = service.claim()
        assert claimed is not None
        self.assertEqual(claimed["id"], job["id"])

        service.restart(env={"JOB_CURRENT_TIME_MS": "1501"})
        reclaimed = service.claim()
        assert reclaimed is not None
        self.assertEqual(reclaimed["id"], job["id"])
        self.assertEqual(reclaimed["attempts"], 2)

    # -- AC-10 ---------------------------------------------------------
    def test_ac10_priority_then_fifo(self) -> None:
        service = self.start()
        low = service.enqueue(priority=10)
        high = service.enqueue(priority=50)
        claimed = service.claim()
        assert claimed is not None
        self.assertEqual(claimed["id"], high["id"])

        same_a = service.enqueue(priority=30)
        same_b = service.enqueue(priority=30)
        order = [service.claim()["id"], service.claim()["id"]]
        self.assertEqual(order, [same_a["id"], same_b["id"]])
        self.assertNotEqual(low["id"], high["id"])

    # -- AC-11 ---------------------------------------------------------
    def test_ac11_sigkill_preserves_retry_and_attempts(self) -> None:
        service = self.start()
        job = service.enqueue(max_attempts=3)
        service.claim()
        service.fail(job["id"])
        service.stop(sig=getattr(signal, "SIGKILL", signal.SIGTERM))

        service.start()
        reclaimed = service.wait_for_retry(timeout=3.0)
        assert reclaimed is not None
        self.assertEqual(reclaimed["id"], job["id"])
        self.assertEqual(reclaimed["attempts"], 2)
        self.assertEqual(service.get(job["id"])["attempts"], 2)

    # -- AC-12 ---------------------------------------------------------
    def test_ac12_lease_expiry_with_injected_clock(self) -> None:
        service = self.start(env={"JOB_CURRENT_TIME_MS": "1000"})
        job = service.enqueue()
        claimed = service.claim(lease_duration_ms=500)
        assert claimed is not None
        self.assertEqual(claimed["lease_expires_at"], "1970-01-01T00:00:01.500Z")

        service.restart(env={"JOB_CURRENT_TIME_MS": "1500"})
        self.assertEqual(service.get(job["id"])["status"], "claimed")

        service.restart(env={"JOB_CURRENT_TIME_MS": "1501"})
        self.assertEqual(service.get(job["id"])["status"], "pending")

    # -- AC-13 ---------------------------------------------------------
    def test_ac13_claim_empty_queue_returns_null(self) -> None:
        service = self.start()
        status, body, _ = service.request(
            "POST", "/jobs/claim", body={"queue": "empty", "lease_duration_ms": 500}
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, {"job": None})

    # -- AC-14 ---------------------------------------------------------
    def test_ac14_concurrent_claims_only_one_wins(self) -> None:
        service = self.start()
        service.enqueue()
        results: list[dict | None] = []

        def claim() -> None:
            results.append(service.claim())

        threads = [threading.Thread(target=claim) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        claimed = [result for result in results if result is not None]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(len(results), 2)

    # -- AC-15 ---------------------------------------------------------
    def test_ac15_attempts_increment_each_cycle(self) -> None:
        service = self.start(env={"JOB_CURRENT_TIME_MS": "1000"})
        job = service.enqueue(max_attempts=5)

        claimed = service.claim()
        self.assertEqual(claimed["attempts"], 1)
        service.fail(job["id"])

        service.restart(env={"JOB_CURRENT_TIME_MS": "1050"})
        claimed = service.claim()
        self.assertEqual(claimed["attempts"], 2)
        service.fail(job["id"])

        service.restart(env={"JOB_CURRENT_TIME_MS": "1150"})
        claimed = service.claim()
        self.assertEqual(claimed["attempts"], 3)

    # -- AC-16 ---------------------------------------------------------
    def test_ac16_all_errors_use_envelope(self) -> None:
        service = self.start()
        job = service.enqueue()

        cases = [
            ("GET", "/nope", None, "not_found", 404),
            ("PUT", "/jobs", None, "method_not_allowed", 405),
            ("POST", "/jobs", b"{", "invalid_json", 400),
            ("POST", "/jobs", json.dumps({"payload": {}}).encode(), "invalid_queue", 422),
            ("POST", f"/jobs/{job['id']}/ack", None, "conflict", 409),
            ("GET", "/jobs/job_does_not_exist", None, "not_found", 404),
        ]
        for method, path, raw, code, expected_status in cases:
            status, body, headers = service.request(method, path, raw=raw)
            self.assertEqual(status, expected_status, (method, path, body))
            self.assertEqual(headers.get("Content-Type"), "application/json; charset=utf-8")
            self.assertIn("error", body)
            self.assertEqual(body["error"]["code"], code)
            self.assertIsInstance(body["error"]["message"], str)
            self.assertIsInstance(body["error"]["details"], dict)

    # -- AC-17 ---------------------------------------------------------
    def test_ac17_startup_failures_exit_1(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as blocker:
            blocker.bind(("127.0.0.1", 0))
            blocker.listen(1)
            busy_port = blocker.getsockname()[1]
            env = os.environ.copy()
            env.update(BASE_ENV)
            env["DATA_DIR"] = tempfile.mkdtemp(prefix="ljq-busy-")
            try:
                result = subprocess.run(
                    [sys.executable, SERVE, "--port", str(busy_port)],
                    cwd=ROOT,
                    env=env,
                    capture_output=True,
                    timeout=10,
                )
            finally:
                shutil.rmtree(env["DATA_DIR"], ignore_errors=True)
            self.assertEqual(result.returncode, 1)
            self.assertNotEqual(result.stderr.decode().strip(), "")

        bad_env = os.environ.copy()
        bad_env.update(BASE_ENV)
        bad_env["JOB_MAX_ATTEMPTS"] = "0"
        bad_env["DATA_DIR"] = tempfile.mkdtemp(prefix="ljq-badenv-")
        try:
            result = subprocess.run(
                [sys.executable, SERVE, "--port", str(free_port())],
                cwd=ROOT,
                env=bad_env,
                capture_output=True,
                timeout=10,
            )
        finally:
            shutil.rmtree(bad_env["DATA_DIR"], ignore_errors=True)
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr.decode().strip(), "")

    # -- AC-18 ---------------------------------------------------------
    def test_ac18_pagination_clamping(self) -> None:
        service = self.start(env={"JOB_CURRENT_TIME_MS": "1000"})
        jobs = [service.enqueue(priority=index, max_attempts=1) for index in range(3)]
        for _ in jobs:
            claimed = service.claim()
            assert claimed is not None
            service.fail(claimed["id"])

        for query in ("limit=0", "limit=500", "offset=-5"):
            status, body, _ = service.request("GET", f"/jobs?{query}")
            self.assertEqual(status, 200)
            self.assertEqual(body["total"], 3)
            self.assertEqual(len(body["jobs"]), 3)

        status, body, _ = service.request("GET", "/jobs/dead-letter?limit=0")
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 3)
        status, body, _ = service.request("GET", "/jobs/dead-letter?limit=1&offset=1")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["jobs"]), 1)
        self.assertEqual(body["total"], 3)

    # -- AC-19 ---------------------------------------------------------
    def test_ac19_error_is_persisted(self) -> None:
        service = self.start()
        job = service.enqueue(max_attempts=3)
        service.claim()
        service.fail(job["id"], error="SMTP connection timeout")
        fetched = service.get(job["id"])
        self.assertEqual(fetched["error"], "SMTP connection timeout")
        self.assertEqual(fetched["status"], "retrying")

    # -- AC-20 ---------------------------------------------------------
    def test_ac20_no_jobs_are_skipped_by_priority(self) -> None:
        service = self.start()
        urgent = service.enqueue(priority=90)
        normal = [service.enqueue(priority=50) for _ in range(10)]

        first = service.claim()
        assert first is not None
        self.assertEqual(first["id"], urgent["id"])

        second = service.claim()
        assert second is not None
        self.assertEqual(second["id"], normal[0]["id"])


class AdditionalSpecTest(unittest.TestCase):
    """Extra coverage for the remaining edge cases in section 7."""

    def setUp(self) -> None:
        self.services: list[Service] = []

    def tearDown(self) -> None:
        for service in self.services:
            service.cleanup()

    def start(self, env: dict | None = None) -> Service:
        service = Service(env=env)
        self.services.append(service)
        service.start()
        return service

    def test_validation_edges(self) -> None:
        service = self.start()
        payloads = [
            ({"queue": "", "payload": {}}, "invalid_queue"),
            ({"queue": "q" * 129, "payload": {}}, "invalid_queue"),
            ({"queue": "q", "payload": []}, "invalid_payload"),
            ({"queue": "q", "payload": "x"}, "invalid_payload"),
            ({"queue": "q", "payload": 1}, "invalid_payload"),
            ({"queue": "q", "priority": 101, "payload": {}}, "invalid_priority"),
            ({"queue": "q", "priority": -1, "payload": {}}, "invalid_priority"),
            ({"queue": "q", "max_attempts": 0, "payload": {}}, "invalid_max_attempts"),
        ]
        for body, code in payloads:
            status, response, _ = service.request("POST", "/jobs", body=body)
            self.assertEqual(status, 422, (body, response))
            self.assertEqual(response["error"]["code"], code)

        status, job, _ = service.request("POST", "/jobs", body={"queue": "q", "payload": {}})
        self.assertEqual(status, 202)
        self.assertEqual(job["priority"], 10)
        self.assertEqual(job["max_attempts"], 3)

    def test_claim_validation_edges(self) -> None:
        service = self.start()
        status, body, _ = service.request("POST", "/jobs/claim", body={"lease_duration_ms": 500})
        self.assertEqual((status, body["error"]["code"]), (422, "invalid_queue"))

        for lease in (None, 0, -1):
            request = {"queue": "q"}
            if lease is not None:
                request["lease_duration_ms"] = lease
            status, body, _ = service.request("POST", "/jobs/claim", body=request)
            self.assertEqual((status, body["error"]["code"]), (422, "lease_duration_required"))

    def test_payload_size_limit(self) -> None:
        service = self.start()
        base = len(json.dumps({"a": ""}, separators=(",", ":")))
        accepted = "x" * (262144 - base)
        job = service.enqueue(payload={"a": accepted})
        self.assertEqual(job["status"], "pending")

        rejected = "x" * (262144 - base + 1)
        status, body, _ = service.request(
            "POST", "/jobs", body={"queue": "q", "payload": {"a": rejected}}
        )
        self.assertEqual((status, body["error"]["code"]), (413, "payload_too_large"))
        status, listing, _ = service.request("GET", "/jobs")
        self.assertEqual(listing["total"], 1)

    def test_content_type_is_required(self) -> None:
        service = self.start()
        status, body, _ = service.request(
            "POST", "/jobs", raw=b'{"queue":"q","payload":{}}', content_type=None
        )
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_json"))

    def test_max_attempts_override(self) -> None:
        service = self.start()
        job = service.enqueue(max_attempts=9)
        self.assertEqual(job["max_attempts"], 9)

    # -- review regressions (round 1) ---------------------------------
    def test_oversized_content_length_is_rejected_before_reading(self) -> None:
        service = self.start()
        with socket.create_connection(("127.0.0.1", service.port), timeout=5) as sock:
            sock.sendall(
                b"POST /jobs HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: 500000000\r\n\r\n"
            )
            response = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response += chunk
        head, _, body = response.partition(b"\r\n\r\n")
        self.assertIn(b" 413 ", head.splitlines()[0])
        self.assertEqual(json.loads(body)["error"]["code"], "payload_too_large")

    def test_lone_surrogate_payload_round_trips(self) -> None:
        service = self.start()
        raw = b'{"queue":"q","payload":{"x":"\\ud800"}}'
        status, job, _ = service.request("POST", "/jobs", raw=raw)
        self.assertEqual(status, 202)
        self.assertEqual(job["payload"], {"x": "\ud800"})

        status, fetched, _ = service.request("GET", f"/jobs/{job['id']}")
        self.assertEqual(status, 200)
        self.assertEqual(fetched["payload"], {"x": "\ud800"})

    def test_huge_max_attempts_is_rejected(self) -> None:
        service = self.start()
        body = {"queue": "q", "payload": {}, "max_attempts": 100000000000000000000}
        status, response, _ = service.request("POST", "/jobs", body=body)
        self.assertEqual((status, response["error"]["code"]), (422, "invalid_max_attempts"))

    def test_huge_offset_is_clamped(self) -> None:
        service = self.start()
        service.enqueue()
        status, body, _ = service.request("GET", "/jobs?offset=99999999999999999999999")
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["jobs"], [])

        status, body, _ = service.request("GET", "/jobs/dead-letter?offset=99999999999999999999999")
        self.assertEqual(status, 200)
        self.assertEqual(body["jobs"], [])

    def test_huge_lease_duration_is_rejected(self) -> None:
        service = self.start()
        status, body, _ = service.request(
            "POST",
            "/jobs/claim",
            body={"queue": "q", "lease_duration_ms": 100000000000000000},
        )
        self.assertEqual((status, body["error"]["code"]), (422, "lease_duration_required"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
