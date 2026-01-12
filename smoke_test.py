#!/usr/bin/env python3
"""
TimrX Anonymous Mode Smoke Tests

Tests:
  1. Health: /api/health
  2. Jobs: create text-to-3d job, poll status
  3. History: list, add item, update, delete

Usage:
  # Local (with .env or env vars)
  python smoke_test.py

  # Against Render
  API_BASE=https://timrx-3d-print.onrender.com python smoke_test.py

  # Verbose mode
  VERBOSE=1 python smoke_test.py

Requirements:
  pip install requests python-dotenv
"""

import os
import sys
import uuid
import requests
from typing import Optional, Tuple
from dataclasses import dataclass

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configuration
API_BASE = os.getenv("API_BASE", "http://localhost:5001")
VERBOSE = os.getenv("VERBOSE", "").lower() in ("1", "true", "yes")

RUN_ID = uuid.uuid4().hex[:8]

# Colors for output
class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

def log(msg: str, color: str = ""):
    """Print with optional color."""
    if color:
        print(f"{color}{msg}{Colors.RESET}")
    else:
        print(msg)

def log_verbose(msg: str):
    """Print only in verbose mode."""
    if VERBOSE:
        print(f"  {Colors.BLUE}[DEBUG]{Colors.RESET} {msg}")

def log_pass(name: str):
    log(f"  ✓ {name}", Colors.GREEN)

def log_fail(name: str, reason: str = ""):
    msg = f"  ✗ {name}"
    if reason:
        msg += f": {reason}"
    log(msg, Colors.RED)

def log_section(name: str):
    log(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")
    log(f"{Colors.BOLD}{name}{Colors.RESET}")
    log(f"{Colors.BOLD}{'='*60}{Colors.RESET}")


@dataclass
class TestSession:
    """Holds session state for a test run."""
    email: str
    password: str
    session: requests.Session
    user_id: Optional[str] = None
    cookies: dict = None

    def __post_init__(self):
        self.cookies = {}


class SmokeTests:
    """Smoke test runner."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.session: Optional[TestSession] = None
        self.job_id: Optional[str] = None
        self.history_id: Optional[str] = None

    def api(self, method: str, path: str, session: requests.Session = None,
            json_data: dict = None, expected_status: int = None) -> Tuple[int, dict]:
        """Make API request and return (status_code, json_response)."""
        url = f"{API_BASE}{path}"
        sess = session or requests.Session()

        try:
            if method.upper() == "GET":
                resp = sess.get(url, timeout=30)
            elif method.upper() == "POST":
                resp = sess.post(url, json=json_data, timeout=30)
            elif method.upper() == "PATCH":
                resp = sess.patch(url, json=json_data, timeout=30)
            elif method.upper() == "DELETE":
                resp = sess.delete(url, timeout=30)
            else:
                raise ValueError(f"Unknown method: {method}")

            log_verbose(f"{method} {path} -> {resp.status_code}")

            try:
                data = resp.json()
            except:
                data = {"_raw": resp.text}

            if expected_status and resp.status_code != expected_status:
                log_verbose(f"Expected {expected_status}, got {resp.status_code}: {data}")

            return resp.status_code, data

        except requests.exceptions.RequestException as e:
            log_verbose(f"Request failed: {e}")
            return 0, {"error": str(e)}

    def assert_test(self, name: str, condition: bool, fail_msg: str = "") -> bool:
        """Assert a test condition."""
        if condition:
            log_pass(name)
            self.passed += 1
            return True
        else:
            log_fail(name, fail_msg)
            self.failed += 1
            return False

    def test_health(self):
        """Test API health endpoint."""
        status, data = self.api("GET", "/api/health")
        self.assert_test("Health check", status == 200, f"status={status}")

    # =========================================================================
    # JOB TESTS
    # =========================================================================

    def test_create_job(self, ts: TestSession) -> Optional[str]:
        """Test creating a text-to-3d job."""
        status, data = self.api("POST", "/api/text-to-3d/start", ts.session, {
            "prompt": f"Test model {RUN_ID}",
            "art_style": "realistic"
        })

        success = status == 200 and "job_id" in data
        self.assert_test("Create text-to-3d job", success,
                        f"status={status}, data={data}")

        if success:
            return data["job_id"]
        return None

    def test_poll_status(self, ts: TestSession, job_id: str) -> bool:
        """Test polling job status."""
        status, data = self.api("GET", f"/api/text-to-3d/status/{job_id}", ts.session)

        # Job should be pending or in_progress
        success = status == 200 and "status" in data
        self.assert_test("Poll job status", success,
                        f"status={status}, job_status={data.get('status')}")
        return success

    # =========================================================================
    # HISTORY TESTS
    # =========================================================================

    def test_list_history(self, ts: TestSession) -> int:
        """Test listing history items. Returns count."""
        status, data = self.api("GET", "/api/history", ts.session)

        success = status == 200 and isinstance(data, list)
        self.assert_test("List history", success,
                        f"status={status}")

        if success:
            log_verbose(f"History has {len(data)} items")
            return len(data)
        return -1

    def test_add_history_item(self, ts: TestSession) -> Optional[str]:
        """Test adding a history item."""
        item_id = str(uuid.uuid4())
        status, data = self.api("POST", "/api/history/item", ts.session, {
            "id": item_id,
            "type": "model",
            "status": "done",
            "title": f"Test Model {RUN_ID}",
            "prompt": "A test 3D model"
        })

        success = status == 200 or status == 201
        self.assert_test("Add history item", success,
                        f"status={status}")

        if success:
            return item_id
        return None

    def test_update_history_item(self, ts: TestSession, item_id: str) -> bool:
        """Test updating a history item."""
        status, data = self.api("PATCH", f"/api/history/item/{item_id}", ts.session, {
            "title": f"Updated Title {RUN_ID}"
        })

        success = status == 200
        self.assert_test("Update history item", success,
                        f"status={status}")
        return success

    def test_delete_history_item(self, ts: TestSession, item_id: str) -> bool:
        """Test deleting a history item."""
        status, data = self.api("DELETE", f"/api/history/item/{item_id}", ts.session)

        success = status == 200
        self.assert_test("Delete history item", success,
                        f"status={status}")
        return success

    # =========================================================================
    # RUN ALL TESTS
    # =========================================================================

    def run(self):
        """Run all smoke tests."""
        log(f"\n{Colors.BOLD}TimrX Smoke Tests{Colors.RESET}")
        log(f"API Base: {API_BASE}")
        log(f"Run ID: {RUN_ID}")
        self.session = TestSession("anonymous", "", requests.Session())

        # ─────────────────────────────────────────────────────────────
        log_section("1. HEALTH CHECK")
        # ─────────────────────────────────────────────────────────────
        self.test_health()

        # ─────────────────────────────────────────────────────────────
        log_section("2. JOB TESTS (Anonymous)")
        # ─────────────────────────────────────────────────────────────
        self.job_id = self.test_create_job(self.session)

        if self.job_id:
            self.test_poll_status(self.session, self.job_id)

        # ─────────────────────────────────────────────────────────────
        log_section("3. HISTORY TESTS (Anonymous)")
        # ─────────────────────────────────────────────────────────────
        self.test_list_history(self.session)

        self.history_id = self.test_add_history_item(self.session)

        if self.history_id:
            self.test_list_history(self.session)  # Verify it's there
            self.test_update_history_item(self.session, self.history_id)

        # ─────────────────────────────────────────────────────────────
        log_section("4. CLEANUP")
        # ─────────────────────────────────────────────────────────────
        # Delete history item (cleanup)
        if self.history_id:
            self.test_delete_history_item(self.session, self.history_id)

        return self.summary()

    def summary(self) -> int:
        """Print summary and return exit code."""
        total = self.passed + self.failed

        log_section("SUMMARY")
        log(f"  Total:  {total}")
        log(f"  Passed: {Colors.GREEN}{self.passed}{Colors.RESET}")
        log(f"  Failed: {Colors.RED}{self.failed}{Colors.RESET}")

        if self.failed == 0:
            log(f"\n{Colors.GREEN}{Colors.BOLD}All tests passed!{Colors.RESET}\n")
            return 0
        else:
            log(f"\n{Colors.RED}{Colors.BOLD}{self.failed} test(s) failed{Colors.RESET}\n")
            return 1


# ─────────────────────────────────────────────────────────────────────────────
# TEST PLAN (for manual reference)
# ─────────────────────────────────────────────────────────────────────────────
TEST_PLAN = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                       TimrX ANONYMOUS SMOKE TEST PLAN                       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  1. HEALTH                                                                   ║
║     └─ GET  /api/health               → 200 OK                               ║
║                                                                              ║
║  2. JOBS (anonymous)                                                         ║
║     ├─ POST /api/text-to-3d/start     → 200 + job_id                         ║
║     └─ GET  /api/text-to-3d/status/X  → 200 + status (pending/in_progress)   ║
║                                                                              ║
║  3. HISTORY (anonymous)                                                      ║
║     ├─ GET  /api/history              → 200 + [] (empty initially)           ║
║     ├─ POST /api/history/item         → 200/201 (item added)                 ║
║     ├─ GET  /api/history              → 200 + [item]                         ║
║     ├─ PATCH /api/history/item/X      → 200 (item updated)                   ║
║     └─ DELETE /api/history/item/X     → 200 (item deleted)                   ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""


if __name__ == "__main__":
    if "--plan" in sys.argv:
        print(TEST_PLAN)
        sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        print(TEST_PLAN)
        sys.exit(0)

    tests = SmokeTests()
    sys.exit(tests.run())
