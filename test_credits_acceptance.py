#!/usr/bin/env python3
"""
TimrX Credits & History Acceptance Tests

Tests the complete credit lifecycle:
  1. Create identity, grant credits, verify /api/me shows expected balance
  2. Call POST /api/_mod/image/openai and verify:
     - If balance < 10 -> 402 (Insufficient Credits)
     - If balance >= 10 -> reservation row created immediately
  3. Simulate completion: verify reservation finalizes, ledger debit exists, wallet decreased by 10
  4. Verify history: generated image saved (DB row + S3 key) and /api/history returns it
  5. Verify restore flow: redeem magic code on another device -> same credits + history appear

Usage:
  # Run against local dev server
  python tests/test_credits_acceptance.py

  # Run against production
  API_BASE=https://3d.timrx.live python tests/test_credits_acceptance.py

  # Verbose mode (show all responses)
  VERBOSE=1 python tests/test_credits_acceptance.py

  # Skip slow tests (no job completion waiting)
  QUICK=1 python tests/test_credits_acceptance.py

  # Admin tests (requires ADMIN_TOKEN env var)
  ADMIN_TOKEN=xxx python tests/test_credits_acceptance.py

Requirements:
  pip install requests python-dotenv
"""

import os
import sys
import time
import json
import uuid
import requests
from typing import Optional, Tuple, Any, Dict
from dataclasses import dataclass, field
from datetime import datetime

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# CONFIGURATION
# ============================================================================

API_BASE = os.getenv("API_BASE", "http://localhost:5001").rstrip("/")
VERBOSE = os.getenv("VERBOSE", "").lower() in ("1", "true", "yes")
QUICK = os.getenv("QUICK", "").lower() in ("1", "true", "yes")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

# Cost of OpenAI image generation
OPENAI_IMAGE_COST = 10

RUN_ID = uuid.uuid4().hex[:8]


# ============================================================================
# COLORS & LOGGING
# ============================================================================

class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
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


def log_pass(name: str, detail: str = ""):
    msg = f"  {Colors.GREEN}✓{Colors.RESET} {name}"
    if detail:
        msg += f" ({detail})"
    print(msg)


def log_fail(name: str, reason: str = ""):
    msg = f"  {Colors.RED}✗{Colors.RESET} {name}"
    if reason:
        msg += f": {reason}"
    print(msg)


def log_warn(name: str, reason: str = ""):
    msg = f"  {Colors.YELLOW}⚠{Colors.RESET} {name}"
    if reason:
        msg += f": {reason}"
    print(msg)


def log_section(name: str):
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{name}{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")


# ============================================================================
# TEST SESSION
# ============================================================================

@dataclass
class TestSession:
    """Holds session state for a test run."""
    session: requests.Session = field(default_factory=requests.Session)
    identity_id: Optional[str] = None
    initial_balance: int = 0
    email: Optional[str] = None


# ============================================================================
# ACCEPTANCE TESTS
# ============================================================================

class CreditsAcceptanceTests:
    """Full credits & history acceptance test suite."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        self.ts: Optional[TestSession] = None
        self.ts2: Optional[TestSession] = None  # For restore test
        self.job_id: Optional[str] = None
        self.reservation_id: Optional[str] = None

    def api(
        self,
        method: str,
        path: str,
        json_data: dict = None,
        session: requests.Session = None,
        headers: dict = None,
        timeout: int = 30,
    ) -> Tuple[int, Any]:
        """Make API request and return (status_code, json_response)."""
        url = f"{API_BASE}{path}"
        s = session or self.ts.session

        try:
            kwargs = {"timeout": timeout}
            if json_data:
                kwargs["json"] = json_data
            if headers:
                kwargs["headers"] = headers

            if method.upper() == "GET":
                resp = s.get(url, **kwargs)
            elif method.upper() == "POST":
                resp = s.post(url, **kwargs)
            elif method.upper() == "PATCH":
                resp = s.patch(url, **kwargs)
            elif method.upper() == "DELETE":
                resp = s.delete(url, **kwargs)
            else:
                raise ValueError(f"Unknown method: {method}")

            log_verbose(f"{method} {path} -> {resp.status_code}")

            try:
                data = resp.json()
                log_verbose(f"Response: {json.dumps(data)[:300]}")
            except Exception:
                data = {"_raw": resp.text[:500]}

            return resp.status_code, data

        except requests.exceptions.Timeout:
            log_verbose(f"Request timed out after {timeout}s")
            return 0, {"error": "timeout"}
        except requests.exceptions.RequestException as e:
            log_verbose(f"Request failed: {e}")
            return 0, {"error": str(e)}

    def admin_api(self, method: str, path: str, json_data: dict = None) -> Tuple[int, Any]:
        """Make admin API request with auth token."""
        headers = {"Authorization": f"Bearer {ADMIN_TOKEN}"} if ADMIN_TOKEN else {}
        return self.api(method, path, json_data, headers=headers, session=self.ts.session)

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

    def warn_test(self, name: str, reason: str = ""):
        """Log a warning."""
        log_warn(name, reason)
        self.warnings += 1

    # =========================================================================
    # TEST 1: Create identity and verify balance
    # =========================================================================
    def test_1_create_identity_and_get_balance(self) -> bool:
        """Create identity via /api/me and verify initial balance."""
        log_section("TEST 1: Create Identity & Verify Balance")

        # Create session
        self.ts = TestSession()

        # Call /api/me to create identity
        status, data = self.api("GET", "/api/me", timeout=25)

        if not self.assert_test(
            "/api/me returns 200",
            status == 200,
            f"status={status}"
        ):
            return False

        self.ts.identity_id = data.get("identity_id")
        if not self.assert_test(
            "identity_id returned",
            bool(self.ts.identity_id),
            f"data={data}"
        ):
            return False

        # Check balance
        balance = data.get("available_credits") or data.get("credits_balance") or 0
        self.ts.initial_balance = balance

        self.assert_test(
            "Balance field present",
            "available_credits" in data or "credits_balance" in data,
            f"keys={list(data.keys())}"
        )

        log(f"  {Colors.CYAN}Identity: {self.ts.identity_id}{Colors.RESET}")
        log(f"  {Colors.CYAN}Balance:  {balance} credits{Colors.RESET}")

        return True

    # =========================================================================
    # TEST 2: Grant credits (requires admin)
    # =========================================================================
    def test_2_grant_credits(self) -> bool:
        """Grant credits via admin API and verify balance increases."""
        log_section("TEST 2: Grant Credits (Admin)")

        if not ADMIN_TOKEN:
            self.warn_test("Skipped - ADMIN_TOKEN not set")
            return True

        grant_amount = 50

        # Grant credits
        status, data = self.admin_api("POST", "/api/admin/credits/grant", {
            "identity_id": self.ts.identity_id,
            "amount": grant_amount,
            "reason": f"test_credits_acceptance_{RUN_ID}",
        })

        if not self.assert_test(
            f"Admin grant {grant_amount} credits",
            status == 200 and data.get("ok"),
            f"status={status}, data={data}"
        ):
            return False

        new_balance = data.get("new_balance", 0)
        expected = self.ts.initial_balance + grant_amount

        self.assert_test(
            f"New balance = {expected}",
            new_balance == expected,
            f"actual={new_balance}, expected={expected}"
        )

        # Verify via /api/me
        status, data = self.api("GET", "/api/me")
        me_balance = data.get("available_credits") or data.get("credits_balance") or 0

        self.assert_test(
            f"/api/me shows {expected} credits",
            me_balance == expected,
            f"actual={me_balance}"
        )

        self.ts.initial_balance = expected
        return True

    # =========================================================================
    # TEST 3: Insufficient credits returns 402
    # =========================================================================
    def test_3_insufficient_credits_402(self) -> bool:
        """Verify that insufficient credits returns 402."""
        log_section("TEST 3: Insufficient Credits -> 402")

        # Create a fresh session with 0 credits
        fresh_ts = TestSession()

        # Establish identity
        status, data = self.api("GET", "/api/me", session=fresh_ts.session)
        if status != 200:
            self.warn_test("Could not create fresh identity")
            return True

        balance = data.get("available_credits") or data.get("credits_balance") or 0

        if balance >= OPENAI_IMAGE_COST:
            self.warn_test(
                f"Fresh identity has {balance} credits (>= {OPENAI_IMAGE_COST})",
                "Cannot test 402 - new identities get free credits"
            )
            return True

        # Try to generate image
        status, data = self.api("POST", "/api/_mod/image/openai", {
            "prompt": "A test image",
            "model": "dall-e-3",
            "size": "1024x1024",
        }, session=fresh_ts.session)

        self.assert_test(
            f"Insufficient credits returns 402",
            status == 402,
            f"status={status}, code={data.get('code')}"
        )

        self.assert_test(
            "Error code is INSUFFICIENT_CREDITS",
            data.get("code") == "INSUFFICIENT_CREDITS",
            f"code={data.get('code')}"
        )

        return True

    # =========================================================================
    # TEST 4: Start OpenAI image job with sufficient credits
    # =========================================================================
    def test_4_start_openai_image_job(self) -> bool:
        """Start an OpenAI image job and verify reservation is created."""
        log_section("TEST 4: Start OpenAI Image Job")

        if self.ts.initial_balance < OPENAI_IMAGE_COST:
            self.warn_test(
                f"Insufficient credits ({self.ts.initial_balance} < {OPENAI_IMAGE_COST})",
                "Run with ADMIN_TOKEN to grant credits first"
            )
            return False

        # Get balance before
        status, data = self.api("GET", "/api/me")
        balance_before = data.get("available_credits") or data.get("credits_balance") or 0

        # Start image generation
        status, data = self.api("POST", "/api/_mod/image/openai", {
            "prompt": f"A simple red cube test_{RUN_ID}",
            "model": "dall-e-3",
            "size": "1024x1024",
        }, timeout=120)

        if not self.assert_test(
            "POST /api/_mod/image/openai returns 200",
            status == 200,
            f"status={status}, error={data.get('error')}"
        ):
            return False

        self.job_id = data.get("job_id")
        self.reservation_id = data.get("reservation_id")

        self.assert_test(
            "job_id returned",
            bool(self.job_id),
            f"data={data}"
        )

        log(f"  {Colors.CYAN}Job ID:         {self.job_id}{Colors.RESET}")
        log(f"  {Colors.CYAN}Reservation ID: {self.reservation_id}{Colors.RESET}")

        # Verify reservation exists (if admin token available)
        if ADMIN_TOKEN and self.reservation_id:
            status, debug_data = self.admin_api(
                "GET",
                f"/api/admin/debug/openai-credits?job_id={self.job_id}"
            )
            if status == 200 and debug_data.get("ok"):
                reservations = debug_data.get("reservations", [])
                held_reservations = [r for r in reservations if r.get("status") == "held"]

                self.assert_test(
                    "Reservation exists in DB with 'held' status",
                    len(held_reservations) > 0,
                    f"found {len(reservations)} reservations, {len(held_reservations)} held"
                )

        return True

    # =========================================================================
    # TEST 5: Poll until completion and verify finalization
    # =========================================================================
    def test_5_poll_and_verify_finalization(self) -> bool:
        """Poll job status and verify credits are finalized after completion."""
        log_section("TEST 5: Poll & Verify Finalization")

        if not self.job_id:
            self.warn_test("No job_id - skipping")
            return True

        if QUICK:
            self.warn_test("Skipped (QUICK mode)")
            return True

        # Poll for completion
        max_polls = 60  # 60 * 2s = 2 minutes max
        final_status = None
        image_url = None

        for i in range(max_polls):
            status, data = self.api("GET", f"/api/_mod/image/openai/status/{self.job_id}")

            if status != 200:
                time.sleep(2)
                continue

            job_status = data.get("status")
            log_verbose(f"Poll {i+1}/{max_polls}: status={job_status}")

            if job_status == "done":
                final_status = "done"
                image_url = data.get("image_url") or data.get("image_urls")
                break
            elif job_status == "failed":
                final_status = "failed"
                break

            time.sleep(2)

        self.assert_test(
            "Job completed (done or failed)",
            final_status in ("done", "failed"),
            f"final_status={final_status}"
        )

        if final_status == "done":
            self.assert_test(
                "Image URL returned",
                bool(image_url),
                f"image_url={image_url}"
            )

            # Verify reservation finalized
            if ADMIN_TOKEN:
                time.sleep(1)  # Give backend time to finalize
                status, debug_data = self.admin_api(
                    "GET",
                    f"/api/admin/debug/openai-credits?job_id={self.job_id}"
                )

                if status == 200 and debug_data.get("ok"):
                    reservations = debug_data.get("reservations", [])
                    finalized = [r for r in reservations if r.get("status") == "finalized"]

                    self.assert_test(
                        "Reservation finalized",
                        len(finalized) > 0,
                        f"found {len(finalized)} finalized reservations"
                    )

                    # Check ledger entries
                    ledger = debug_data.get("ledger_entries", [])
                    debits = [le for le in ledger if le.get("amount_credits", 0) < 0]

                    self.assert_test(
                        "Ledger debit exists",
                        len(debits) > 0,
                        f"found {len(debits)} debit entries"
                    )

            # Verify balance decreased
            status, data = self.api("GET", "/api/me")
            balance_after = data.get("available_credits") or data.get("credits_balance") or 0
            expected_balance = self.ts.initial_balance - OPENAI_IMAGE_COST

            self.assert_test(
                f"Balance decreased by {OPENAI_IMAGE_COST}",
                balance_after == expected_balance,
                f"before={self.ts.initial_balance}, after={balance_after}, expected={expected_balance}"
            )

            self.ts.initial_balance = balance_after

        elif final_status == "failed":
            log(f"  {Colors.YELLOW}Job failed - checking credits were refunded{Colors.RESET}")

            # Verify reservation released
            if ADMIN_TOKEN:
                time.sleep(1)
                status, debug_data = self.admin_api(
                    "GET",
                    f"/api/admin/debug/openai-credits?job_id={self.job_id}"
                )

                if status == 200 and debug_data.get("ok"):
                    reservations = debug_data.get("reservations", [])
                    released = [r for r in reservations if r.get("status") == "released"]

                    self.assert_test(
                        "Reservation released (refunded)",
                        len(released) > 0,
                        f"found {len(released)} released reservations"
                    )

        return True

    # =========================================================================
    # TEST 6: Verify history
    # =========================================================================
    def test_6_verify_history(self) -> bool:
        """Verify generated image appears in history."""
        log_section("TEST 6: Verify History")

        if not self.job_id:
            self.warn_test("No job_id - skipping")
            return True

        status, history = self.api("GET", "/api/_mod/history")

        self.assert_test(
            "/api/_mod/history returns 200",
            status == 200,
            f"status={status}"
        )

        self.assert_test(
            "History is a list",
            isinstance(history, list),
            f"type={type(history)}"
        )

        # Find our job in history
        our_item = None
        for item in history:
            if item.get("id") == self.job_id:
                our_item = item
                break

        self.assert_test(
            f"Job {self.job_id} found in history",
            our_item is not None,
            f"searched {len(history)} items"
        )

        if our_item:
            # Verify it has required fields
            self.assert_test(
                "History item has image_url",
                bool(our_item.get("image_url") or our_item.get("thumbnail_url")),
                f"item={our_item}"
            )

            self.assert_test(
                "History item has status",
                bool(our_item.get("status")),
                f"status={our_item.get('status')}"
            )

            # Check S3 URL (not Meshy)
            image_url = our_item.get("image_url", "")
            if image_url:
                is_s3 = ".s3." in image_url or "amazonaws.com" in image_url
                self.assert_test(
                    "Image stored in S3 (not Meshy)",
                    is_s3,
                    f"url={image_url[:60]}..."
                )

        return True

    # =========================================================================
    # TEST 7: Magic code restore flow
    # =========================================================================
    def test_7_magic_code_restore(self) -> bool:
        """Test restore flow: magic code on new device shows same credits & history."""
        log_section("TEST 7: Magic Code Restore Flow")

        if not ADMIN_TOKEN:
            self.warn_test("Skipped - ADMIN_TOKEN required for magic code test setup")
            return True

        # Step 1: Set email on current identity
        test_email = f"test_{RUN_ID}@example.com"

        status, data = self.admin_api("PATCH", f"/api/admin/identity/{self.ts.identity_id}", {
            "email": test_email,
        })

        if status != 200:
            self.warn_test(f"Could not set email: {data}")
            return True

        self.ts.email = test_email
        log(f"  {Colors.CYAN}Test email: {test_email}{Colors.RESET}")

        # Get current balance and history count
        status, me_data = self.api("GET", "/api/me")
        original_balance = me_data.get("available_credits") or me_data.get("credits_balance") or 0

        status, history = self.api("GET", "/api/_mod/history")
        original_history_count = len(history) if isinstance(history, list) else 0

        log(f"  {Colors.CYAN}Original balance: {original_balance}{Colors.RESET}")
        log(f"  {Colors.CYAN}Original history: {original_history_count} items{Colors.RESET}")

        # Step 2: Create a magic code directly (admin bypass)
        # In production, user would call POST /api/auth/restore/request
        # For testing, we can use admin to get the code

        # Request magic code
        status, data = self.api("POST", "/api/auth/restore/request", {
            "email": test_email,
        })

        if status != 200:
            self.warn_test(f"Could not request magic code: {data}")
            return True

        # For testing, we need to get the code from DB (admin only)
        # In real flow, user gets it via email
        status, code_data = self.admin_api("GET", f"/api/admin/debug/magic-code?email={test_email}")

        if status != 200 or not code_data.get("code"):
            self.warn_test("Could not retrieve magic code (need debug endpoint)")
            return True

        magic_code = code_data.get("code")
        log(f"  {Colors.CYAN}Magic code: {magic_code}{Colors.RESET}")

        # Step 3: Create new session (different device simulation)
        self.ts2 = TestSession()

        # Establish new identity
        status, data = self.api("GET", "/api/me", session=self.ts2.session)
        new_identity_id = data.get("identity_id")

        self.assert_test(
            "New session has different identity",
            new_identity_id != self.ts.identity_id,
            f"original={self.ts.identity_id}, new={new_identity_id}"
        )

        # Step 4: Redeem magic code
        status, data = self.api("POST", "/api/auth/restore/redeem", {
            "email": test_email,
            "code": magic_code,
        }, session=self.ts2.session)

        self.assert_test(
            "Magic code redemption successful",
            status == 200 and data.get("ok"),
            f"status={status}, data={data}"
        )

        # Step 5: Verify same identity
        status, data = self.api("GET", "/api/me", session=self.ts2.session)
        restored_identity = data.get("identity_id")

        self.assert_test(
            "Session linked to original identity",
            restored_identity == self.ts.identity_id,
            f"original={self.ts.identity_id}, restored={restored_identity}"
        )

        # Step 6: Verify same balance
        restored_balance = data.get("available_credits") or data.get("credits_balance") or 0

        self.assert_test(
            "Credits match original",
            restored_balance == original_balance,
            f"original={original_balance}, restored={restored_balance}"
        )

        # Step 7: Verify same history
        status, restored_history = self.api("GET", "/api/_mod/history", session=self.ts2.session)
        restored_history_count = len(restored_history) if isinstance(restored_history, list) else 0

        self.assert_test(
            "History count matches original",
            restored_history_count == original_history_count,
            f"original={original_history_count}, restored={restored_history_count}"
        )

        return True

    # =========================================================================
    # RUN ALL TESTS
    # =========================================================================
    def run(self) -> int:
        """Run all acceptance tests."""
        log(f"\n{Colors.BOLD}{Colors.CYAN}TimrX Credits & History Acceptance Tests{Colors.RESET}")
        log(f"API Base: {API_BASE}")
        log(f"Run ID:   {RUN_ID}")
        log(f"Admin:    {'Yes' if ADMIN_TOKEN else 'No'}")
        log(f"Quick:    {QUICK}")

        # Run tests in order
        tests = [
            self.test_1_create_identity_and_get_balance,
            self.test_2_grant_credits,
            self.test_3_insufficient_credits_402,
            self.test_4_start_openai_image_job,
            self.test_5_poll_and_verify_finalization,
            self.test_6_verify_history,
            self.test_7_magic_code_restore,
        ]

        for test in tests:
            try:
                test()
            except Exception as e:
                log_fail(test.__name__, str(e))
                self.failed += 1
                if VERBOSE:
                    import traceback
                    traceback.print_exc()

        return self.summary()

    def summary(self) -> int:
        """Print summary and return exit code."""
        total = self.passed + self.failed

        log_section("SUMMARY")
        log(f"  Total:    {total}")
        log(f"  Passed:   {Colors.GREEN}{self.passed}{Colors.RESET}")
        log(f"  Failed:   {Colors.RED}{self.failed}{Colors.RESET}")
        log(f"  Warnings: {Colors.YELLOW}{self.warnings}{Colors.RESET}")

        if self.failed == 0:
            log(f"\n{Colors.GREEN}{Colors.BOLD}All tests passed!{Colors.RESET}\n")
            return 0
        else:
            log(f"\n{Colors.RED}{Colors.BOLD}{self.failed} test(s) failed{Colors.RESET}\n")
            return 1


# ============================================================================
# MANUAL CURL COMMANDS
# ============================================================================

CURL_COMMANDS = """
================================================================================
                    MANUAL CURL TEST COMMANDS
================================================================================

# Prerequisites: Set these variables
BASE=https://3d.timrx.live  # or http://localhost:5001
ADMIN_TOKEN=your_admin_token_here

# ─────────────────────────────────────────────────────────────────────────────
# TEST 1: Create identity and get balance
# ─────────────────────────────────────────────────────────────────────────────

curl -c cookies.txt -b cookies.txt "$BASE/api/me" | jq

# Expected: identity_id, available_credits returned

# ─────────────────────────────────────────────────────────────────────────────
# TEST 2: Grant credits (admin)
# ─────────────────────────────────────────────────────────────────────────────

IDENTITY_ID=$(curl -s -b cookies.txt "$BASE/api/me" | jq -r '.identity_id')

curl -X POST "$BASE/api/admin/credits/grant" \\
  -H "Authorization: Bearer $ADMIN_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{"identity_id":"'"$IDENTITY_ID"'","amount":50,"reason":"test"}' | jq

# Expected: ok: true, new_balance: 50

# ─────────────────────────────────────────────────────────────────────────────
# TEST 3: Insufficient credits returns 402
# ─────────────────────────────────────────────────────────────────────────────

# Create new session with no credits
curl -c cookies2.txt -b cookies2.txt "$BASE/api/me" | jq

curl -X POST "$BASE/api/_mod/image/openai" \\
  -H "Content-Type: application/json" \\
  -b cookies2.txt \\
  -d '{"prompt":"test","model":"dall-e-3","size":"1024x1024"}' | jq

# Expected: 402, code: INSUFFICIENT_CREDITS

# ─────────────────────────────────────────────────────────────────────────────
# TEST 4: Start OpenAI image job
# ─────────────────────────────────────────────────────────────────────────────

JOB_RESPONSE=$(curl -s -X POST "$BASE/api/_mod/image/openai" \\
  -H "Content-Type: application/json" \\
  -b cookies.txt \\
  -d '{"prompt":"A red cube","model":"dall-e-3","size":"1024x1024"}')

echo $JOB_RESPONSE | jq
JOB_ID=$(echo $JOB_RESPONSE | jq -r '.job_id')

# Expected: job_id returned

# ─────────────────────────────────────────────────────────────────────────────
# TEST 5: Poll for completion
# ─────────────────────────────────────────────────────────────────────────────

while true; do
  STATUS=$(curl -s -b cookies.txt "$BASE/api/_mod/image/openai/status/$JOB_ID")
  echo $STATUS | jq
  JOB_STATUS=$(echo $STATUS | jq -r '.status')
  if [ "$JOB_STATUS" = "done" ] || [ "$JOB_STATUS" = "failed" ]; then
    break
  fi
  sleep 2
done

# Verify credits finalized (admin)
curl "$BASE/api/admin/debug/openai-credits?job_id=$JOB_ID" \\
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq

# Expected: reservation status = "finalized", ledger debit exists

# ─────────────────────────────────────────────────────────────────────────────
# TEST 6: Verify history
# ─────────────────────────────────────────────────────────────────────────────

curl -b cookies.txt "$BASE/api/_mod/history" | jq ".[] | select(.id == \\"$JOB_ID\\")"

# Expected: job found in history with image_url

# ─────────────────────────────────────────────────────────────────────────────
# TEST 7: Magic code restore
# ─────────────────────────────────────────────────────────────────────────────

# Set email on identity (admin)
curl -X PATCH "$BASE/api/admin/identity/$IDENTITY_ID" \\
  -H "Authorization: Bearer $ADMIN_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{"email":"test@example.com"}' | jq

# Request magic code
curl -X POST "$BASE/api/auth/restore/request" \\
  -H "Content-Type: application/json" \\
  -d '{"email":"test@example.com"}' | jq

# Get code from admin debug (testing only)
curl "$BASE/api/admin/debug/magic-code?email=test@example.com" \\
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq

# Redeem on new session
curl -c cookies3.txt -b cookies3.txt "$BASE/api/me" | jq
curl -X POST "$BASE/api/auth/restore/redeem" \\
  -H "Content-Type: application/json" \\
  -b cookies3.txt \\
  -d '{"email":"test@example.com","code":"123456"}' | jq

# Verify same credits and history
curl -b cookies3.txt "$BASE/api/me" | jq
curl -b cookies3.txt "$BASE/api/_mod/history" | jq '. | length'

================================================================================
"""


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    if "--curl" in sys.argv:
        print(CURL_COMMANDS)
        sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        print(CURL_COMMANDS)
        sys.exit(0)

    tests = CreditsAcceptanceTests()
    sys.exit(tests.run())
