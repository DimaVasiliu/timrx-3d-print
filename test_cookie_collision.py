"""
Test script for cookie collision resolution.

Simulates the scenario where a browser sends multiple timrx_sid cookies
(e.g., host-only cookie + domain cookie) and verifies:
1. All cookie values are parsed from the raw Cookie header
2. The active session is selected
3. Legacy cookies are expired in the response

Run with:
    cd TimrX/Backend/meshy
    python -m pytest tests/test_cookie_collision.py -v

Or standalone:
    cd TimrX/Backend/meshy
    python tests/test_cookie_collision.py
"""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest
from unittest.mock import MagicMock, patch
from flask import Flask


class MockRequest:
    """Mock Flask request with configurable Cookie header."""

    def __init__(self, cookie_header: str = "", cookies: dict = None):
        self.headers = {"Cookie": cookie_header}
        self.cookies = cookies or {}

    def __getattr__(self, name):
        if name == "headers":
            return self.headers
        raise AttributeError(name)


class TestCookieCollisionParsing(unittest.TestCase):
    """Test parsing multiple cookies from raw header."""

    def setUp(self):
        # Mock config
        self.config_patch = patch("config.config")
        self.mock_config = self.config_patch.start()
        self.mock_config.SESSION_COOKIE_NAME = "timrx_sid"
        self.mock_config.IS_PROD = True
        self.mock_config.SESSION_COOKIE_DOMAIN = ".timrx.live"

    def tearDown(self):
        self.config_patch.stop()

    def test_parse_single_cookie(self):
        """Single cookie should be parsed correctly."""
        from identity_service import IdentityService

        request = MockRequest(
            cookie_header="timrx_sid=abc-123-def-456",
            cookies={"timrx_sid": "abc-123-def-456"},
        )

        candidates = IdentityService._parse_all_session_ids_from_header(request)
        self.assertEqual(candidates, ["abc-123-def-456"])

    def test_parse_multiple_cookies_collision(self):
        """Multiple cookies with same name should all be parsed."""
        from identity_service import IdentityService

        # Simulate browser sending both host-only and domain cookies
        request = MockRequest(
            cookie_header="timrx_sid=old-session-id; timrx_sid=new-session-id",
            cookies={"timrx_sid": "old-session-id"},  # Flask only sees first
        )

        candidates = IdentityService._parse_all_session_ids_from_header(request)
        self.assertEqual(len(candidates), 2)
        self.assertIn("old-session-id", candidates)
        self.assertIn("new-session-id", candidates)

    def test_parse_removes_duplicates(self):
        """Duplicate values should be removed."""
        from identity_service import IdentityService

        request = MockRequest(
            cookie_header="timrx_sid=same-id; other=value; timrx_sid=same-id",
            cookies={"timrx_sid": "same-id"},
        )

        candidates = IdentityService._parse_all_session_ids_from_header(request)
        self.assertEqual(candidates, ["same-id"])

    def test_parse_with_other_cookies(self):
        """Should only extract timrx_sid, ignore other cookies."""
        from identity_service import IdentityService

        request = MockRequest(
            cookie_header="other_cookie=xyz; timrx_sid=my-session; another=123",
            cookies={"timrx_sid": "my-session", "other_cookie": "xyz"},
        )

        candidates = IdentityService._parse_all_session_ids_from_header(request)
        self.assertEqual(candidates, ["my-session"])

    def test_parse_uuid_format(self):
        """Should parse UUID-formatted session IDs."""
        from identity_service import IdentityService

        uuid_sid = "550e8400-e29b-41d4-a716-446655440000"
        request = MockRequest(
            cookie_header=f"timrx_sid={uuid_sid}",
            cookies={"timrx_sid": uuid_sid},
        )

        candidates = IdentityService._parse_all_session_ids_from_header(request)
        self.assertEqual(candidates, [uuid_sid])


class TestCookieCollisionResolution(unittest.TestCase):
    """Test selecting the active session from multiple cookies."""

    def setUp(self):
        self.config_patch = patch("config.config")
        self.mock_config = self.config_patch.start()
        self.mock_config.SESSION_COOKIE_NAME = "timrx_sid"
        self.mock_config.IS_PROD = True

    def tearDown(self):
        self.config_patch.stop()

    @patch("identity_service.IdentityService._check_session_active")
    def test_resolve_single_active_from_collision(self, mock_check):
        """When collision exists, should select the active session."""
        from identity_service import IdentityService

        # old-session is inactive, new-session is active
        mock_check.side_effect = lambda sid: sid == "new-session-id"

        request = MockRequest(
            cookie_header="timrx_sid=old-session-id; timrx_sid=new-session-id",
            cookies={"timrx_sid": "old-session-id"},
        )

        selected, candidates, reason = IdentityService.resolve_session_id(request)

        self.assertEqual(selected, "new-session-id")
        self.assertEqual(len(candidates), 2)
        self.assertEqual(reason, "single_active_from_collision")

    @patch("identity_service.IdentityService._check_session_active")
    def test_resolve_no_active_sessions(self, mock_check):
        """When no sessions are active, return first candidate."""
        from identity_service import IdentityService

        mock_check.return_value = False  # All sessions inactive

        request = MockRequest(
            cookie_header="timrx_sid=expired-1; timrx_sid=expired-2",
            cookies={"timrx_sid": "expired-1"},
        )

        selected, candidates, reason = IdentityService.resolve_session_id(request)

        self.assertEqual(selected, "expired-1")  # First candidate
        self.assertEqual(reason, "no_active_sessions")

    @patch("identity_service.IdentityService._check_session_active")
    def test_resolve_multiple_active_picks_first(self, mock_check):
        """When multiple active, pick first (deterministic)."""
        from identity_service import IdentityService

        mock_check.return_value = True  # Both active

        request = MockRequest(
            cookie_header="timrx_sid=session-a; timrx_sid=session-b",
            cookies={"timrx_sid": "session-a"},
        )

        selected, candidates, reason = IdentityService.resolve_session_id(request)

        self.assertEqual(selected, "session-a")
        self.assertEqual(reason, "first_active_from_multiple")


class TestLegacyCookieKiller(unittest.TestCase):
    """Test that legacy cookies are expired in response."""

    def setUp(self):
        self.app = Flask(__name__)
        self.config_patch = patch("config.config")
        self.mock_config = self.config_patch.start()
        self.mock_config.SESSION_COOKIE_NAME = "timrx_sid"
        self.mock_config.IS_PROD = True
        self.mock_config.SESSION_COOKIE_DOMAIN = ".timrx.live"
        self.mock_config.SESSION_TTL_SECONDS = 2592000

    def tearDown(self):
        self.config_patch.stop()

    def test_set_cookie_expires_legacy_cookies(self):
        """Setting canonical cookie should also expire legacy variants."""
        from identity_service import IdentityService

        with self.app.test_request_context():
            from flask import make_response

            response = make_response("OK")
            IdentityService.set_session_cookie(response, "new-session-id")

            # Get all Set-Cookie headers
            set_cookies = response.headers.getlist("Set-Cookie")

            # Should have multiple Set-Cookie headers:
            # 1. Expire host-only (no domain, max-age=0)
            # 2. Expire 3d.timrx.live (domain=3d.timrx.live, max-age=0)
            # 3. Expire www.timrx.live (domain=www.timrx.live, max-age=0)
            # 4. Set canonical (domain=.timrx.live, with session value)

            # Check that we have expiration cookies
            expire_cookies = [c for c in set_cookies if "Max-Age=0" in c or "max-age=0" in c.lower()]
            self.assertGreaterEqual(
                len(expire_cookies), 1, f"Expected at least 1 expiration cookie, got: {set_cookies}"
            )

            # Check canonical cookie is set
            canonical_cookies = [c for c in set_cookies if "new-session-id" in c]
            self.assertEqual(len(canonical_cookies), 1, f"Expected 1 canonical cookie, got: {set_cookies}")

            # Verify canonical has correct domain
            canonical = canonical_cookies[0]
            self.assertIn("Domain=.timrx.live", canonical)
            self.assertIn("SameSite=None", canonical)
            self.assertIn("Secure", canonical)


def run_manual_test():
    """
    Manual test that can be run against a real Flask app.
    Simulates a collision request and prints the results.
    """
    print("=" * 60)
    print("Manual Cookie Collision Test")
    print("=" * 60)

    # Create a mock request with collision
    old_sid = "11111111-1111-1111-1111-111111111111"
    new_sid = "22222222-2222-2222-2222-222222222222"

    print(f"\nSimulating request with two cookies:")
    print(f"  Cookie: timrx_sid={old_sid}; timrx_sid={new_sid}")

    # Import after path setup
    try:
        from identity_service import IdentityService

        request = MockRequest(
            cookie_header=f"timrx_sid={old_sid}; timrx_sid={new_sid}",
            cookies={"timrx_sid": old_sid},
        )

        # Test parsing
        print("\n1. Parsing all cookie values:")
        candidates = IdentityService._parse_all_session_ids_from_header(request)
        print(f"   Found {len(candidates)} candidates: {candidates}")

        # Note: resolve_session_id will try DB check, which will fail without DB
        print("\n2. Resolution (without DB, will return first candidate):")
        with patch.object(IdentityService, "_check_session_active", return_value=False):
            selected, _, reason = IdentityService.resolve_session_id(request)
            print(f"   Selected: {selected}")
            print(f"   Reason: {reason}")

        print("\n3. Testing with mocked active session:")
        # Mock: new_sid is active, old_sid is not
        with patch.object(
            IdentityService, "_check_session_active", side_effect=lambda s: s == new_sid
        ):
            selected, _, reason = IdentityService.resolve_session_id(request)
            print(f"   Selected: {selected}")
            print(f"   Reason: {reason}")
            if selected == new_sid:
                print("   SUCCESS: Correctly selected the active session!")
            else:
                print("   FAILURE: Selected wrong session")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()

    print("\n" + "=" * 60)


if __name__ == "__main__":
    # Run manual test first
    run_manual_test()

    # Then run unit tests
    print("\nRunning unit tests...\n")
    unittest.main(verbosity=2)
