"""
Tests for job identity enforcement (P1-D1 fix).

These tests verify that jobs cannot be created without identity_id,
preventing orphaned jobs that cannot be retrieved by any user.

Run locally:
    python -m pytest backend/tests/test_job_identity.py -v

Or:
    python -m backend.tests.test_job_identity
"""

from __future__ import annotations

import os
import uuid
import pytest

from backend.services.job_service import (
    MissingIdentityError,
    save_active_job_to_db,
    create_internal_job_row,
    USE_DB,
)


class TestMissingIdentityError:
    """Test the MissingIdentityError exception."""

    def test_error_message_without_job_id(self):
        error = MissingIdentityError("test_operation")
        assert "identity_id is required" in str(error)
        assert "test_operation" in str(error)
        assert error.operation == "test_operation"
        assert error.job_id is None

    def test_error_message_with_job_id(self):
        error = MissingIdentityError("test_operation", "job-123")
        assert "identity_id is required" in str(error)
        assert "test_operation" in str(error)
        assert "job-123" in str(error)
        assert error.operation == "test_operation"
        assert error.job_id == "job-123"

    def test_is_value_error(self):
        """MissingIdentityError should be a ValueError for compatibility."""
        error = MissingIdentityError("test")
        assert isinstance(error, ValueError)


class TestSaveActiveJobToDb:
    """Test save_active_job_to_db identity enforcement."""

    def test_raises_when_no_identity(self):
        """save_active_job_to_db must raise MissingIdentityError when identity_id is missing."""
        if not USE_DB:
            pytest.skip("Database not available")

        job_id = f"test-{uuid.uuid4().hex[:8]}"

        with pytest.raises(MissingIdentityError) as exc_info:
            save_active_job_to_db(
                job_id=job_id,
                job_type="text-to-3d",
                metadata={"prompt": "test prompt"},
                user_id=None,  # No identity
            )

        assert exc_info.value.operation == "save_active_job_to_db"
        assert exc_info.value.job_id == job_id

    def test_raises_when_empty_identity(self):
        """save_active_job_to_db must raise when identity_id is empty string."""
        if not USE_DB:
            pytest.skip("Database not available")

        job_id = f"test-{uuid.uuid4().hex[:8]}"

        with pytest.raises(MissingIdentityError):
            save_active_job_to_db(
                job_id=job_id,
                job_type="text-to-3d",
                user_id="",  # Empty string
            )

    def test_allows_anonymous_when_flag_set(self):
        """save_active_job_to_db allows anonymous when allow_anonymous=True."""
        if not USE_DB:
            pytest.skip("Database not available")

        job_id = f"test-anon-{uuid.uuid4().hex[:8]}"

        # Should not raise when allow_anonymous=True
        result = save_active_job_to_db(
            job_id=job_id,
            job_type="text-to-3d",
            user_id=None,
            allow_anonymous=True,
        )

        # Result depends on DB state, but should not raise
        assert result in (True, False)

    def test_extracts_identity_from_metadata(self):
        """save_active_job_to_db should extract identity_id from metadata."""
        if not USE_DB:
            pytest.skip("Database not available")

        job_id = f"test-meta-{uuid.uuid4().hex[:8]}"
        # Use a fake UUID that won't conflict (test will fail on FK constraint anyway)
        fake_identity = str(uuid.uuid4())

        # This should NOT raise because identity_id is in metadata
        # Note: Will likely fail on FK constraint in real DB, but that's expected
        try:
            save_active_job_to_db(
                job_id=job_id,
                job_type="text-to-3d",
                metadata={"identity_id": fake_identity},
                user_id=None,  # Will be extracted from metadata
            )
        except MissingIdentityError:
            pytest.fail("Should have extracted identity_id from metadata")
        except Exception:
            # Other exceptions (like FK constraint) are expected in test
            pass


class TestCreateInternalJobRow:
    """Test create_internal_job_row identity enforcement."""

    def test_raises_when_no_identity(self):
        """create_internal_job_row must raise MissingIdentityError when identity_id is missing."""
        if not USE_DB:
            pytest.skip("Database not available")

        job_id = str(uuid.uuid4())

        with pytest.raises(MissingIdentityError) as exc_info:
            create_internal_job_row(
                internal_job_id=job_id,
                identity_id=None,  # No identity
                provider="meshy",
                action_key="text_to_3d_generate",
            )

        assert exc_info.value.operation == "create_internal_job_row"
        assert exc_info.value.job_id == job_id

    def test_raises_when_empty_identity(self):
        """create_internal_job_row must raise when identity_id is empty string."""
        if not USE_DB:
            pytest.skip("Database not available")

        job_id = str(uuid.uuid4())

        with pytest.raises(MissingIdentityError):
            create_internal_job_row(
                internal_job_id=job_id,
                identity_id="",  # Empty string
                provider="meshy",
                action_key="text_to_3d_generate",
            )


class TestNoDbMode:
    """Test behavior when database is not available."""

    def test_save_active_job_returns_false_without_db(self, monkeypatch):
        """save_active_job_to_db returns False when USE_DB is False."""
        # Temporarily disable DB
        import backend.services.job_service as job_service_module
        original_use_db = job_service_module.USE_DB

        try:
            monkeypatch.setattr(job_service_module, "USE_DB", False)

            # Should return False, not raise
            result = save_active_job_to_db(
                job_id="test",
                job_type="text-to-3d",
                user_id=None,
            )
            assert result is False
        finally:
            monkeypatch.setattr(job_service_module, "USE_DB", original_use_db)

    def test_create_internal_job_returns_false_without_db(self, monkeypatch):
        """create_internal_job_row returns False when USE_DB is False."""
        import backend.services.job_service as job_service_module
        original_use_db = job_service_module.USE_DB

        try:
            monkeypatch.setattr(job_service_module, "USE_DB", False)

            # Should return False, not raise
            result = create_internal_job_row(
                internal_job_id="test",
                identity_id=None,
                provider="meshy",
                action_key="text_to_3d_generate",
            )
            assert result is False
        finally:
            monkeypatch.setattr(job_service_module, "USE_DB", original_use_db)


def test_missing_identity_error():
    """Basic test for MissingIdentityError."""
    error = MissingIdentityError("test_op", "job-123")
    assert "identity_id is required" in str(error)
    assert "test_op" in str(error)
    assert "job-123" in str(error)
    print("[OK] MissingIdentityError test passed")


def test_save_active_job_raises_without_identity():
    """Test that save_active_job_to_db raises without identity_id."""
    if not USE_DB:
        print("[SKIP] USE_DB is False; skipping DB test")
        return

    job_id = f"test-orphan-{uuid.uuid4().hex[:8]}"

    try:
        save_active_job_to_db(
            job_id=job_id,
            job_type="text-to-3d",
            user_id=None,
        )
        raise AssertionError("Should have raised MissingIdentityError")
    except MissingIdentityError as e:
        assert e.operation == "save_active_job_to_db"
        print(f"[OK] Correctly raised MissingIdentityError: {e}")


def test_create_internal_job_raises_without_identity():
    """Test that create_internal_job_row raises without identity_id."""
    if not USE_DB:
        print("[SKIP] USE_DB is False; skipping DB test")
        return

    job_id = str(uuid.uuid4())

    try:
        create_internal_job_row(
            internal_job_id=job_id,
            identity_id=None,
            provider="meshy",
            action_key="text_to_3d_generate",
        )
        raise AssertionError("Should have raised MissingIdentityError")
    except MissingIdentityError as e:
        assert e.operation == "create_internal_job_row"
        print(f"[OK] Correctly raised MissingIdentityError: {e}")


if __name__ == "__main__":
    test_missing_identity_error()
    test_save_active_job_raises_without_identity()
    test_create_internal_job_raises_without_identity()
    print("\n[ALL TESTS PASSED]")
