#!/usr/bin/env python3
"""
Subscription Health Check Script
---------------------------------
Runs periodic subscription health checks to ensure subscriptions are
properly managed and users are notified of payment issues.

This script should be run hourly via cron:
    0 * * * * cd /path/to/meshy && python scripts/subscription_health_check.py >> /var/log/timrx-subscription-health.log 2>&1

Or manually with options:
    # Full health check (all operations)
    python scripts/subscription_health_check.py

    # Grant due credits only
    python scripts/subscription_health_check.py --credits-only

    # Check expirations only
    python scripts/subscription_health_check.py --expire-only

    # Send past_due reminders only
    python scripts/subscription_health_check.py --reminders-only

    # Dry-run mode (detect only, no changes)
    python scripts/subscription_health_check.py --dry-run

    # Verbose output
    python scripts/subscription_health_check.py --verbose

Environment variables:
    DATABASE_URL: PostgreSQL connection string
    MOLLIE_API_KEY: Mollie API key (optional, for provider verification)
"""
import argparse
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    parser = argparse.ArgumentParser(
        description="Run subscription health checks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect issues but don't apply changes",
    )
    parser.add_argument(
        "--credits-only",
        action="store_true",
        help="Only process due credit allocations",
    )
    parser.add_argument(
        "--expire-only",
        action="store_true",
        help="Only check and expire invalid subscriptions",
    )
    parser.add_argument(
        "--reminders-only",
        action="store_true",
        help="Only send past_due reminder emails",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print detailed output",
    )
    args = parser.parse_args()

    # Default to running all checks if no specific flag is set
    run_all = not (args.credits_only or args.expire_only or args.reminders_only)

    # Log start
    start_time = datetime.utcnow()
    print(f"[{start_time.isoformat()}] Starting subscription health check...")
    print(f"  Mode: {'dry-run' if args.dry_run else 'apply'}")
    if not run_all:
        checks = []
        if args.credits_only:
            checks.append("credits")
        if args.expire_only:
            checks.append("expire")
        if args.reminders_only:
            checks.append("reminders")
        print(f"  Checks: {', '.join(checks)}")
    else:
        print("  Checks: all")
    print()

    total_errors = 0

    try:
        from backend.services.subscription_service import SubscriptionService

        # 1. Check and expire invalid subscriptions (cancelled past period_end, past_due too long)
        if run_all or args.expire_only:
            print("--- Checking Expired Subscriptions ---")
            if args.dry_run:
                print("  (dry-run: would check for expired subscriptions)")
            else:
                # Check cancelled subscriptions past their period
                expired_cancelled = SubscriptionService.check_expired_subscriptions()
                print(f"  Cancelled subscriptions expired: {expired_cancelled}")

                # Check active/past_due subscriptions that should be expired
                expire_result = SubscriptionService.check_and_expire_past_due_subscriptions()
                print(f"  Active subscriptions expired (past period_end): {expire_result.get('period_end_expired', 0)}")
                print(f"  Past_due subscriptions expired (timeout): {expire_result.get('past_due_expired', 0)}")

                if args.verbose and expire_result.get("details"):
                    print("  Details:")
                    for d in expire_result["details"]:
                        print(f"    - {d['subscription_id'][:8]}...: {d['reason']}")
            print()

        # 2. Process due credit allocations (with hardened checks)
        if run_all or args.credits_only:
            print("--- Processing Due Credit Allocations ---")
            if args.dry_run:
                print("  (dry-run: would process due credit allocations)")
            else:
                credit_result = SubscriptionService.process_due_credit_allocations()
                print(f"  Processed: {credit_result.get('processed', 0)}")
                print(f"  Granted:   {credit_result.get('granted', 0)}")
                print(f"  Errors:    {credit_result.get('errors', 0)}")
                total_errors += credit_result.get("errors", 0)

                if args.verbose and credit_result.get("details"):
                    print("  Details:")
                    for d in credit_result["details"][:10]:
                        print(f"    - {d['subscription_id'][:8]}...: +{d['credits']} credits")
            print()

        # 3. Send past_due reminder emails
        if run_all or args.reminders_only:
            print("--- Sending Past-Due Reminders ---")
            if args.dry_run:
                print("  (dry-run: would send past_due reminders)")
            else:
                reminder_result = SubscriptionService.send_past_due_reminders()
                print(f"  Reminders sent: {reminder_result.get('sent', 0)}")
                print(f"  Skipped (already sent): {reminder_result.get('skipped', 0)}")

                if args.verbose and reminder_result.get("details"):
                    print("  Details:")
                    for d in reminder_result["details"]:
                        print(f"    - {d['subscription_id'][:8]}...: {d['notification_type']}")
            print()

        # Log completion
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        print(f"[{end_time.isoformat()}] Health check completed in {duration:.1f}s")

        if total_errors > 0:
            print(f"  WARNING: {total_errors} errors encountered")
            sys.exit(1)

    except Exception as e:
        print(f"ERROR: Subscription health check failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
