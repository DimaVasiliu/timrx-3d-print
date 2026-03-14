#!/usr/bin/env python3
"""
Daily Reconciliation Script
----------------------------
Runs the full reconciliation process to ensure all Mollie payments
are properly reflected in the database.

This script should be run daily via cron:
    0 3 * * * cd /path/to/meshy && python scripts/daily_reconciliation.py >> /var/log/timrx-reconcile.log 2>&1

Or manually with options:
    # Full reconciliation (safety + Mollie)
    python scripts/daily_reconciliation.py

    # Dry-run mode (detect only, no fixes)
    python scripts/daily_reconciliation.py --dry-run

    # Mollie reconciliation only
    python scripts/daily_reconciliation.py --mollie-only

    # Safety reconciliation only (DB consistency)
    python scripts/daily_reconciliation.py --safety-only

    # Custom days back (default: 30)
    python scripts/daily_reconciliation.py --days 7

    # Skip email alerts
    python scripts/daily_reconciliation.py --no-alert

Environment variables:
    DATABASE_URL: PostgreSQL connection string
    MOLLIE_API_KEY: Mollie API key for fetching payments
    ADMIN_EMAIL: Email address for alerts (optional)
"""
import argparse
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    parser = argparse.ArgumentParser(
        description="Run daily payment reconciliation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect issues but don't apply fixes",
    )
    parser.add_argument(
        "--mollie-only",
        action="store_true",
        help="Only run Mollie API reconciliation (skip safety checks)",
    )
    parser.add_argument(
        "--safety-only",
        action="store_true",
        help="Only run safety reconciliation (skip Mollie API)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="How many days back to scan Mollie payments (default: 30)",
    )
    parser.add_argument(
        "--no-alert",
        action="store_true",
        help="Don't send email alerts",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print detailed output",
    )
    parser.add_argument(
        "--include-wallet-audit",
        action="store_true",
        help="Also run wallet drift audit after reconciliation",
    )
    args = parser.parse_args()

    # Validate arguments
    if args.mollie_only and args.safety_only:
        print("ERROR: Cannot specify both --mollie-only and --safety-only")
        sys.exit(1)

    if args.days < 1 or args.days > 90:
        print("ERROR: --days must be between 1 and 90")
        sys.exit(1)

    # Log start
    start_time = datetime.utcnow()
    print(f"[{start_time.isoformat()}] Starting reconciliation...")
    print(f"  Mode: {'dry-run' if args.dry_run else 'apply'}")
    print(f"  Type: {'mollie-only' if args.mollie_only else 'safety-only' if args.safety_only else 'full'}")
    print(f"  Days: {args.days}")
    print(f"  Alert: {'no' if args.no_alert else 'yes'}")
    print()

    try:
        from backend.services.reconciliation_service import ReconciliationService

        if args.mollie_only:
            # Mollie API reconciliation only
            result = ReconciliationService.reconcile_mollie_payments(
                days_back=args.days,
                dry_run=args.dry_run,
                run_type="mollie_only",
            )
            print_mollie_result(result, args.verbose)

        elif args.safety_only:
            # Safety reconciliation only
            result = ReconciliationService.reconcile_safety(
                dry_run=args.dry_run,
                send_alert=not args.no_alert,
            )
            print_safety_result(result, args.verbose)

        else:
            # Full reconciliation
            result = ReconciliationService.reconcile_full(
                days_back=args.days,
                dry_run=args.dry_run,
                send_alert=not args.no_alert,
            )
            print_full_result(result, args.verbose)

        # Optional: Run wallet drift audit
        if args.include_wallet_audit:
            print()
            print("--- Wallet Drift Audit ---")
            from backend.services.wallet_drift_service import WalletDriftService

            wallet_result = WalletDriftService.run_daily_wallet_audit(
                dry_run=args.dry_run
            )
            print_wallet_audit_result(wallet_result, args.verbose)

        # Log completion
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        print()
        print(f"[{end_time.isoformat()}] Reconciliation completed in {duration:.1f}s")

        # Exit with error code if there were errors
        errors = result.get("errors_count", 0) or result.get("errors", 0)
        if errors > 0:
            print(f"  WARNING: {errors} errors encountered")
            sys.exit(1)

    except Exception as e:
        print(f"ERROR: Reconciliation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)


def print_safety_result(result: dict, verbose: bool = False):
    """Print safety reconciliation results."""
    print("=== Safety Reconciliation Results ===")
    print(f"  Purchases missing ledger: {result.get('purchases_missing_ledger', 0)} (fixed: {result.get('purchases_fixed', 0)})")
    print(f"  Wallet mismatches: {result.get('wallet_mismatches', 0)} (fixed: {result.get('wallets_fixed', 0)})")
    print(f"  Stale reservations: {result.get('stale_reservations', 0)} (released: {result.get('reservations_released', 0)})")
    print(f"  Jobs missing history: {result.get('jobs_missing_history', 0)} (fixed: {result.get('history_items_fixed', 0)})")

    if verbose and result.get("details"):
        print()
        print("  Details:")
        for key, value in result.get("details", {}).items():
            print(f"    {key}: {value}")


def print_mollie_result(result: dict, verbose: bool = False):
    """Print Mollie reconciliation results."""
    print("=== Mollie Reconciliation Results ===")
    print(f"  Payments scanned: {result.get('scanned_count', 0)}")
    print(f"  Fixes applied: {result.get('fixed_count', 0)}")
    print(f"  Errors: {result.get('errors_count', 0)}")
    print()
    print("  Breakdown:")
    print(f"    Purchases created: {result.get('purchases_fixed', 0)}")
    print(f"    Subscriptions granted: {result.get('subscriptions_fixed', 0)}")
    print(f"    Refunds applied: {result.get('refunds_fixed', 0)}")
    print(f"    Wallets corrected: {result.get('wallets_fixed', 0)}")

    if result.get("run_id"):
        print()
        print(f"  Run ID: {result.get('run_id')}")

    if verbose and result.get("notes"):
        print()
        print(f"  Notes: {result.get('notes')}")


def print_full_result(result: dict, verbose: bool = False):
    """Print full reconciliation results."""
    print("=== Full Reconciliation Results ===")
    print()

    if "safety" in result:
        print("--- Safety Phase ---")
        print_safety_result(result["safety"], verbose)
        print()

    if "mollie" in result:
        print("--- Mollie Phase ---")
        print_mollie_result(result["mollie"], verbose)


def print_wallet_audit_result(result: dict, verbose: bool = False):
    """Print wallet drift audit results."""
    print(f"  Total wallets:       {result.get('total_wallets', 0)}")
    print(f"  Drifts found:        {result.get('drifts_found', 0)}")
    print(f"  Total drift amount:  {result.get('total_drift_amount', 0)}")

    if result.get("dry_run"):
        print("  Mode: DRY-RUN (no repairs applied)")
    else:
        print(f"  Repairs applied:     {result.get('repairs_applied', 0)}")
        print(f"  Repair failures:     {result.get('repair_failures', 0)}")

    if verbose and result.get("repairs"):
        print()
        print("  Repairs made:")
        for r in result["repairs"][:10]:
            if r.get("repaired"):
                print(f"    - {r['identity_id'][:8]}...: drift={r['drift_amount']:+d}")


if __name__ == "__main__":
    main()
