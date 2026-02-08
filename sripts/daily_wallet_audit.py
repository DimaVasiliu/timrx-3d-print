#!/usr/bin/env python3
"""
Daily Wallet Audit Script
--------------------------
Runs the wallet drift detection and repair process to ensure all wallet
balances match their ledger sums.

The wallet.balance_credits is a cached value for performance. This script
ensures it stays in sync with the ledger (immutable source of truth).

This script should be run daily via cron:
    0 4 * * * cd /path/to/meshy && python scripts/daily_wallet_audit.py >> /var/log/timrx-wallet-audit.log 2>&1

Or manually with options:
    # Full audit (detect and repair)
    python scripts/daily_wallet_audit.py

    # Dry-run mode (detect only, no repairs)
    python scripts/daily_wallet_audit.py --dry-run

    # Verbose output
    python scripts/daily_wallet_audit.py --verbose

    # Check a specific identity
    python scripts/daily_wallet_audit.py --identity <uuid>

    # Show repair history
    python scripts/daily_wallet_audit.py --stats

Environment variables:
    DATABASE_URL: PostgreSQL connection string
"""
import argparse
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    parser = argparse.ArgumentParser(
        description="Run daily wallet drift audit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect drifts but don't repair them",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print detailed output",
    )
    parser.add_argument(
        "--identity",
        type=str,
        help="Check/repair a specific identity UUID",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show repair statistics instead of running audit",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="For --stats: how many days back to look (default: 30)",
    )
    args = parser.parse_args()

    # Log start
    start_time = datetime.utcnow()
    print(f"[{start_time.isoformat()}] Wallet Drift Audit")
    print(f"  Mode: {'dry-run' if args.dry_run else 'apply'}")
    print()

    try:
        from backend.services.wallet_drift_service import WalletDriftService

        if args.stats:
            # Show statistics
            print_stats(WalletDriftService, args.days)
            return

        if args.identity:
            # Check/repair specific identity
            result = check_single_identity(WalletDriftService, args.identity, args.dry_run, args.verbose)
        else:
            # Full audit
            result = WalletDriftService.run_daily_wallet_audit(dry_run=args.dry_run)
            print_audit_result(result, args.verbose)

        # Log completion
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        print()
        print(f"[{end_time.isoformat()}] Audit completed in {duration:.1f}s")

        # Exit with error code if there were failures
        if not args.stats and not args.identity:
            failures = result.get("repair_failures", 0)
            if failures > 0:
                print(f"  WARNING: {failures} repair failures")
                sys.exit(1)

    except Exception as e:
        print(f"ERROR: Wallet audit failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)


def check_single_identity(service, identity_id: str, dry_run: bool, verbose: bool):
    """Check or repair a single identity."""
    print(f"Checking identity: {identity_id}")
    print()

    # Get comparison
    comparison = service.get_wallet_comparison(identity_id)
    if not comparison:
        print(f"  ERROR: No wallet found for identity {identity_id}")
        return {"error": "Wallet not found"}

    print(f"  Cached balance: {comparison.get('cached_balance', 0)}")
    print(f"  Ledger sum:     {comparison.get('ledger_sum', 0)}")
    print(f"  Drift:          {comparison.get('drift', 0):+d}")
    print(f"  Entry count:    {comparison.get('entry_count', 0)}")
    print()

    if not comparison.get("has_drift"):
        print("  ✓ No drift detected - wallet is consistent")
        return {"repaired": False, "drift": 0}

    if dry_run:
        print("  ⚠ Drift detected (dry-run mode - no repair applied)")
        return {"repaired": False, "drift": comparison.get("drift", 0)}

    # Repair
    print("  Repairing...")
    result = service.repair_wallet(
        identity_id=identity_id,
        reason="manual_repair",
        trigger_source="cli",
    )

    if result.get("repaired"):
        print(f"  ✓ Repaired: {result['old_balance']} -> {result['new_balance']}")
        print(f"    Repair ID: {result.get('repair_id')}")
    else:
        print(f"  ✗ Repair failed or not needed")
        if result.get("error"):
            print(f"    Error: {result['error']}")

    return result


def print_audit_result(result: dict, verbose: bool = False):
    """Print audit results."""
    print("=== Wallet Drift Audit Results ===")
    print(f"  Total wallets:       {result.get('total_wallets', 0)}")
    print(f"  Drifts found:        {result.get('drifts_found', 0)}")
    print(f"  Total drift amount:  {result.get('total_drift_amount', 0)}")
    print()

    if result.get("dry_run"):
        print("  Mode: DRY-RUN (no repairs applied)")
        if verbose and result.get("drifts"):
            print()
            print("  Drifts detected:")
            for d in result["drifts"][:20]:  # Show first 20
                print(f"    - {d['identity_id'][:8]}...: {d['cached_balance']} vs {d['ledger_sum']} (drift: {d['drift']:+d})")
            if len(result["drifts"]) > 20:
                print(f"    ... and {len(result['drifts']) - 20} more")
    else:
        print(f"  Repairs applied:     {result.get('repairs_applied', 0)}")
        print(f"  Repair failures:     {result.get('repair_failures', 0)}")

        if verbose and result.get("repairs"):
            print()
            print("  Repairs made:")
            for r in result["repairs"][:20]:  # Show first 20
                if r.get("repaired"):
                    print(f"    - {r['identity_id'][:8]}...: {r['old_balance']} -> {r['new_balance']} (drift: {r['drift_amount']:+d})")
                else:
                    print(f"    - {r['identity_id'][:8]}...: FAILED - {r.get('error', 'unknown')}")


def print_stats(service, days: int):
    """Print repair statistics."""
    print(f"=== Wallet Drift Statistics (last {days} days) ===")
    print()

    # Current drifts
    current_drifts = service.count_drifts()
    print(f"  Current drifts:        {current_drifts}")
    print()

    # Historical stats
    stats = service.get_repair_stats(days=days)
    print(f"  Total repairs:         {stats.get('total_repairs', 0)}")
    print(f"  Total drift corrected: {stats.get('total_drift_corrected', 0)}")
    print()

    # By reason
    if stats.get("by_reason"):
        print("  By reason:")
        for r in stats["by_reason"]:
            print(f"    - {r['reason']}: {r['count']} repairs (drift: {r['drift_total']})")
        print()

    # By trigger
    if stats.get("by_trigger"):
        print("  By trigger source:")
        for t in stats["by_trigger"]:
            print(f"    - {t['trigger_source']}: {t['count']} repairs (drift: {t['drift_total']})")
        print()

    # Show current drifts if any
    if current_drifts > 0:
        print("  Current drifts (first 10):")
        drifts = service.find_drifts(limit=10)
        for d in drifts:
            print(f"    - {str(d['identity_id'])[:8]}...: {d.get('cached_balance', 0)} vs {d.get('ledger_sum', 0)} (drift: {d.get('drift', 0):+d})")


if __name__ == "__main__":
    main()
