#!/usr/bin/env python3
"""
Parallel Stress Test Runner

Runs stress tests on Gen1 and Gen2 warehouses simultaneously for fair comparison.
Both tests use identical parameters and run at the same time.

Usage:
    python run_comparison.py --size XSMALL --users 20 --duration 2m
    python run_comparison.py --size SMALL --users 50 --duration 5m
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def run_locust_test(warehouse: str, connection: str, users: int, spawn_rate: int, duration: str) -> dict:
    """
    Run a single Locust test and return results.

    Args:
        warehouse: Warehouse name
        connection: Snow CLI connection name
        users: Number of concurrent users
        spawn_rate: User spawn rate per second
        duration: Test duration (e.g., "2m", "5m")

    Returns:
        dict with test results
    """
    tests_dir = Path(__file__).parent / "tests"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_file = Path(__file__).parent / "results" / f"{warehouse}_{timestamp}.html"

    cmd = [
        "locust",
        "-f", str(tests_dir / "locustfile-snowflake.py"),
        "--connection", connection,
        "--warehouse", warehouse,
        "--users", str(users),
        "--spawn-rate", str(spawn_rate),
        "--run-time", duration,
        "--headless",
        "--html", str(html_file),
    ]

    print(f"🚀 Starting test on {warehouse}...")
    start_time = time.time()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        elapsed = time.time() - start_time

        return {
            "warehouse": warehouse,
            "success": result.returncode == 0,
            "elapsed_seconds": elapsed,
            "html_report": str(html_file),
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.TimeoutExpired:
        return {
            "warehouse": warehouse,
            "success": False,
            "error": "Test timed out",
        }
    except Exception as e:
        return {
            "warehouse": warehouse,
            "success": False,
            "error": str(e),
        }


def run_parallel_comparison(
    size: str,
    connection: str,
    users: int,
    spawn_rate: int,
    duration: str
) -> None:
    """
    Run Gen1 and Gen2 tests in parallel.

    Args:
        size: Warehouse size (XSMALL, SMALL, MEDIUM)
        connection: Snow CLI connection name
        users: Number of concurrent users
        spawn_rate: User spawn rate
        duration: Test duration
    """
    gen1_wh = f"STRESS_TEST_{size}_GEN1"
    gen2_wh = f"STRESS_TEST_{size}_GEN2"

    print("=" * 60)
    print("🔬 PARALLEL STRESS TEST COMPARISON")
    print("=" * 60)
    print(f"Size:       {size}")
    print(f"Users:      {users}")
    print(f"Duration:   {duration}")
    print(f"Connection: {connection}")
    print()
    print(f"Gen1: {gen1_wh}")
    print(f"Gen2: {gen2_wh}")
    print("=" * 60)
    print()

    # Run both tests in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(run_locust_test, gen1_wh, connection, users, spawn_rate, duration): "Gen1",
            executor.submit(run_locust_test, gen2_wh, connection, users, spawn_rate, duration): "Gen2",
        }

        results = {}
        for future in as_completed(futures):
            gen = futures[future]
            try:
                result = future.result()
                results[gen] = result
                if result["success"]:
                    print(f"✅ {gen} completed in {result['elapsed_seconds']:.1f}s")
                    print(f"   Report: {result['html_report']}")
                else:
                    print(f"❌ {gen} failed: {result.get('error', 'Unknown error')}")
            except Exception as e:
                print(f"❌ {gen} exception: {e}")

    print()
    print("=" * 60)
    print("📊 TESTS COMPLETE")
    print("=" * 60)
    print()
    print("View comparison in Streamlit dashboard:")
    print("  streamlit run streamlit_app.py")
    print()
    print("Or check HTML reports in results/ folder")


def main():
    parser = argparse.ArgumentParser(
        description="Run parallel Gen1 vs Gen2 stress tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Quick test (1 minute, 10 users)
    python run_comparison.py --size XSMALL --users 10 --duration 1m

    # Standard test (2 minutes, 20 users)
    python run_comparison.py --size XSMALL --users 20 --duration 2m

    # Heavy load test (5 minutes, 50 users)
    python run_comparison.py --size SMALL --users 50 --duration 5m
        """
    )

    parser.add_argument(
        "--size",
        choices=["XSMALL", "SMALL", "MEDIUM"],
        default="XSMALL",
        help="Warehouse size to test (default: XSMALL)"
    )
    parser.add_argument(
        "--users",
        type=int,
        default=20,
        help="Number of concurrent users (default: 20)"
    )
    parser.add_argument(
        "--spawn-rate",
        type=int,
        default=2,
        help="User spawn rate per second (default: 2)"
    )
    parser.add_argument(
        "--duration",
        default="2m",
        help="Test duration, e.g., 1m, 2m, 5m (default: 2m)"
    )
    parser.add_argument(
        "--connection",
        default="stress_test",
        help="Snow CLI connection name (default: stress_test)"
    )

    args = parser.parse_args()

    # Ensure results directory exists
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)

    run_parallel_comparison(
        size=args.size,
        connection=args.connection,
        users=args.users,
        spawn_rate=args.spawn_rate,
        duration=args.duration,
    )


if __name__ == "__main__":
    main()
