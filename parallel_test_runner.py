"""
Parallel Test Runner for Snowflake Warehouse Stress Testing.

Orchestrates stress tests across multiple warehouse configurations
simultaneously to enable side-by-side comparison of Gen1 vs Gen2
and different warehouse sizes.
"""

from __future__ import annotations

import os
import sys
import time
import json
import logging
import subprocess
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml

from warehouse_manager import (
    WarehouseManager,
    WarehouseConfig,
    WarehouseSize,
    WarehouseGeneration
)

logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).parent.resolve()
TESTS_DIR = PROJECT_ROOT / "tests"
RESULTS_DIR = PROJECT_ROOT / "results"
CONFIG_PATH = TESTS_DIR / "locust_config.yaml"


@dataclass
class TestConfig:
    """Configuration for a single stress test run."""
    warehouse_name: str
    warehouse_size: str
    warehouse_generation: str
    users: int = 50
    spawn_rate: float = 2.0
    duration: str = "10m"
    query_mix: str = "mixed"
    connection_name: str = "stress_testing"


@dataclass
class TestResult:
    """Results from a single test run."""
    warehouse_name: str
    warehouse_size: str
    warehouse_generation: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    avg_response_time_ms: float = 0.0
    median_response_time_ms: float = 0.0
    p95_response_time_ms: float = 0.0
    p99_response_time_ms: float = 0.0
    max_response_time_ms: float = 0.0
    min_response_time_ms: float = 0.0
    requests_per_second: float = 0.0
    error_rate: float = 0.0
    credits_estimate: float = 0.0
    locust_report_path: Optional[str] = None
    raw_stats: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        result['start_time'] = self.start_time.isoformat()
        result['end_time'] = self.end_time.isoformat()
        return result


class ParallelTestRunner:
    """
    Orchestrates parallel stress tests across multiple warehouses.

    Features:
    - Manages warehouse lifecycle (create, resume, suspend)
    - Runs Locust tests in parallel against different warehouses
    - Collects and aggregates results for comparison
    """

    def __init__(
        self,
        connection_name: str = "stress_testing",
        results_dir: Optional[Path] = None
    ):
        """
        Initialize the test runner.

        Args:
            connection_name: Snowflake connection name from config.toml
            results_dir: Directory to store test results
        """
        self.connection_name = connection_name
        self.results_dir = results_dir or RESULTS_DIR
        self.results_dir.mkdir(parents=True, exist_ok=True)

        self.warehouse_manager = WarehouseManager(connection_name)
        self.test_results: Dict[str, TestResult] = {}

        # Load config
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load the locust configuration."""
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH) as f:
                return yaml.safe_load(f)
        return {}

    def setup_warehouses(
        self,
        sizes: List[WarehouseSize] = None,
        generations: List[WarehouseGeneration] = None
    ) -> Dict[str, bool]:
        """
        Set up all test warehouses.

        Args:
            sizes: Warehouse sizes to create
            generations: Warehouse generations to create

        Returns:
            Dict mapping warehouse names to creation status
        """
        if sizes is None:
            sizes = [WarehouseSize.XSMALL, WarehouseSize.SMALL, WarehouseSize.MEDIUM]
        if generations is None:
            generations = [WarehouseGeneration.GEN1, WarehouseGeneration.GEN2]

        logger.info("Setting up test warehouses...")
        return self.warehouse_manager.create_test_warehouses(sizes, generations)

    def warmup_warehouses(self, warehouses: List[str], warmup_seconds: int = 60):
        """
        Warm up warehouses by resuming them and running a simple query.

        Args:
            warehouses: List of warehouse names to warm up
            warmup_seconds: Seconds to wait for warmup
        """
        logger.info(f"Warming up {len(warehouses)} warehouses...")

        for wh_name in warehouses:
            self.warehouse_manager.resume_warehouse(wh_name)

        # Wait for warehouses to be fully available
        time.sleep(warmup_seconds)
        logger.info("Warehouse warmup complete")

    def _generate_locust_config(self, config: TestConfig) -> Path:
        """
        Generate a temporary Locust config file for a specific warehouse test.

        Args:
            config: Test configuration

        Returns:
            Path to the generated config file
        """
        config_content = f"""
# Auto-generated config for {config.warehouse_name}
users = {config.users}
spawn-rate = {config.spawn_rate}
run-time = {config.duration}
headless = true
autostart = true
autoquit = 10
print-stats = true
html = {self.results_dir}/{config.warehouse_name}_report.html
loglevel = INFO
connection = {config.connection_name}
"""

        config_path = TESTS_DIR / f"locust_{config.warehouse_name}.conf"
        with open(config_path, 'w') as f:
            f.write(config_content)

        return config_path

    def _run_single_test(self, config: TestConfig) -> TestResult:
        """
        Run a single Locust test against a warehouse.

        Args:
            config: Test configuration

        Returns:
            Test results
        """
        logger.info(f"Starting test for {config.warehouse_name}...")

        # Generate config file
        locust_config_path = self._generate_locust_config(config)

        # Build Locust command
        locust_file = TESTS_DIR / "locustfile-snowflake.py"
        stats_json_path = self.results_dir / f"{config.warehouse_name}_stats.json"

        cmd = [
            "locust",
            "-f", str(locust_file),
            "--config", str(locust_config_path),
            "--warehouse", config.warehouse_name,
            "--json",
        ]

        start_time = datetime.now()

        try:
            # Run Locust
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(TESTS_DIR),
                env={**os.environ, "LOCUST_WAREHOUSE": config.warehouse_name}
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Parse results from Locust output
            test_result = self._parse_locust_output(
                config, result.stdout, result.stderr, start_time, end_time
            )

            # Calculate credits estimate
            hours = duration / 3600
            credits_per_hour = {
                "XSMALL": 1, "SMALL": 2, "MEDIUM": 4,
                "LARGE": 8, "XLARGE": 16
            }.get(config.warehouse_size, 1)
            test_result.credits_estimate = hours * credits_per_hour

            logger.info(
                f"Test complete for {config.warehouse_name}: "
                f"{test_result.total_requests} requests, "
                f"{test_result.avg_response_time_ms:.2f}ms avg latency"
            )

            return test_result

        except Exception as e:
            logger.error(f"Test failed for {config.warehouse_name}: {e}")
            return TestResult(
                warehouse_name=config.warehouse_name,
                warehouse_size=config.warehouse_size,
                warehouse_generation=config.warehouse_generation,
                start_time=start_time,
                end_time=datetime.now(),
                duration_seconds=0,
                error_rate=1.0
            )
        finally:
            # Clean up temp config
            if locust_config_path.exists():
                locust_config_path.unlink()

    def _parse_locust_output(
        self,
        config: TestConfig,
        stdout: str,
        stderr: str,
        start_time: datetime,
        end_time: datetime
    ) -> TestResult:
        """
        Parse Locust output to extract metrics.

        Args:
            config: Test configuration
            stdout: Locust stdout
            stderr: Locust stderr
            start_time: Test start time
            end_time: Test end time

        Returns:
            Parsed test results
        """
        duration = (end_time - start_time).total_seconds()

        result = TestResult(
            warehouse_name=config.warehouse_name,
            warehouse_size=config.warehouse_size,
            warehouse_generation=config.warehouse_generation,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            locust_report_path=str(self.results_dir / f"{config.warehouse_name}_report.html")
        )

        # Try to parse JSON stats from stdout
        try:
            # Look for JSON in the output
            for line in stdout.split('\n'):
                if line.strip().startswith('{') and '"stats"' in line:
                    stats = json.loads(line)
                    result.raw_stats = stats

                    # Extract aggregate stats
                    if 'stats' in stats:
                        for stat in stats['stats']:
                            if stat.get('name') == 'Aggregated':
                                result.total_requests = stat.get('num_requests', 0)
                                result.failed_requests = stat.get('num_failures', 0)
                                result.successful_requests = result.total_requests - result.failed_requests
                                result.avg_response_time_ms = stat.get('avg_response_time', 0)
                                result.median_response_time_ms = stat.get('median_response_time', 0)
                                result.p95_response_time_ms = stat.get('percentile_95', 0)
                                result.p99_response_time_ms = stat.get('percentile_99', 0)
                                result.max_response_time_ms = stat.get('max_response_time', 0)
                                result.min_response_time_ms = stat.get('min_response_time', 0)
                                result.requests_per_second = stat.get('current_rps', 0)
                                break
                    break
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Could not parse Locust JSON output: {e}")

        # Calculate error rate
        if result.total_requests > 0:
            result.error_rate = result.failed_requests / result.total_requests

        return result

    def run_parallel_tests(
        self,
        warehouses: List[str] = None,
        users_per_warehouse: int = 50,
        spawn_rate: float = 2.0,
        duration: str = "10m",
        max_parallel: int = 3
    ) -> Dict[str, TestResult]:
        """
        Run tests in parallel across multiple warehouses.

        Args:
            warehouses: List of warehouse names to test
            users_per_warehouse: Number of concurrent users per warehouse
            spawn_rate: Users spawned per second
            duration: Test duration (e.g., "10m", "1h")
            max_parallel: Maximum parallel test runs

        Returns:
            Dict mapping warehouse names to test results
        """
        if warehouses is None:
            # Get all configured warehouses
            warehouse_configs = self.config.get('locust', {}).get('warehouses', [])
            warehouses = [wh['name'] for wh in warehouse_configs]

        if not warehouses:
            logger.error("No warehouses specified for testing")
            return {}

        logger.info(f"Running parallel tests on {len(warehouses)} warehouses")
        logger.info(f"Config: {users_per_warehouse} users, {duration} duration")

        # Warm up all warehouses first
        self.warmup_warehouses(warehouses, warmup_seconds=30)

        # Create test configs
        test_configs = []
        for wh_name in warehouses:
            # Parse warehouse name to get size and generation
            parts = wh_name.split('_')
            size = parts[2] if len(parts) > 2 else "XSMALL"
            gen = parts[3] if len(parts) > 3 else "GEN1"

            test_configs.append(TestConfig(
                warehouse_name=wh_name,
                warehouse_size=size,
                warehouse_generation=gen,
                users=users_per_warehouse,
                spawn_rate=spawn_rate,
                duration=duration,
                connection_name=self.connection_name
            ))

        # Run tests in parallel
        results = {}
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            future_to_config = {
                executor.submit(self._run_single_test, config): config
                for config in test_configs
            }

            for future in as_completed(future_to_config):
                config = future_to_config[future]
                try:
                    result = future.result()
                    results[config.warehouse_name] = result
                    self.test_results[config.warehouse_name] = result
                except Exception as e:
                    logger.error(f"Test failed for {config.warehouse_name}: {e}")

        # Save results
        self._save_results(results)

        return results

    def run_sequential_tests(
        self,
        warehouses: List[str] = None,
        users_per_warehouse: int = 50,
        spawn_rate: float = 2.0,
        duration: str = "10m"
    ) -> Dict[str, TestResult]:
        """
        Run tests sequentially (one warehouse at a time).

        Useful for more controlled testing where you want to minimize
        cross-warehouse interference.

        Args:
            warehouses: List of warehouse names to test
            users_per_warehouse: Number of concurrent users
            spawn_rate: Users spawned per second
            duration: Test duration

        Returns:
            Dict mapping warehouse names to test results
        """
        return self.run_parallel_tests(
            warehouses=warehouses,
            users_per_warehouse=users_per_warehouse,
            spawn_rate=spawn_rate,
            duration=duration,
            max_parallel=1
        )

    def _save_results(self, results: Dict[str, TestResult]):
        """
        Save test results to JSON file.

        Args:
            results: Dict of test results
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = self.results_dir / f"test_results_{timestamp}.json"

        serializable_results = {
            name: result.to_dict()
            for name, result in results.items()
        }

        with open(results_file, 'w') as f:
            json.dump(serializable_results, f, indent=2)

        logger.info(f"Results saved to {results_file}")

    def get_comparison_summary(self) -> Dict[str, Any]:
        """
        Generate a comparison summary of all test results.

        Returns:
            Summary dict comparing warehouse performance
        """
        if not self.test_results:
            return {}

        summary = {
            "test_timestamp": datetime.now().isoformat(),
            "total_warehouses_tested": len(self.test_results),
            "results_by_warehouse": {},
            "gen1_vs_gen2": {},
            "size_comparison": {}
        }

        # Group results by generation and size
        gen1_results = []
        gen2_results = []
        size_groups = {}

        for name, result in self.test_results.items():
            summary["results_by_warehouse"][name] = {
                "avg_latency_ms": result.avg_response_time_ms,
                "p95_latency_ms": result.p95_response_time_ms,
                "throughput_rps": result.requests_per_second,
                "error_rate": result.error_rate,
                "credits_estimate": result.credits_estimate
            }

            if "GEN1" in name:
                gen1_results.append(result)
            else:
                gen2_results.append(result)

            size = result.warehouse_size
            if size not in size_groups:
                size_groups[size] = []
            size_groups[size].append(result)

        # Gen1 vs Gen2 comparison
        if gen1_results and gen2_results:
            gen1_avg = sum(r.avg_response_time_ms for r in gen1_results) / len(gen1_results)
            gen2_avg = sum(r.avg_response_time_ms for r in gen2_results) / len(gen2_results)

            summary["gen1_vs_gen2"] = {
                "gen1_avg_latency_ms": gen1_avg,
                "gen2_avg_latency_ms": gen2_avg,
                "gen2_improvement_pct": ((gen1_avg - gen2_avg) / gen1_avg * 100) if gen1_avg > 0 else 0,
                "gen1_avg_throughput": sum(r.requests_per_second for r in gen1_results) / len(gen1_results),
                "gen2_avg_throughput": sum(r.requests_per_second for r in gen2_results) / len(gen2_results)
            }

        # Size comparison
        for size, results in size_groups.items():
            summary["size_comparison"][size] = {
                "avg_latency_ms": sum(r.avg_response_time_ms for r in results) / len(results),
                "avg_throughput_rps": sum(r.requests_per_second for r in results) / len(results),
                "total_credits": sum(r.credits_estimate for r in results)
            }

        return summary

    def cleanup(self, suspend_warehouses: bool = True):
        """
        Clean up after tests.

        Args:
            suspend_warehouses: Whether to suspend test warehouses
        """
        if suspend_warehouses:
            logger.info("Suspending test warehouses...")
            self.warehouse_manager.suspend_all_test_warehouses()

        self.warehouse_manager.close()


def run_baseline_test(
    connection_name: str = "stress_testing",
    users: int = 50,
    duration: str = "10m"
) -> Dict[str, TestResult]:
    """
    Run a baseline stress test across all warehouse configurations.

    Args:
        connection_name: Snowflake connection name
        users: Number of concurrent users per warehouse
        duration: Test duration

    Returns:
        Test results for all warehouses
    """
    runner = ParallelTestRunner(connection_name)

    try:
        # Setup warehouses (will skip if already exist)
        runner.setup_warehouses()

        # Run tests
        results = runner.run_parallel_tests(
            users_per_warehouse=users,
            duration=duration,
            max_parallel=2  # Run 2 at a time to avoid overwhelming
        )

        # Print summary
        summary = runner.get_comparison_summary()
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)

        if summary.get("gen1_vs_gen2"):
            g = summary["gen1_vs_gen2"]
            print(f"\nGen1 vs Gen2 Comparison:")
            print(f"  Gen1 Avg Latency: {g['gen1_avg_latency_ms']:.2f}ms")
            print(f"  Gen2 Avg Latency: {g['gen2_avg_latency_ms']:.2f}ms")
            print(f"  Gen2 Improvement: {g['gen2_improvement_pct']:.1f}%")

        print("\nResults by Warehouse:")
        for name, metrics in summary.get("results_by_warehouse", {}).items():
            print(f"\n  {name}:")
            print(f"    Avg Latency: {metrics['avg_latency_ms']:.2f}ms")
            print(f"    P95 Latency: {metrics['p95_latency_ms']:.2f}ms")
            print(f"    Throughput:  {metrics['throughput_rps']:.2f} req/s")
            print(f"    Error Rate:  {metrics['error_rate']:.2%}")

        return results

    finally:
        runner.cleanup(suspend_warehouses=True)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description="Parallel Snowflake Stress Test Runner")
    parser.add_argument(
        "--connection", "-c",
        default="stress_testing",
        help="Snowflake connection name"
    )
    parser.add_argument(
        "--users", "-u",
        type=int,
        default=50,
        help="Number of concurrent users per warehouse"
    )
    parser.add_argument(
        "--duration", "-d",
        default="10m",
        help="Test duration (e.g., '5m', '1h')"
    )
    parser.add_argument(
        "--warehouses", "-w",
        nargs="+",
        help="Specific warehouses to test (default: all)"
    )
    parser.add_argument(
        "--setup-only",
        action="store_true",
        help="Only set up warehouses, don't run tests"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up (suspend) warehouses after tests"
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Run tests sequentially instead of parallel"
    )

    args = parser.parse_args()

    runner = ParallelTestRunner(args.connection)

    try:
        if args.setup_only:
            print("Setting up warehouses...")
            results = runner.setup_warehouses()
            for name, success in results.items():
                status = "✓" if success else "✗"
                print(f"  {status} {name}")
        else:
            if args.sequential:
                results = runner.run_sequential_tests(
                    warehouses=args.warehouses,
                    users_per_warehouse=args.users,
                    duration=args.duration
                )
            else:
                results = runner.run_parallel_tests(
                    warehouses=args.warehouses,
                    users_per_warehouse=args.users,
                    duration=args.duration
                )

            # Print comparison
            summary = runner.get_comparison_summary()
            print(json.dumps(summary, indent=2))

    finally:
        if args.cleanup:
            runner.cleanup(suspend_warehouses=True)
        else:
            runner.warehouse_manager.close()
