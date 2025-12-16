"""
Results Aggregator for Snowflake Stress Tests.

Collects, analyzes, and visualizes test results across different
warehouse configurations for Gen1 vs Gen2 and size comparisons.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from warehouse_manager import WarehouseManager

logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).parent.resolve()
RESULTS_DIR = PROJECT_ROOT / "results"


@dataclass
class QueryMetrics:
    """Metrics for a single query type."""
    query_name: str
    complexity: str
    count: int = 0
    avg_latency_ms: float = 0.0
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    min_latency_ms: float = 0.0
    error_count: int = 0
    error_rate: float = 0.0


@dataclass
class WarehouseMetrics:
    """Aggregated metrics for a warehouse."""
    warehouse_name: str
    warehouse_size: str
    warehouse_generation: str
    test_duration_seconds: float = 0.0
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    overall_avg_latency_ms: float = 0.0
    overall_p50_latency_ms: float = 0.0
    overall_p95_latency_ms: float = 0.0
    overall_p99_latency_ms: float = 0.0
    queries_per_second: float = 0.0
    error_rate: float = 0.0
    estimated_credits: float = 0.0
    queries_per_credit: float = 0.0
    query_metrics: Dict[str, QueryMetrics] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'warehouse_name': self.warehouse_name,
            'warehouse_size': self.warehouse_size,
            'warehouse_generation': self.warehouse_generation,
            'test_duration_seconds': self.test_duration_seconds,
            'total_queries': self.total_queries,
            'successful_queries': self.successful_queries,
            'failed_queries': self.failed_queries,
            'overall_avg_latency_ms': self.overall_avg_latency_ms,
            'overall_p50_latency_ms': self.overall_p50_latency_ms,
            'overall_p95_latency_ms': self.overall_p95_latency_ms,
            'overall_p99_latency_ms': self.overall_p99_latency_ms,
            'queries_per_second': self.queries_per_second,
            'error_rate': self.error_rate,
            'estimated_credits': self.estimated_credits,
            'queries_per_credit': self.queries_per_credit,
        }


class ResultsAggregator:
    """
    Aggregates and analyzes stress test results across warehouses.

    Features:
    - Load results from JSON files
    - Query QUERY_HISTORY for additional metrics
    - Compare Gen1 vs Gen2 performance
    - Generate comparison reports
    """

    CREDITS_PER_HOUR = {
        'XSMALL': 1,
        'SMALL': 2,
        'MEDIUM': 4,
        'LARGE': 8,
        'XLARGE': 16,
        '2XLARGE': 32,
    }

    def __init__(
        self,
        connection_name: str = "stress_testing",
        results_dir: Optional[Path] = None
    ):
        """
        Initialize the aggregator.

        Args:
            connection_name: Snowflake connection name
            results_dir: Directory containing test results
        """
        self.connection_name = connection_name
        self.results_dir = results_dir or RESULTS_DIR
        self.warehouse_manager = WarehouseManager(connection_name)
        self.warehouse_metrics: Dict[str, WarehouseMetrics] = {}

    def load_results_from_file(self, results_file: Path) -> Dict[str, Any]:
        """
        Load test results from a JSON file.

        Args:
            results_file: Path to results JSON

        Returns:
            Parsed results dict
        """
        with open(results_file) as f:
            return json.load(f)

    def load_latest_results(self) -> Dict[str, Any]:
        """
        Load the most recent results file.

        Returns:
            Parsed results dict
        """
        result_files = list(self.results_dir.glob("test_results_*.json"))
        if not result_files:
            logger.warning("No result files found")
            return {}

        latest = max(result_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Loading results from {latest}")
        return self.load_results_from_file(latest)

    def collect_query_history_metrics(
        self,
        warehouse_name: str,
        hours_back: int = 2
    ) -> WarehouseMetrics:
        """
        Collect metrics from QUERY_HISTORY for a warehouse.

        Args:
            warehouse_name: Target warehouse
            hours_back: Hours of history to analyze

        Returns:
            Warehouse metrics
        """
        summary = self.warehouse_manager.get_query_performance_summary(
            warehouse_name, hours_back
        )

        # Parse warehouse name for size and generation
        parts = warehouse_name.split('_')
        size = parts[2] if len(parts) > 2 else "UNKNOWN"
        gen = parts[3] if len(parts) > 3 else "UNKNOWN"

        metrics = WarehouseMetrics(
            warehouse_name=warehouse_name,
            warehouse_size=size,
            warehouse_generation=gen,
            total_queries=summary.get('total_queries', 0),
            successful_queries=summary.get('successful_queries', 0),
            failed_queries=summary.get('failed_queries', 0),
            overall_avg_latency_ms=summary.get('avg_elapsed_ms', 0),
            overall_p50_latency_ms=summary.get('median_elapsed_ms', 0),
            overall_p95_latency_ms=summary.get('p95_elapsed_ms', 0),
            overall_p99_latency_ms=summary.get('p99_elapsed_ms', 0),
        )

        # Calculate error rate
        if metrics.total_queries > 0:
            metrics.error_rate = metrics.failed_queries / metrics.total_queries

        return metrics

    def collect_all_warehouse_metrics(
        self,
        warehouses: List[str] = None,
        hours_back: int = 2
    ) -> Dict[str, WarehouseMetrics]:
        """
        Collect metrics for all test warehouses.

        Args:
            warehouses: List of warehouse names (default: all test warehouses)
            hours_back: Hours of history to analyze

        Returns:
            Dict mapping warehouse names to metrics
        """
        if warehouses is None:
            wh_list = self.warehouse_manager.list_test_warehouses()
            warehouses = [wh['name'] for wh in wh_list]

        for wh_name in warehouses:
            logger.info(f"Collecting metrics for {wh_name}")
            self.warehouse_metrics[wh_name] = self.collect_query_history_metrics(
                wh_name, hours_back
            )

        return self.warehouse_metrics

    def compare_gen1_vs_gen2(self) -> Dict[str, Any]:
        """
        Compare Gen1 and Gen2 warehouse performance.

        Returns:
            Comparison results
        """
        gen1_metrics = []
        gen2_metrics = []

        for name, metrics in self.warehouse_metrics.items():
            if 'GEN1' in name:
                gen1_metrics.append(metrics)
            elif 'GEN2' in name:
                gen2_metrics.append(metrics)

        if not gen1_metrics or not gen2_metrics:
            return {"error": "Need both Gen1 and Gen2 results for comparison"}

        # Calculate averages
        gen1_avg_latency = sum(m.overall_avg_latency_ms for m in gen1_metrics) / len(gen1_metrics)
        gen2_avg_latency = sum(m.overall_avg_latency_ms for m in gen2_metrics) / len(gen2_metrics)

        gen1_avg_p95 = sum(m.overall_p95_latency_ms for m in gen1_metrics) / len(gen1_metrics)
        gen2_avg_p95 = sum(m.overall_p95_latency_ms for m in gen2_metrics) / len(gen2_metrics)

        gen1_total_queries = sum(m.total_queries for m in gen1_metrics)
        gen2_total_queries = sum(m.total_queries for m in gen2_metrics)

        gen1_error_rate = sum(m.error_rate for m in gen1_metrics) / len(gen1_metrics)
        gen2_error_rate = sum(m.error_rate for m in gen2_metrics) / len(gen2_metrics)

        latency_improvement = ((gen1_avg_latency - gen2_avg_latency) / gen1_avg_latency * 100) if gen1_avg_latency > 0 else 0
        p95_improvement = ((gen1_avg_p95 - gen2_avg_p95) / gen1_avg_p95 * 100) if gen1_avg_p95 > 0 else 0

        return {
            "gen1": {
                "warehouses_tested": len(gen1_metrics),
                "total_queries": gen1_total_queries,
                "avg_latency_ms": round(gen1_avg_latency, 2),
                "avg_p95_latency_ms": round(gen1_avg_p95, 2),
                "avg_error_rate": round(gen1_error_rate, 4),
            },
            "gen2": {
                "warehouses_tested": len(gen2_metrics),
                "total_queries": gen2_total_queries,
                "avg_latency_ms": round(gen2_avg_latency, 2),
                "avg_p95_latency_ms": round(gen2_avg_p95, 2),
                "avg_error_rate": round(gen2_error_rate, 4),
            },
            "comparison": {
                "latency_improvement_pct": round(latency_improvement, 2),
                "p95_improvement_pct": round(p95_improvement, 2),
                "gen2_faster": gen2_avg_latency < gen1_avg_latency,
            }
        }

    def compare_sizes(self) -> Dict[str, Any]:
        """
        Compare performance across warehouse sizes.

        Returns:
            Size comparison results
        """
        by_size: Dict[str, List[WarehouseMetrics]] = {}

        for name, metrics in self.warehouse_metrics.items():
            size = metrics.warehouse_size
            if size not in by_size:
                by_size[size] = []
            by_size[size].append(metrics)

        comparison = {}

        for size, metrics_list in by_size.items():
            avg_latency = sum(m.overall_avg_latency_ms for m in metrics_list) / len(metrics_list)
            avg_p95 = sum(m.overall_p95_latency_ms for m in metrics_list) / len(metrics_list)
            total_queries = sum(m.total_queries for m in metrics_list)
            credits_per_hour = self.CREDITS_PER_HOUR.get(size, 1)

            comparison[size] = {
                "warehouses_tested": len(metrics_list),
                "total_queries": total_queries,
                "avg_latency_ms": round(avg_latency, 2),
                "avg_p95_latency_ms": round(avg_p95, 2),
                "credits_per_hour": credits_per_hour,
                "queries_per_credit_estimate": round(total_queries / credits_per_hour, 2) if credits_per_hour > 0 else 0,
            }

        return comparison

    def generate_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive comparison report.

        Returns:
            Full report dict
        """
        report = {
            "generated_at": datetime.now().isoformat(),
            "warehouses_analyzed": len(self.warehouse_metrics),
            "individual_results": {
                name: metrics.to_dict()
                for name, metrics in self.warehouse_metrics.items()
            },
            "gen1_vs_gen2": self.compare_gen1_vs_gen2(),
            "size_comparison": self.compare_sizes(),
            "recommendations": self._generate_recommendations(),
        }

        return report

    def _generate_recommendations(self) -> List[str]:
        """
        Generate recommendations based on test results.

        Returns:
            List of recommendation strings
        """
        recommendations = []

        gen_comparison = self.compare_gen1_vs_gen2()
        if gen_comparison.get("comparison", {}).get("gen2_faster"):
            improvement = gen_comparison["comparison"]["latency_improvement_pct"]
            recommendations.append(
                f"Gen2 warehouses show {improvement:.1f}% latency improvement. "
                "Consider migrating production workloads to Gen2."
            )

        size_comparison = self.compare_sizes()

        # Find best cost efficiency
        best_efficiency = None
        best_size = None
        for size, data in size_comparison.items():
            efficiency = data.get("queries_per_credit_estimate", 0)
            if best_efficiency is None or efficiency > best_efficiency:
                best_efficiency = efficiency
                best_size = size

        if best_size:
            recommendations.append(
                f"{best_size} warehouses show best cost efficiency "
                f"at {best_efficiency:.0f} queries per credit."
            )

        # Check for high error rates
        for name, metrics in self.warehouse_metrics.items():
            if metrics.error_rate > 0.01:  # > 1% errors
                recommendations.append(
                    f"Warning: {name} has {metrics.error_rate:.2%} error rate. "
                    "Investigate query failures."
                )

        return recommendations

    def save_report(self, output_file: Optional[Path] = None) -> Path:
        """
        Save the comparison report to a JSON file.

        Args:
            output_file: Output path (default: auto-generated)

        Returns:
            Path to saved report
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.results_dir / f"comparison_report_{timestamp}.json"

        self.results_dir.mkdir(parents=True, exist_ok=True)

        report = self.generate_report()

        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)

        logger.info(f"Report saved to {output_file}")
        return output_file

    def print_summary(self):
        """Print a formatted summary to console."""
        print("\n" + "=" * 70)
        print("SNOWFLAKE STRESS TEST RESULTS SUMMARY")
        print("=" * 70)

        gen_comparison = self.compare_gen1_vs_gen2()

        if "gen1" in gen_comparison and "gen2" in gen_comparison:
            print("\n--- Gen1 vs Gen2 Comparison ---")
            print(f"{'Metric':<30} {'Gen1':>15} {'Gen2':>15} {'Improvement':>15}")
            print("-" * 75)

            g1 = gen_comparison["gen1"]
            g2 = gen_comparison["gen2"]
            comp = gen_comparison.get("comparison", {})

            print(f"{'Avg Latency (ms)':<30} {g1['avg_latency_ms']:>15.2f} {g2['avg_latency_ms']:>15.2f} {comp.get('latency_improvement_pct', 0):>14.1f}%")
            print(f"{'P95 Latency (ms)':<30} {g1['avg_p95_latency_ms']:>15.2f} {g2['avg_p95_latency_ms']:>15.2f} {comp.get('p95_improvement_pct', 0):>14.1f}%")
            print(f"{'Total Queries':<30} {g1['total_queries']:>15} {g2['total_queries']:>15}")
            print(f"{'Error Rate':<30} {g1['avg_error_rate']:>15.4f} {g2['avg_error_rate']:>15.4f}")

        size_comparison = self.compare_sizes()

        if size_comparison:
            print("\n--- Size Comparison ---")
            print(f"{'Size':<15} {'Queries':>12} {'Avg Latency':>15} {'P95 Latency':>15} {'Q/Credit':>12}")
            print("-" * 70)

            for size in ['XSMALL', 'SMALL', 'MEDIUM', 'LARGE']:
                if size in size_comparison:
                    data = size_comparison[size]
                    print(f"{size:<15} {data['total_queries']:>12} {data['avg_latency_ms']:>15.2f} {data['avg_p95_latency_ms']:>15.2f} {data['queries_per_credit_estimate']:>12.0f}")

        print("\n--- Individual Warehouse Results ---")
        print(f"{'Warehouse':<35} {'Queries':>10} {'Avg(ms)':>10} {'P95(ms)':>10} {'Errors':>10}")
        print("-" * 75)

        for name, metrics in sorted(self.warehouse_metrics.items()):
            print(f"{name:<35} {metrics.total_queries:>10} {metrics.overall_avg_latency_ms:>10.2f} {metrics.overall_p95_latency_ms:>10.2f} {metrics.error_rate:>10.2%}")

        # Recommendations
        recommendations = self._generate_recommendations()
        if recommendations:
            print("\n--- Recommendations ---")
            for i, rec in enumerate(recommendations, 1):
                print(f"{i}. {rec}")

        print("\n" + "=" * 70)

    def close(self):
        """Close connections."""
        self.warehouse_manager.close()


def analyze_results(
    connection_name: str = "stress_testing",
    hours_back: int = 2
):
    """
    Analyze recent test results and print summary.

    Args:
        connection_name: Snowflake connection name
        hours_back: Hours of query history to analyze
    """
    aggregator = ResultsAggregator(connection_name)

    try:
        # Collect metrics from query history
        aggregator.collect_all_warehouse_metrics(hours_back=hours_back)

        # Print summary
        aggregator.print_summary()

        # Save report
        report_path = aggregator.save_report()
        print(f"\nFull report saved to: {report_path}")

    finally:
        aggregator.close()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description="Analyze Snowflake stress test results")
    parser.add_argument(
        "--connection", "-c",
        default="stress_testing",
        help="Snowflake connection name"
    )
    parser.add_argument(
        "--hours", "-H",
        type=int,
        default=2,
        help="Hours of query history to analyze"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file path for report"
    )

    args = parser.parse_args()

    aggregator = ResultsAggregator(args.connection)

    try:
        print("Collecting warehouse metrics from QUERY_HISTORY...")
        aggregator.collect_all_warehouse_metrics(hours_back=args.hours)

        aggregator.print_summary()

        output_path = Path(args.output) if args.output else None
        report_path = aggregator.save_report(output_path)
        print(f"\nReport saved to: {report_path}")

    finally:
        aggregator.close()
