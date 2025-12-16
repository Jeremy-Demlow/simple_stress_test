"""
Warehouse Manager for Snowflake Stress Testing.

Uses Snowpark (via snowflake_connection.py) to manage warehouse lifecycle
for parallel stress testing across different warehouse configurations.
"""

from __future__ import annotations

import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from enum import Enum

from snowflake_connection import SnowflakeConnection, ConnectionConfig

logger = logging.getLogger(__name__)


class WarehouseGeneration(Enum):
    """Warehouse generation types."""
    GEN1 = "STANDARD_GEN_1"
    GEN2 = "STANDARD_GEN_2"


class WarehouseSize(Enum):
    """Standard warehouse sizes."""
    XSMALL = "XSMALL"
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"
    XLARGE = "XLARGE"


@dataclass
class WarehouseConfig:
    """Configuration for a test warehouse."""
    name: str
    size: WarehouseSize
    generation: WarehouseGeneration
    auto_suspend: int = 60
    auto_resume: bool = True
    min_cluster_count: int = 1
    max_cluster_count: int = 1

    @property
    def full_name(self) -> str:
        """Generate full warehouse name with prefix."""
        return f"STRESS_TEST_{self.size.value}_{self.generation.name}"


class WarehouseManager:
    """
    Manages Snowflake warehouses for stress testing.

    Uses Snowpark connection for warehouse DDL operations while
    keeping the raw connector for high-throughput load testing.
    """

    WAREHOUSE_PREFIX = "STRESS_TEST"

    def __init__(self, connection_name: str = "stress_testing"):
        """
        Initialize warehouse manager.

        Args:
            connection_name: Name of connection in Snow CLI config.toml
        """
        self.connection_name = connection_name
        self._conn: Optional[SnowflakeConnection] = None

    @property
    def conn(self) -> SnowflakeConnection:
        """Lazy connection initialization."""
        if self._conn is None:
            self._conn = SnowflakeConnection.from_snow_cli(self.connection_name)
            logger.info(f"Connected to Snowflake via {self.connection_name}")
        return self._conn

    def close(self):
        """Close the Snowpark connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create_warehouse(self, config: WarehouseConfig) -> bool:
        """
        Create a warehouse with the specified configuration.

        Args:
            config: Warehouse configuration

        Returns:
            True if successful, False otherwise
        """
        warehouse_name = config.full_name

        sql = f"""
        CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}
            WAREHOUSE_SIZE = '{config.size.value}'
            RESOURCE_CONSTRAINT = {config.generation.value}
            AUTO_SUSPEND = {config.auto_suspend}
            AUTO_RESUME = {str(config.auto_resume).upper()}
            MIN_CLUSTER_COUNT = {config.min_cluster_count}
            MAX_CLUSTER_COUNT = {config.max_cluster_count}
            INITIALLY_SUSPENDED = TRUE
        """

        try:
            self.conn.execute(sql)
            logger.info(f"Created warehouse: {warehouse_name} ({config.size.value}, {config.generation.name})")
            return True
        except Exception as e:
            logger.error(f"Failed to create warehouse {warehouse_name}: {e}")
            return False

    def create_test_warehouses(
        self,
        sizes: List[WarehouseSize] = None,
        generations: List[WarehouseGeneration] = None
    ) -> Dict[str, bool]:
        """
        Create all test warehouses for the specified sizes and generations.

        Args:
            sizes: List of warehouse sizes (default: XS, S, M)
            generations: List of generations (default: GEN1, GEN2)

        Returns:
            Dict mapping warehouse names to creation success status
        """
        if sizes is None:
            sizes = [WarehouseSize.XSMALL, WarehouseSize.SMALL, WarehouseSize.MEDIUM]
        if generations is None:
            generations = [WarehouseGeneration.GEN1, WarehouseGeneration.GEN2]

        results = {}

        for size in sizes:
            for gen in generations:
                config = WarehouseConfig(
                    name=f"{self.WAREHOUSE_PREFIX}_{size.value}_{gen.name}",
                    size=size,
                    generation=gen
                )
                results[config.full_name] = self.create_warehouse(config)

        logger.info(f"Created {sum(results.values())}/{len(results)} warehouses")
        return results

    def resize_warehouse(self, warehouse_name: str, new_size: WarehouseSize) -> bool:
        """
        Resize an existing warehouse.

        Args:
            warehouse_name: Name of warehouse to resize
            new_size: New warehouse size

        Returns:
            True if successful
        """
        sql = f"ALTER WAREHOUSE {warehouse_name} SET WAREHOUSE_SIZE = '{new_size.value}'"

        try:
            self.conn.execute(sql)
            logger.info(f"Resized warehouse {warehouse_name} to {new_size.value}")
            return True
        except Exception as e:
            logger.error(f"Failed to resize warehouse {warehouse_name}: {e}")
            return False

    def set_generation(self, warehouse_name: str, generation: WarehouseGeneration) -> bool:
        """
        Change warehouse generation (Gen1 vs Gen2).

        Args:
            warehouse_name: Name of warehouse
            generation: Target generation

        Returns:
            True if successful
        """
        sql = f"ALTER WAREHOUSE {warehouse_name} SET RESOURCE_CONSTRAINT = {generation.value}"

        try:
            self.conn.execute(sql)
            logger.info(f"Set warehouse {warehouse_name} to {generation.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to set generation for {warehouse_name}: {e}")
            return False

    def resume_warehouse(self, warehouse_name: str) -> bool:
        """Resume a suspended warehouse."""
        try:
            self.conn.execute(f"ALTER WAREHOUSE {warehouse_name} RESUME")
            logger.info(f"Resumed warehouse: {warehouse_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to resume warehouse {warehouse_name}: {e}")
            return False

    def suspend_warehouse(self, warehouse_name: str) -> bool:
        """Suspend a running warehouse."""
        try:
            self.conn.execute(f"ALTER WAREHOUSE {warehouse_name} SUSPEND")
            logger.info(f"Suspended warehouse: {warehouse_name}")
            return True
        except Exception as e:
            # May fail if already suspended
            logger.debug(f"Could not suspend warehouse {warehouse_name}: {e}")
            return False

    def suspend_all_test_warehouses(self) -> int:
        """
        Suspend all test warehouses.

        Returns:
            Number of warehouses suspended
        """
        warehouses = self.list_test_warehouses()
        suspended = 0

        for wh in warehouses:
            if self.suspend_warehouse(wh['name']):
                suspended += 1

        logger.info(f"Suspended {suspended} test warehouses")
        return suspended

    def resume_all_test_warehouses(self) -> int:
        """
        Resume all test warehouses.

        Returns:
            Number of warehouses resumed
        """
        warehouses = self.list_test_warehouses()
        resumed = 0

        for wh in warehouses:
            if self.resume_warehouse(wh['name']):
                resumed += 1

        logger.info(f"Resumed {resumed} test warehouses")
        return resumed

    def drop_warehouse(self, warehouse_name: str) -> bool:
        """Drop a warehouse."""
        try:
            self.conn.execute(f"DROP WAREHOUSE IF EXISTS {warehouse_name}")
            logger.info(f"Dropped warehouse: {warehouse_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to drop warehouse {warehouse_name}: {e}")
            return False

    def drop_all_test_warehouses(self) -> int:
        """
        Drop all test warehouses.

        Returns:
            Number of warehouses dropped
        """
        warehouses = self.list_test_warehouses()
        dropped = 0

        for wh in warehouses:
            if self.drop_warehouse(wh['name']):
                dropped += 1

        logger.info(f"Dropped {dropped} test warehouses")
        return dropped

    def list_test_warehouses(self) -> List[Dict[str, Any]]:
        """
        List all test warehouses.

        Returns:
            List of warehouse info dicts
        """
        sql = f"SHOW WAREHOUSES LIKE '{self.WAREHOUSE_PREFIX}%'"

        try:
            results = self.conn.fetch(sql)
            warehouses = []
            for row in results:
                warehouses.append({
                    'name': row['name'],
                    'size': row['size'],
                    'state': row['state'],
                    'type': row['type'],
                    'resource_constraint': row.get('resource_constraint', 'N/A'),
                    'running': row['running'],
                    'queued': row['queued']
                })
            return warehouses
        except Exception as e:
            logger.error(f"Failed to list warehouses: {e}")
            return []

    def get_warehouse_status(self, warehouse_name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed status of a warehouse.

        Args:
            warehouse_name: Name of warehouse

        Returns:
            Warehouse status dict or None
        """
        warehouses = self.list_test_warehouses()
        for wh in warehouses:
            if wh['name'] == warehouse_name:
                return wh
        return None

    def get_warehouse_metrics(
        self,
        warehouse_name: str,
        hours_back: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Get query metrics from QUERY_HISTORY for a warehouse.

        Args:
            warehouse_name: Name of warehouse
            hours_back: How many hours of history to fetch

        Returns:
            List of query metrics
        """
        sql = f"""
        SELECT
            QUERY_ID,
            QUERY_TEXT,
            DATABASE_NAME,
            SCHEMA_NAME,
            QUERY_TYPE,
            SESSION_ID,
            USER_NAME,
            WAREHOUSE_NAME,
            WAREHOUSE_SIZE,
            EXECUTION_STATUS,
            ERROR_CODE,
            ERROR_MESSAGE,
            START_TIME,
            END_TIME,
            TOTAL_ELAPSED_TIME,
            BYTES_SCANNED,
            ROWS_PRODUCED,
            COMPILATION_TIME,
            EXECUTION_TIME,
            QUEUED_PROVISIONING_TIME,
            QUEUED_REPAIR_TIME,
            QUEUED_OVERLOAD_TIME,
            TRANSACTION_BLOCKED_TIME,
            CREDITS_USED_CLOUD_SERVICES
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
            DATEADD('hours', -{hours_back}, CURRENT_TIMESTAMP()),
            CURRENT_TIMESTAMP(),
            RESULT_LIMIT => 10000
        ))
        WHERE WAREHOUSE_NAME = '{warehouse_name}'
        ORDER BY START_TIME DESC
        """

        try:
            results = self.conn.fetch(sql)
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Failed to get metrics for {warehouse_name}: {e}")
            return []

    def get_query_performance_summary(
        self,
        warehouse_name: str,
        hours_back: int = 1
    ) -> Dict[str, Any]:
        """
        Get aggregated performance summary for a warehouse.

        Args:
            warehouse_name: Name of warehouse
            hours_back: How many hours of history

        Returns:
            Performance summary dict
        """
        sql = f"""
        SELECT
            COUNT(*) as total_queries,
            COUNT(CASE WHEN EXECUTION_STATUS = 'SUCCESS' THEN 1 END) as successful_queries,
            COUNT(CASE WHEN EXECUTION_STATUS = 'FAIL' THEN 1 END) as failed_queries,
            AVG(TOTAL_ELAPSED_TIME) as avg_elapsed_ms,
            MEDIAN(TOTAL_ELAPSED_TIME) as median_elapsed_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY TOTAL_ELAPSED_TIME) as p95_elapsed_ms,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY TOTAL_ELAPSED_TIME) as p99_elapsed_ms,
            MAX(TOTAL_ELAPSED_TIME) as max_elapsed_ms,
            MIN(TOTAL_ELAPSED_TIME) as min_elapsed_ms,
            SUM(BYTES_SCANNED) as total_bytes_scanned,
            SUM(ROWS_PRODUCED) as total_rows_produced,
            AVG(QUEUED_OVERLOAD_TIME) as avg_queue_time_ms,
            SUM(CREDITS_USED_CLOUD_SERVICES) as total_credits
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
            DATEADD('hours', -{hours_back}, CURRENT_TIMESTAMP()),
            CURRENT_TIMESTAMP(),
            RESULT_LIMIT => 10000
        ))
        WHERE WAREHOUSE_NAME = '{warehouse_name}'
          AND QUERY_TYPE = 'SELECT'
        """

        try:
            results = self.conn.fetch(sql)
            if results:
                return dict(results[0])
            return {}
        except Exception as e:
            logger.error(f"Failed to get performance summary for {warehouse_name}: {e}")
            return {}


def setup_test_warehouses(connection_name: str = "stress_testing") -> Dict[str, bool]:
    """
    Convenience function to set up all test warehouses.

    Args:
        connection_name: Snowflake connection name

    Returns:
        Dict mapping warehouse names to creation status
    """
    with WarehouseManager(connection_name) as manager:
        return manager.create_test_warehouses()


def cleanup_test_warehouses(connection_name: str = "stress_testing") -> int:
    """
    Convenience function to clean up all test warehouses.

    Args:
        connection_name: Snowflake connection name

    Returns:
        Number of warehouses dropped
    """
    with WarehouseManager(connection_name) as manager:
        return manager.drop_all_test_warehouses()


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Parse command line args
    action = sys.argv[1] if len(sys.argv) > 1 else "list"
    connection = sys.argv[2] if len(sys.argv) > 2 else "stress_testing"

    with WarehouseManager(connection) as manager:
        if action == "setup":
            print("Creating test warehouses...")
            results = manager.create_test_warehouses()
            for name, success in results.items():
                status = "✓" if success else "✗"
                print(f"  {status} {name}")

        elif action == "list":
            print("Test warehouses:")
            warehouses = manager.list_test_warehouses()
            if not warehouses:
                print("  No test warehouses found")
            for wh in warehouses:
                print(f"  {wh['name']}: {wh['size']} ({wh['state']})")

        elif action == "suspend":
            count = manager.suspend_all_test_warehouses()
            print(f"Suspended {count} warehouses")

        elif action == "resume":
            count = manager.resume_all_test_warehouses()
            print(f"Resumed {count} warehouses")

        elif action == "cleanup":
            print("Dropping all test warehouses...")
            count = manager.drop_all_test_warehouses()
            print(f"Dropped {count} warehouses")

        elif action == "metrics":
            warehouse_name = sys.argv[3] if len(sys.argv) > 3 else None
            if warehouse_name:
                summary = manager.get_query_performance_summary(warehouse_name)
                print(f"\nPerformance Summary for {warehouse_name}:")
                for key, value in summary.items():
                    print(f"  {key}: {value}")
            else:
                print("Usage: python warehouse_manager.py metrics <connection> <warehouse_name>")

        else:
            print(f"""
Warehouse Manager for Snowflake Stress Testing

Usage: python warehouse_manager.py <action> [connection_name]

Actions:
  setup     Create all test warehouses (XS/S/M x Gen1/Gen2)
  list      List all test warehouses
  suspend   Suspend all test warehouses
  resume    Resume all test warehouses
  cleanup   Drop all test warehouses
  metrics   Get performance metrics for a warehouse

Examples:
  python warehouse_manager.py setup stress_testing
  python warehouse_manager.py list stress_testing
  python warehouse_manager.py metrics stress_testing STRESS_TEST_XSMALL_GEN1
""")
