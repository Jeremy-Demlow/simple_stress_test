"""
Snowflake Stress Testing with Locust - TPC-H Workload

This Locust test file executes TPC-H benchmark queries against Snowflake
warehouses with configurable complexity weighting and parallel warehouse support.

Features:
- Connection pooling for high throughput
- TPC-H query mix (simple, medium, complex)
- Multi-warehouse support for Gen1 vs Gen2 comparison
- ID caching for parameterized queries
- Configurable via YAML and command-line arguments

Usage:
    locust -f locustfile-snowflake.py --connection stress_testing --warehouse STRESS_TEST_XSMALL_GEN1
"""

import os
import yaml
import time
import random
import logging
import pickle
from pathlib import Path
from queue import Queue, Empty
from typing import Dict, List, Optional

from locust import User, events, task, between

import snowflake.connector
from snowflake.connector.converter_null import SnowflakeNoConverterToPython
from tomlkit import parse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Get the directory of the current file
CURRENT_DIR = Path(__file__).parent.resolve()

# Define default paths
DEFAULT_CONFIG_PATH = CURRENT_DIR / 'locust_config.yaml'
ID_CACHE_PATH = CURRENT_DIR / 'snowflake_id_cache.pickle'

# Global state
ID_LISTS: Dict[str, List[str]] = {}
IDS_LOADED = False
CONFIG: Optional[Dict] = None
CURRENT_WAREHOUSE: Optional[str] = None

# Connection pool per warehouse
CONNECTION_POOLS: Dict[str, Queue] = {}
MAX_POOL_SIZE = 100
POOL_INITIALIZED: Dict[str, bool] = {}


def load_config(file_path=DEFAULT_CONFIG_PATH) -> Optional[Dict]:
    """Load the YAML configuration file."""
    global CONFIG
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
            CONFIG = config
            return config
    except Exception as e:
        logger.error(f"Failed to load config: {str(e)}")
        return None


def get_snowflake_credentials(connection_name: str) -> Dict[str, str]:
    """Get Snowflake credentials from Snow CLI config.toml."""
    config_paths = [
        Path.home() / ".snowflake" / "config.toml",
        Path.home() / "Library/Application Support/snowflake/config.toml"
    ]

    for config_path in config_paths:
        if config_path.exists():
            with open(config_path) as f:
                config = parse(f.read())
            return config["connections"][connection_name]

    raise FileNotFoundError("Could not find Snow CLI config.toml")


def connect_to_snowflake(
    connection_name: str,
    warehouse_override: Optional[str] = None
) -> Optional[snowflake.connector.SnowflakeConnection]:
    """
    Connect to Snowflake using Snow CLI config.

    Args:
        connection_name: Name of connection in config.toml
        warehouse_override: Override warehouse from config

    Returns:
        Snowflake connection or None
    """
    try:
        creds = get_snowflake_credentials(connection_name)

        # Set paramstyle globally
        snowflake.connector.paramstyle = 'qmark'

        warehouse = warehouse_override or creds.get("warehouse")

        conn = snowflake.connector.connect(
            account=creds["account"],
            user=creds["user"],
            password=creds["password"],
            database=creds.get("database"),
            schema=creds.get("schema"),
            role=creds.get("role"),
            warehouse=warehouse,
            converter_class=SnowflakeNoConverterToPython,
        )

        # If warehouse override, make sure we're using it
        if warehouse_override:
            cursor = conn.cursor()
            cursor.execute(f"USE WAREHOUSE {warehouse_override}")

        return conn

    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        return None


def get_connection_pool(warehouse_name: str) -> Queue:
    """Get or create a connection pool for a warehouse."""
    if warehouse_name not in CONNECTION_POOLS:
        CONNECTION_POOLS[warehouse_name] = Queue()
        POOL_INITIALIZED[warehouse_name] = False
    return CONNECTION_POOLS[warehouse_name]


def initialize_connection_pool(
    connection_name: str,
    warehouse_name: str,
    size: int = MAX_POOL_SIZE
) -> bool:
    """
    Initialize a connection pool for a specific warehouse.

    Args:
        connection_name: Snow CLI connection name
        warehouse_name: Target warehouse name
        size: Maximum pool size

    Returns:
        True if successful
    """
    pool = get_connection_pool(warehouse_name)

    if POOL_INITIALIZED.get(warehouse_name, False):
        logger.info(f"Connection pool for {warehouse_name} already initialized")
        return True

    logger.info(f"Initializing connection pool for {warehouse_name} (size={size})")
    connections_created = 0

    for _ in range(size):
        try:
            conn = connect_to_snowflake(connection_name, warehouse_name)
            if conn:
                pool.put(conn)
                connections_created += 1
                if connections_created % 10 == 0:
                    logger.info(f"Created {connections_created} connections for {warehouse_name}")
        except Exception as e:
            logger.error(f"Failed to create connection: {str(e)}")
            break

    logger.info(f"Pool for {warehouse_name} initialized with {connections_created} connections")
    POOL_INITIALIZED[warehouse_name] = True
    return connections_created > 0


def preload_ids(connection_name: str, config: Dict) -> bool:
    """
    Preload ID lists for parameterized queries.

    Args:
        connection_name: Snow CLI connection name
        config: Loaded YAML config

    Returns:
        True if successful
    """
    global ID_LISTS, IDS_LOADED

    logger.info("==== PRELOADING IDs FOR TPC-H QUERIES ====")

    # Check cache first
    if ID_CACHE_PATH.exists():
        try:
            logger.info(f"Loading IDs from cache: {ID_CACHE_PATH}")
            with open(ID_CACHE_PATH, 'rb') as f:
                cached_ids = pickle.load(f)

            # Verify cache has data
            if all(len(ids) >= 5 for ids in cached_ids.values()):
                ID_LISTS = cached_ids
                logger.info(f"Loaded {len(ID_LISTS)} ID lists from cache")
                for key, ids in ID_LISTS.items():
                    logger.info(f"  {key}: {len(ids)} IDs")
                IDS_LOADED = True
                return True
            else:
                logger.warning("Cache incomplete, reloading from Snowflake")
        except Exception as e:
            logger.warning(f"Failed to load cache: {str(e)}")

    # Load from Snowflake
    ID_LISTS = {}
    conn = None

    try:
        conn = connect_to_snowflake(connection_name)
        if not conn:
            logger.error("Failed to connect for ID loading")
            return False

        id_list_config = config.get('locust', {}).get('id_lists', {})
        success_count = 0

        for id_type, query in id_list_config.items():
            list_name = id_type.replace('_query', '')
            logger.info(f"Loading {list_name}...")

            try:
                cursor = conn.cursor()
                cursor.execute(f"/*+ QUERY_TIMEOUT=60 */ {query}")
                result = cursor.fetchall()

                if result:
                    ids = [str(row[0]) for row in result if row[0] is not None]
                    if ids:
                        ID_LISTS[list_name] = ids
                        logger.info(f"  ✓ {list_name}: {len(ids)} IDs")
                        success_count += 1
                    else:
                        logger.warning(f"  ✗ {list_name}: No valid IDs")
                else:
                    logger.warning(f"  ✗ {list_name}: Empty result")

            except Exception as e:
                logger.error(f"  ✗ {list_name}: {str(e)}")

        # Cache results
        if success_count > 0:
            try:
                with open(ID_CACHE_PATH, 'wb') as f:
                    pickle.dump(ID_LISTS, f)
                logger.info(f"Cached {success_count} ID lists")
            except Exception as e:
                logger.warning(f"Failed to cache IDs: {str(e)}")

        IDS_LOADED = success_count == len(id_list_config)

        if not IDS_LOADED:
            logger.error("Not all ID lists loaded - test may fail")

        return IDS_LOADED

    except Exception as e:
        logger.error(f"Error during ID preloading: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()


def get_queries_by_complexity(config: Dict) -> Dict[str, List[str]]:
    """
    Group queries by complexity level.

    Args:
        config: Loaded YAML config

    Returns:
        Dict mapping complexity to list of query names
    """
    queries = config.get('locust', {}).get('queries', {})
    by_complexity = {'simple': [], 'medium': [], 'complex': []}

    for name, query_config in queries.items():
        complexity = query_config.get('complexity', 'medium')
        if complexity in by_complexity:
            by_complexity[complexity].append(name)

    return by_complexity


def get_weighted_query_name(config: Dict) -> str:
    """
    Select a query based on complexity weights.

    Args:
        config: Loaded YAML config

    Returns:
        Selected query name
    """
    weights = config.get('locust', {}).get('query_weights', {
        'simple': 5,
        'medium': 3,
        'complex': 1
    })

    by_complexity = get_queries_by_complexity(config)

    # Build weighted list
    weighted_pool = []
    for complexity, query_names in by_complexity.items():
        weight = weights.get(complexity, 1)
        for name in query_names:
            weighted_pool.extend([name] * weight)

    if not weighted_pool:
        # Fallback to any available query
        queries = config.get('locust', {}).get('queries', {})
        return random.choice(list(queries.keys())) if queries else None

    return random.choice(weighted_pool)


# ==============================================================================
# Locust Event Handlers
# ==============================================================================

@events.init_command_line_parser.add_listener
def on_locust_init_parser(parser):
    """Add custom command-line arguments."""
    parser.add_argument(
        "--connection",
        type=str,
        default="stress_testing",
        help="Snowflake connection name from config.toml"
    )
    parser.add_argument(
        "--warehouse",
        type=str,
        help="Target warehouse name (overrides config)"
    )
    parser.add_argument(
        "--force-id-reload",
        action="store_true",
        help="Force reload IDs from Snowflake (ignore cache)"
    )
    parser.add_argument(
        "--print-ids",
        action="store_true",
        help="Print sample IDs before starting"
    )
    parser.add_argument(
        "--min-wait",
        type=int,
        help="Minimum wait time between tasks (ms)"
    )
    parser.add_argument(
        "--max-wait",
        type=int,
        help="Maximum wait time between tasks (ms)"
    )
    parser.add_argument(
        "--query-complexity",
        type=str,
        choices=['simple', 'medium', 'complex', 'mixed'],
        default='mixed',
        help="Query complexity filter"
    )


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    """Initialize the test environment."""
    global CURRENT_WAREHOUSE, CONFIG

    # Set paramstyle globally
    snowflake.connector.paramstyle = 'qmark'

    # Load configuration
    environment.config = load_config()
    if not environment.config:
        logger.error("Failed to load configuration")
        environment.runner.quit()
        return

    CONFIG = environment.config

    # Get connection settings
    opts = environment.parsed_options
    connection_name = opts.connection or "stress_testing"
    environment.snowflake_connection = connection_name

    # Get warehouse (CLI > env var > config)
    warehouse = (
        opts.warehouse or
        os.environ.get('LOCUST_WAREHOUSE') or
        environment.config.get('locust', {}).get('warehouses', [{}])[0].get('name')
    )

    if warehouse:
        CURRENT_WAREHOUSE = warehouse
        environment.snowflake_warehouse = warehouse
        logger.info(f"Using warehouse: {warehouse}")
    else:
        logger.warning("No warehouse specified - using connection default")

    # Store wait settings
    locust_config = environment.config.get('locust', {})
    user_behavior = locust_config.get('user_behavior', {})

    environment.min_wait = opts.min_wait or user_behavior.get('min_wait', 100)
    environment.max_wait = opts.max_wait or user_behavior.get('max_wait', 2000)

    logger.info(f"Wait time: {environment.min_wait}-{environment.max_wait}ms")

    # Store query complexity filter
    environment.query_complexity = opts.query_complexity or 'mixed'
    logger.info(f"Query complexity: {environment.query_complexity}")

    # Delete cache if force reload
    if opts.force_id_reload and ID_CACHE_PATH.exists():
        ID_CACHE_PATH.unlink()
        logger.info("ID cache deleted for forced reload")

    # Initialize connection pool
    pool_size = min(MAX_POOL_SIZE, opts.num_users or 50)
    initialize_connection_pool(connection_name, warehouse or "default", pool_size)

    # Preload IDs
    if not preload_ids(connection_name, environment.config):
        logger.error("ID preloading failed")
        environment.runner.quit()
        return

    # Print IDs if requested
    if opts.print_ids:
        logger.info("==== SAMPLE IDs ====")
        for key, ids in ID_LISTS.items():
            sample = ids[:5] + (["..."] if len(ids) > 5 else [])
            logger.info(f"  {key}: {sample}")

    environment.id_lists = ID_LISTS


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when the test starts."""
    global IDS_LOADED, ID_LISTS

    # Emergency cache load for workers
    if not IDS_LOADED:
        try:
            if ID_CACHE_PATH.exists():
                with open(ID_CACHE_PATH, 'rb') as f:
                    ID_LISTS = pickle.load(f)
                IDS_LOADED = True
                logger.info(f"Worker loaded {len(ID_LISTS)} ID lists from cache")
        except Exception as e:
            logger.error(f"Emergency cache load failed: {e}")

    if not IDS_LOADED or not ID_LISTS:
        logger.error("ID lists not available - aborting")
        environment.runner.quit()
        return

    logger.info("==== TEST STARTED ====")
    logger.info(f"Warehouse: {CURRENT_WAREHOUSE}")
    logger.info(f"ID lists: {list(ID_LISTS.keys())}")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Clean up when test stops."""
    logger.info("Test stopping, cleaning up connections...")

    connections_closed = 0
    for warehouse_name, pool in CONNECTION_POOLS.items():
        while not pool.empty():
            try:
                conn = pool.get(block=False)
                conn.close()
                connections_closed += 1
            except Empty:
                break
            except Exception:
                pass

    logger.info(f"Closed {connections_closed} connections")


# ==============================================================================
# User Classes
# ==============================================================================

class SnowflakeUser(User):
    """
    Base Snowflake user class with connection pooling.

    Executes TPC-H queries against the configured warehouse
    with weighted complexity selection.
    """

    # Default wait time (overridden in __init__)
    wait_time = between(0.1, 2.0)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = None
        self.warehouse = CURRENT_WAREHOUSE

        # Set wait time from environment
        if hasattr(self.environment, 'min_wait') and hasattr(self.environment, 'max_wait'):
            min_wait = self.environment.min_wait / 1000.0
            max_wait = self.environment.max_wait / 1000.0

            def wait_time_func(self):
                return random.uniform(min_wait, max_wait)

            self.wait_time = wait_time_func.__get__(self)

        self.setup_connection()

    def setup_connection(self):
        """Get a connection from the pool."""
        pool = get_connection_pool(self.warehouse or "default")

        try:
            # Try to get from pool
            self.conn = pool.get(block=True, timeout=5)
            logger.debug(f"Got connection from pool for {self.warehouse}")
        except Empty:
            # Pool exhausted, create new connection
            self.conn = connect_to_snowflake(
                self.environment.snowflake_connection,
                self.warehouse
            )
            if self.conn:
                logger.info(f"Created new connection for {self.warehouse}")
            else:
                logger.error("Failed to get connection")

    def get_random_id(self, list_name: str) -> str:
        """Get a random ID from a preloaded list."""
        # Map param names to ID list names
        id_mapping = {
            # Tasty Bytes mappings
            'random_customer_id': 'customer_id',
            'random_order_id': 'order_id',
            'random_menu_item_id': 'menu_item_id',
            'random_truck_brand': 'truck_brand',
            'random_city': 'city',
            # TPC-H mappings (for compatibility)
            'random_customer_key': 'customer_key',
            'random_order_key': 'order_key',
            'random_part_key': 'part_key',
            'random_supplier_key': 'supplier_key',
            'random_market_segment': 'market_segment',
            'random_region': 'region',
            'random_nation': 'nation',
            'random_part_type': 'part_type',
        }

        mapped_name = id_mapping.get(list_name, list_name)

        if mapped_name in ID_LISTS and ID_LISTS[mapped_name]:
            return random.choice(ID_LISTS[mapped_name])

        logger.warning(f"No IDs for {list_name} (mapped: {mapped_name})")
        return "1"  # Fallback

    def execute_query(self, query_name: str) -> bool:
        """
        Execute a query and record metrics.

        Args:
            query_name: Name of query from config

        Returns:
            True if successful
        """
        if not self.conn:
            self.setup_connection()
            if not self.conn:
                return False

        start_time = time.time()

        try:
            config = self.environment.config
            queries = config.get('locust', {}).get('queries', {})

            if query_name not in queries:
                logger.warning(f"Query {query_name} not found")
                return False

            query_config = queries[query_name]
            sql = query_config['sql']

            # Build parameters
            params = []
            for param in query_config.get('params', []):
                params.append(self.get_random_id(param))

            # Ensure param count matches placeholders
            placeholder_count = sql.count('?')
            while len(params) < placeholder_count:
                params.append("1")
            params = params[:placeholder_count]

            # Execute query
            cursor = self.conn.cursor()
            cursor.execute(f"/*+ QUERY_TIMEOUT=120 */ {sql}", tuple(params))

            # Fetch results (at least first row)
            try:
                cursor.fetchone()
            except Exception:
                pass

            # Record success
            response_time = int((time.time() - start_time) * 1000)
            complexity = query_config.get('complexity', 'medium')

            self.environment.events.request.fire(
                request_type="snowflake",
                name=f"{complexity}:{query_name}",
                response_time=response_time,
                response_length=0,
                exception=None,
            )

            return True

        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)

            logger.error(f"Query {query_name} failed: {str(e)}")

            self.environment.events.request.fire(
                request_type="snowflake",
                name=f"error:{query_name}",
                response_time=response_time,
                response_length=0,
                exception=e,
            )

            # Handle connection errors
            if isinstance(e, snowflake.connector.errors.OperationalError):
                self.return_or_close_connection(bad=True)
                self.conn = None
                self.setup_connection()

            return False

    def return_or_close_connection(self, bad: bool = False):
        """Return connection to pool or close if bad."""
        if not self.conn:
            return

        pool = get_connection_pool(self.warehouse or "default")

        if bad:
            try:
                self.conn.close()
            except Exception:
                pass
            logger.debug("Closed bad connection")
        else:
            try:
                # Test connection before returning
                cursor = self.conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                pool.put(self.conn)
                logger.debug("Returned connection to pool")
            except Exception:
                try:
                    self.conn.close()
                except Exception:
                    pass

    def on_stop(self):
        """Return connection to pool when stopping."""
        self.return_or_close_connection(bad=False)
        self.conn = None

    @task(1)
    def execute_weighted_query(self):
        """Execute a query selected by complexity weight."""
        if not hasattr(self.environment, 'config'):
            return

        config = self.environment.config
        complexity_filter = getattr(self.environment, 'query_complexity', 'mixed')

        if complexity_filter == 'mixed':
            query_name = get_weighted_query_name(config)
        else:
            # Filter by specific complexity
            by_complexity = get_queries_by_complexity(config)
            queries = by_complexity.get(complexity_filter, [])
            if queries:
                query_name = random.choice(queries)
            else:
                query_name = get_weighted_query_name(config)

        if query_name:
            self.execute_query(query_name)


class SimpleQueryUser(SnowflakeUser):
    """User that only runs simple queries (point lookups)."""

    weight = 5  # Higher weight = more instances

    @task(1)
    def run_simple_query(self):
        """Execute a random simple query."""
        config = self.environment.config
        by_complexity = get_queries_by_complexity(config)
        simple_queries = by_complexity.get('simple', [])

        if simple_queries:
            self.execute_query(random.choice(simple_queries))


class AnalyticalUser(SnowflakeUser):
    """User that runs medium complexity analytical queries."""

    weight = 3

    @task(1)
    def run_analytical_query(self):
        """Execute a random medium complexity query."""
        config = self.environment.config
        by_complexity = get_queries_by_complexity(config)
        medium_queries = by_complexity.get('medium', [])

        if medium_queries:
            self.execute_query(random.choice(medium_queries))


class ComplexQueryUser(SnowflakeUser):
    """User that runs complex multi-join queries."""

    weight = 1  # Lower weight = fewer instances

    @task(1)
    def run_complex_query(self):
        """Execute a random complex query."""
        config = self.environment.config
        by_complexity = get_queries_by_complexity(config)
        complex_queries = by_complexity.get('complex', [])

        if complex_queries:
            self.execute_query(random.choice(complex_queries))


class MixedWorkloadUser(SnowflakeUser):
    """
    User that executes a mix of query complexities
    based on configured weights.
    """

    weight = 10  # This is the primary user class

    @task(5)
    def run_simple(self):
        """Run simple queries more frequently."""
        config = self.environment.config
        by_complexity = get_queries_by_complexity(config)
        if by_complexity.get('simple'):
            self.execute_query(random.choice(by_complexity['simple']))

    @task(3)
    def run_medium(self):
        """Run medium queries moderately."""
        config = self.environment.config
        by_complexity = get_queries_by_complexity(config)
        if by_complexity.get('medium'):
            self.execute_query(random.choice(by_complexity['medium']))

    @task(1)
    def run_complex(self):
        """Run complex queries less frequently."""
        config = self.environment.config
        by_complexity = get_queries_by_complexity(config)
        if by_complexity.get('complex'):
            self.execute_query(random.choice(by_complexity['complex']))


# ==============================================================================
# Main Entry Point
# ==============================================================================

if __name__ == "__main__":
    config = load_config()
    if config:
        print("\n=== Snowflake Stress Test Configuration ===\n")

        # Load configuration
        locust_config = config.get('locust', {})
        load_config_section = locust_config.get('load_configuration', {})
        user_behavior = locust_config.get('user_behavior', {})
        warehouses = locust_config.get('warehouses', [])
        queries = locust_config.get('queries', {})

        print("Load Configuration:")
        print(f"  Min Users: {load_config_section.get('min_users', 'N/A')}")
        print(f"  Max Users: {load_config_section.get('max_users', 'N/A')}")

        print("\nUser Behavior:")
        print(f"  Min Wait: {user_behavior.get('min_wait', 'N/A')}ms")
        print(f"  Max Wait: {user_behavior.get('max_wait', 'N/A')}ms")

        print("\nConfigured Warehouses:")
        for wh in warehouses:
            print(f"  - {wh['name']} ({wh['size']}, {wh['generation']})")

        print("\nQuery Complexity Distribution:")
        by_complexity = get_queries_by_complexity(config)
        for complexity, query_list in by_complexity.items():
            print(f"  {complexity.capitalize()}: {len(query_list)} queries")
            for q in query_list[:3]:
                print(f"    - {q}")
            if len(query_list) > 3:
                print(f"    ... and {len(query_list) - 3} more")

        print("""
=== Usage ===

Basic test:
  locust -f locustfile-snowflake.py --connection stress_testing --warehouse STRESS_TEST_XSMALL_GEN1

With custom parameters:
  locust -f locustfile-snowflake.py \\
    --connection stress_testing \\
    --warehouse STRESS_TEST_MEDIUM_GEN2 \\
    --users 50 \\
    --spawn-rate 2 \\
    --run-time 10m \\
    --min-wait 100 \\
    --max-wait 2000 \\
    --query-complexity mixed \\
    --html report.html

Headless mode:
  locust -f locustfile-snowflake.py \\
    --connection stress_testing \\
    --warehouse STRESS_TEST_SMALL_GEN1 \\
    --headless \\
    --users 100 \\
    --spawn-rate 5 \\
    --run-time 5m

Additional options:
  --force-id-reload    Force reload IDs from Snowflake
  --print-ids          Print sample IDs before starting
  --query-complexity   Filter: simple, medium, complex, or mixed
""")
