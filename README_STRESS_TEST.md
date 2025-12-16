# Snowflake Stress Testing Framework

A comprehensive load testing framework for Snowflake warehouses using Locust, with support for Gen1 vs Gen2 comparison, multiple warehouse sizes, and TPC-H benchmark queries.

## Features

- **TPC-H Benchmark Queries**: Simple, medium, and complex query tiers with configurable weighting
- **Parallel Warehouse Testing**: Test multiple warehouse configurations simultaneously
- **Gen1 vs Gen2 Comparison**: Direct performance comparison between warehouse generations
- **Size Scaling Analysis**: X-Small, Small, and Medium warehouse comparison
- **Connection Pooling**: Efficient connection management for high throughput
- **Results Aggregation**: Automated metrics collection from QUERY_HISTORY
- **Snowflake CLI Integration**: Uses `~/.snowflake/config.toml` for credentials

## Quick Start

### 1. Setup Snowflake Connection

Create a `stress_testing` connection in `~/.snowflake/config.toml`:

```toml
[connections.stress_testing]
account = "your_account"
user = "your_user"
password = "your_password"
role = "ACCOUNTADMIN"
warehouse = "COMPUTE_WH"
database = "SNOWFLAKE_SAMPLE_DATA"
schema = "TPCH_SF1"
```

### 2. Create Test Warehouses

Option A - Using Python:
```bash
python warehouse_manager.py setup stress_testing
```

Option B - Using SQL:
```bash
snow sql -f sql/setup_warehouses.sql -c stress_testing
```

### 3. Run a Basic Test

```bash
cd tests
locust -f locustfile-snowflake.py \
    --connection stress_testing \
    --warehouse STRESS_TEST_XSMALL_GEN1 \
    --users 50 \
    --spawn-rate 2 \
    --run-time 10m \
    --html report.html
```

### 4. Run Parallel Tests (Gen1 vs Gen2)

```bash
python parallel_test_runner.py \
    --connection stress_testing \
    --users 50 \
    --duration 10m
```

### 5. Analyze Results

```bash
python results_aggregator.py --connection stress_testing --hours 2
```

## Project Structure

```
simple_stress_test/
├── warehouse_manager.py      # Warehouse lifecycle management (Snowpark)
├── parallel_test_runner.py   # Orchestrates parallel warehouse tests
├── results_aggregator.py     # Collects and compares test results
├── snowflake_connection.py   # Snowpark connection utilities
├── requirements.txt          # Python dependencies
│
├── tests/
│   ├── locustfile-snowflake.py   # Main Locust test file
│   ├── locust_config.yaml        # Query and test configuration
│   └── locust.conf               # Locust default settings
│
├── sql/
│   ├── setup_warehouses.sql      # Create test warehouses
│   ├── cleanup_warehouses.sql    # Drop test warehouses
│   └── suspend_warehouses.sql    # Suspend all warehouses
│
└── results/                  # Test results and reports
```

## Configuration

### locust_config.yaml

```yaml
locust:
  connection_name: stress_testing

  load_configuration:
    min_users: 10
    max_users: 50

  user_behavior:
    min_wait: 100   # ms between queries
    max_wait: 2000

  query_weights:
    simple: 5    # Point lookups (most frequent)
    medium: 3    # Joins, aggregations
    complex: 1   # Multi-join CTEs (least frequent)

  queries:
    customer_lookup:
      complexity: simple
      sql: SELECT * FROM CUSTOMER WHERE C_CUSTKEY = ?
      params: [random_customer_key]
    # ... more queries
```

### Command Line Options

```bash
locust -f locustfile-snowflake.py [OPTIONS]

Options:
  --connection NAME       Snowflake connection name (default: stress_testing)
  --warehouse NAME        Target warehouse to test
  --users N              Number of concurrent users
  --spawn-rate N         Users spawned per second
  --run-time DURATION    Test duration (e.g., "10m", "1h")
  --min-wait MS          Min wait between queries
  --max-wait MS          Max wait between queries
  --query-complexity     Filter: simple, medium, complex, or mixed
  --force-id-reload      Reload IDs from Snowflake (ignore cache)
  --print-ids            Print sample IDs before starting
  --html FILE            Generate HTML report
```

## Warehouse Configurations

The framework creates 6 test warehouses:

| Warehouse Name | Size | Generation | Credits/Hour |
|----------------|------|------------|--------------|
| STRESS_TEST_XSMALL_GEN1 | X-Small | Gen1 | 1 |
| STRESS_TEST_XSMALL_GEN2 | X-Small | Gen2 | 1 |
| STRESS_TEST_SMALL_GEN1 | Small | Gen1 | 2 |
| STRESS_TEST_SMALL_GEN2 | Small | Gen2 | 2 |
| STRESS_TEST_MEDIUM_GEN1 | Medium | Gen1 | 4 |
| STRESS_TEST_MEDIUM_GEN2 | Medium | Gen2 | 4 |

## TPC-H Query Types

### Simple Queries (Weight: 5)
- Point lookups by primary key
- Single table scans with filters
- Example: `customer_lookup`, `order_lookup`

### Medium Queries (Weight: 3)
- 2-3 table joins
- Aggregations with GROUP BY
- Window functions
- Example: `pricing_summary`, `customer_order_summary`

### Complex Queries (Weight: 1)
- 4+ table joins
- CTEs with analytical functions
- Nested subqueries
- Example: `market_share`, `customer_lifetime_value`

## Usage Examples

### Test Single Warehouse
```bash
locust -f tests/locustfile-snowflake.py \
    --connection stress_testing \
    --warehouse STRESS_TEST_SMALL_GEN2 \
    --users 100 \
    --run-time 5m
```

### Compare Gen1 vs Gen2
```bash
# Run parallel tests on all warehouses
python parallel_test_runner.py \
    --connection stress_testing \
    --users 50 \
    --duration 10m

# View comparison
python results_aggregator.py --connection stress_testing
```

### Test Only Simple Queries
```bash
locust -f tests/locustfile-snowflake.py \
    --connection stress_testing \
    --warehouse STRESS_TEST_MEDIUM_GEN1 \
    --query-complexity simple \
    --users 200 \
    --run-time 5m
```

### Warehouse Management
```bash
# List test warehouses
python warehouse_manager.py list stress_testing

# Suspend all (stop credit usage)
python warehouse_manager.py suspend stress_testing

# Resume all
python warehouse_manager.py resume stress_testing

# Get metrics
python warehouse_manager.py metrics stress_testing STRESS_TEST_XSMALL_GEN1

# Cleanup (drop all)
python warehouse_manager.py cleanup stress_testing
```

## Results Analysis

After running tests, analyze results:

```bash
python results_aggregator.py --connection stress_testing --hours 2
```

Output:
```
======================================================================
SNOWFLAKE STRESS TEST RESULTS SUMMARY
======================================================================

--- Gen1 vs Gen2 Comparison ---
Metric                         Gen1            Gen2      Improvement
---------------------------------------------------------------------------
Avg Latency (ms)             245.32          198.45          19.1%
P95 Latency (ms)             523.18          412.67          21.1%
Total Queries                 12453           12891
Error Rate                   0.0012          0.0008

--- Size Comparison ---
Size           Queries   Avg Latency   P95 Latency      Q/Credit
----------------------------------------------------------------------
XSMALL            4521        312.45        687.23          4521
SMALL             8234        234.67        498.34          4117
MEDIUM           12589        178.23        356.78          3147

--- Recommendations ---
1. Gen2 warehouses show 19.1% latency improvement. Consider migrating...
2. XSMALL warehouses show best cost efficiency at 4521 queries per credit.
```

## Tips

1. **Start Small**: Begin with X-Small warehouses and 10-20 users
2. **Monitor Credits**: Use `suspend_warehouses.sql` when not testing
3. **Warmup**: Warehouses need 30-60 seconds to warm up after resume
4. **Cache IDs**: First run caches IDs to `snowflake_id_cache.pickle`
5. **Check Errors**: High error rates may indicate warehouse saturation

## Troubleshooting

### "No connection found"
Verify your `~/.snowflake/config.toml` has a `stress_testing` connection.

### "Permission denied on warehouse"
Ensure your role has USAGE grant on the test warehouses.

### "Query timeout"
Increase the timeout in queries or reduce concurrent users.

### "Connection pool exhausted"
Reduce users or increase `MAX_POOL_SIZE` in locustfile.
