# Snowflake Stress Testing Framework

A comprehensive framework for stress testing Snowflake warehouses with Gen1 vs Gen2 comparison capabilities.

## Features

- **TPC-H Benchmark Queries** - Industry-standard queries for database performance testing
- **Gen1 vs Gen2 Comparison** - Compare standard vs next-gen warehouse performance
- **Query Tagging** - All test queries are tagged for easy tracking and analysis
- **Parallel Test Runner** - Run tests on multiple warehouses simultaneously
- **Interactive Dashboard** - Streamlit app for visualizing and comparing results
- **Warehouse Management** - Scripts to setup, suspend, and cleanup test warehouses

## Quick Start

### 1. Setup Warehouses

```bash
# Create 6 test warehouses (XS/S/M × Gen1/Gen2)
python warehouse_manager.py setup stress_test
```

### 2. Run Parallel Comparison Test

```bash
# Quick test (30 seconds)
python run_comparison.py --size XSMALL --users 10 --duration 30s

# Standard test (2 minutes)
python run_comparison.py --size XSMALL --users 20 --duration 2m

# Heavy load test (5 minutes, larger warehouse)
python run_comparison.py --size SMALL --users 50 --duration 5m
```

### 3. View Results in Dashboard

```bash
streamlit run streamlit_app.py
```

Open http://localhost:8501 in your browser.

## Dashboard Pages

| Page | Description |
|------|-------------|
| **Overview** | Shows test warehouses and recent test runs |
| **Gen1 vs Gen2** | Compare performance between warehouse generations |
| **Warehouse Details** | Deep dive into specific warehouse metrics |
| **Run Test** | Commands to run tests and manage warehouses |
| **Debug** | Test queries and diagnose connection issues |

## Warehouse Types

This framework tests **Gen2 Standard** warehouses vs **Gen1 Standard** warehouses:

| Type | RESOURCE_CONSTRAINT | Description |
|------|---------------------|-------------|
| Gen1 | `STANDARD_GEN_1` | Original Snowflake warehouses |
| Gen2 | `STANDARD_GEN_2` | Next-generation with improved compute |

**Note:** These are different from Snowpark-optimized warehouses (`MEMORY_*X`), which are designed for ML/UDF workloads.

## Query Tagging

All stress test queries are automatically tagged with:

```
STRESS_TEST|<warehouse>|<run_id>
```

Example: `STRESS_TEST|STRESS_TEST_XSMALL_GEN1|20251216_140234`

This allows filtering query history in:
- Snowflake's QUERY_HISTORY
- The Streamlit dashboard
- Custom analysis queries

## Project Structure

```
simple_stress_test/
├── streamlit_app.py       # Dashboard application
├── run_comparison.py      # Parallel test runner
├── warehouse_manager.py   # Warehouse lifecycle management
├── snowflake_connection.py # Snowflake connection utilities
├── tests/
│   ├── locustfile-snowflake.py  # Locust test definitions
│   └── locust_config.yaml       # Query configurations
├── sql/
│   ├── setup_warehouses.sql     # Warehouse creation SQL
│   ├── cleanup_warehouses.sql   # Warehouse deletion SQL
│   └── suspend_warehouses.sql   # Warehouse suspension SQL
└── results/               # Test reports (HTML)
```

## Running Individual Tests

### Single Warehouse Test

```bash
cd tests && locust -f locustfile-snowflake.py \
    --connection stress_test \
    --warehouse STRESS_TEST_XSMALL_GEN1 \
    --users 20 \
    --spawn-rate 2 \
    --run-time 2m \
    --headless \
    --html ../results/test_report.html
```

### With Web UI

```bash
cd tests && locust -f locustfile-snowflake.py \
    --connection stress_test \
    --warehouse STRESS_TEST_XSMALL_GEN1
```

Open http://localhost:8089 to control the test.

## Warehouse Management

```bash
# Setup (create warehouses)
python warehouse_manager.py setup stress_test

# List warehouses
python warehouse_manager.py list stress_test

# Suspend all (stop credit usage)
python warehouse_manager.py suspend stress_test

# Resume all
python warehouse_manager.py resume stress_test

# Cleanup (drop all test warehouses)
python warehouse_manager.py cleanup stress_test
```

## Configuration

### Snow CLI Connection

Add to `~/.snowflake/config.toml`:

```toml
[connections.stress_test]
account = "your_account"
user = "your_user"
password = "your_password"
warehouse = "COMPUTE_WH"
database = "your_database"
schema = "your_schema"
role = "ACCOUNTADMIN"
```

### Test Configuration

Edit `tests/locust_config.yaml` to customize:
- Query weights (simple/medium/complex)
- User behavior (wait times)
- ID list queries

## Requirements

```bash
pip install locust snowflake-connector-python snowflake-snowpark-python streamlit plotly pyyaml tomlkit
```

## Sample Results

After running a comparison test:

```
============================================================
🔬 PARALLEL STRESS TEST COMPARISON
============================================================
Size:       XSMALL
Users:      20
Duration:   2m

✅ Gen1 completed in 125.3s - 523 queries
✅ Gen2 completed in 124.8s - 548 queries
============================================================

Gen1: Avg=0.21s, Median=0.14s, P95=0.49s
Gen2: Avg=0.18s, Median=0.13s, P95=0.47s

✅ Gen2 is 17.3% FASTER on average
```

## License

MIT
