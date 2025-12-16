-- ============================================================
-- Snowflake Stress Test Warehouse Setup
-- ============================================================
-- This script creates test warehouses for Gen1 vs Gen2 comparison
-- Run with: snow sql -f sql/setup_warehouses.sql -c stress_testing
-- ============================================================

-- Set role (adjust if needed)
USE ROLE ACCOUNTADMIN;

-- ============================================================
-- X-Small Warehouses
-- ============================================================

-- Gen1 X-Small
CREATE WAREHOUSE IF NOT EXISTS STRESS_TEST_XSMALL_GEN1
    WAREHOUSE_SIZE = 'XSMALL'
    RESOURCE_CONSTRAINT = STANDARD_GEN_1
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Stress test warehouse: X-Small Gen1';

-- Gen2 X-Small
CREATE WAREHOUSE IF NOT EXISTS STRESS_TEST_XSMALL_GEN2
    WAREHOUSE_SIZE = 'XSMALL'
    RESOURCE_CONSTRAINT = STANDARD_GEN_2
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Stress test warehouse: X-Small Gen2';

-- ============================================================
-- Small Warehouses
-- ============================================================

-- Gen1 Small
CREATE WAREHOUSE IF NOT EXISTS STRESS_TEST_SMALL_GEN1
    WAREHOUSE_SIZE = 'SMALL'
    RESOURCE_CONSTRAINT = STANDARD_GEN_1
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Stress test warehouse: Small Gen1';

-- Gen2 Small
CREATE WAREHOUSE IF NOT EXISTS STRESS_TEST_SMALL_GEN2
    WAREHOUSE_SIZE = 'SMALL'
    RESOURCE_CONSTRAINT = STANDARD_GEN_2
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Stress test warehouse: Small Gen2';

-- ============================================================
-- Medium Warehouses
-- ============================================================

-- Gen1 Medium
CREATE WAREHOUSE IF NOT EXISTS STRESS_TEST_MEDIUM_GEN1
    WAREHOUSE_SIZE = 'MEDIUM'
    RESOURCE_CONSTRAINT = STANDARD_GEN_1
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Stress test warehouse: Medium Gen1';

-- Gen2 Medium
CREATE WAREHOUSE IF NOT EXISTS STRESS_TEST_MEDIUM_GEN2
    WAREHOUSE_SIZE = 'MEDIUM'
    RESOURCE_CONSTRAINT = STANDARD_GEN_2
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Stress test warehouse: Medium Gen2';

-- ============================================================
-- Verify Setup
-- ============================================================

SHOW WAREHOUSES LIKE 'STRESS_TEST%';

-- ============================================================
-- Grant Usage (adjust role as needed)
-- ============================================================
-- GRANT USAGE ON WAREHOUSE STRESS_TEST_XSMALL_GEN1 TO ROLE YOUR_ROLE;
-- GRANT USAGE ON WAREHOUSE STRESS_TEST_XSMALL_GEN2 TO ROLE YOUR_ROLE;
-- GRANT USAGE ON WAREHOUSE STRESS_TEST_SMALL_GEN1 TO ROLE YOUR_ROLE;
-- GRANT USAGE ON WAREHOUSE STRESS_TEST_SMALL_GEN2 TO ROLE YOUR_ROLE;
-- GRANT USAGE ON WAREHOUSE STRESS_TEST_MEDIUM_GEN1 TO ROLE YOUR_ROLE;
-- GRANT USAGE ON WAREHOUSE STRESS_TEST_MEDIUM_GEN2 TO ROLE YOUR_ROLE;
