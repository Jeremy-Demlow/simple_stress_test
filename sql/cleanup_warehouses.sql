-- ============================================================
-- Snowflake Stress Test Warehouse Cleanup
-- ============================================================
-- This script drops all test warehouses
-- Run with: snow sql -f sql/cleanup_warehouses.sql -c stress_testing
-- ============================================================

-- Set role (adjust if needed)
USE ROLE ACCOUNTADMIN;

-- ============================================================
-- Suspend all test warehouses first
-- ============================================================

ALTER WAREHOUSE IF EXISTS STRESS_TEST_XSMALL_GEN1 SUSPEND;
ALTER WAREHOUSE IF EXISTS STRESS_TEST_XSMALL_GEN2 SUSPEND;
ALTER WAREHOUSE IF EXISTS STRESS_TEST_SMALL_GEN1 SUSPEND;
ALTER WAREHOUSE IF EXISTS STRESS_TEST_SMALL_GEN2 SUSPEND;
ALTER WAREHOUSE IF EXISTS STRESS_TEST_MEDIUM_GEN1 SUSPEND;
ALTER WAREHOUSE IF EXISTS STRESS_TEST_MEDIUM_GEN2 SUSPEND;

-- ============================================================
-- Drop all test warehouses
-- ============================================================

DROP WAREHOUSE IF EXISTS STRESS_TEST_XSMALL_GEN1;
DROP WAREHOUSE IF EXISTS STRESS_TEST_XSMALL_GEN2;
DROP WAREHOUSE IF EXISTS STRESS_TEST_SMALL_GEN1;
DROP WAREHOUSE IF EXISTS STRESS_TEST_SMALL_GEN2;
DROP WAREHOUSE IF EXISTS STRESS_TEST_MEDIUM_GEN1;
DROP WAREHOUSE IF EXISTS STRESS_TEST_MEDIUM_GEN2;

-- ============================================================
-- Verify Cleanup
-- ============================================================

SHOW WAREHOUSES LIKE 'STRESS_TEST%';
