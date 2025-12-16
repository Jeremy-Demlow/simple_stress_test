-- ============================================================
-- Suspend All Test Warehouses
-- ============================================================
-- Run this to suspend all test warehouses and stop credit usage
-- Run with: snow sql -f sql/suspend_warehouses.sql -c stress_testing
-- ============================================================

ALTER WAREHOUSE IF EXISTS STRESS_TEST_XSMALL_GEN1 SUSPEND;
ALTER WAREHOUSE IF EXISTS STRESS_TEST_XSMALL_GEN2 SUSPEND;
ALTER WAREHOUSE IF EXISTS STRESS_TEST_SMALL_GEN1 SUSPEND;
ALTER WAREHOUSE IF EXISTS STRESS_TEST_SMALL_GEN2 SUSPEND;
ALTER WAREHOUSE IF EXISTS STRESS_TEST_MEDIUM_GEN1 SUSPEND;
ALTER WAREHOUSE IF EXISTS STRESS_TEST_MEDIUM_GEN2 SUSPEND;

-- Show status
SHOW WAREHOUSES LIKE 'STRESS_TEST%';
