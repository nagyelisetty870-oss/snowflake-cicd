/*
================================================================================
HEALTHCARE DATA PLATFORM - 03 WAREHOUSES & RESOURCE MONITORS
================================================================================
Execution Order: 3 of 7
Dependencies: 02_roles_hierarchy.sql
Purpose: Create compute warehouses and cost control monitors

RUN AS: ACCOUNTADMIN

WAREHOUSE STRATEGY:
┌────────────────────┬─────────┬─────────────┬─────────────────────────────┐
│ Warehouse          │ Size    │ Auto-Suspend│ Purpose                     │
├────────────────────┼─────────┼─────────────┼─────────────────────────────┤
│ WH_COCO            │ XSMALL  │ 60 sec      │ General queries, ad-hoc     │
│ WH_COCO_ETL        │ SMALL   │ 300 sec     │ ETL pipelines, batch jobs   │
│ WH_COCO_ANALYTICS  │ SMALL   │ 120 sec     │ BI dashboards, reporting    │
└────────────────────┴─────────┴─────────────┴─────────────────────────────┘

COST CONTROL:
┌────────────────────────┬─────────────┬────────────────────────────────────┐
│ Resource Monitor       │ Credits/Mo  │ Triggers                           │
├────────────────────────┼─────────────┼────────────────────────────────────┤
│ COCO_MONITOR_GENERAL   │ 20          │ 50% notify, 75%, 90%, 100% suspend │
│ COCO_MONITOR_ETL       │ 40          │ 50% notify, 75%, 90%, 100% suspend │
│ COCO_MONITOR_ANALYTICS │ 30          │ 50% notify, 75%, 90%, 100% suspend │
└────────────────────────┴─────────────┴────────────────────────────────────┘
Total Monthly Budget: 90 credits
================================================================================
*/

-- ============================================================================
-- STEP 3.1: Create Warehouses
-- ============================================================================

-- General Purpose Warehouse (Ad-hoc queries, development)
CREATE OR REPLACE WAREHOUSE WH_COCO
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'General queries and ad-hoc analysis';

-- ETL Warehouse (Data pipelines, batch processing)
CREATE OR REPLACE WAREHOUSE WH_COCO_ETL
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'ETL pipelines and data transformations';

-- Analytics Warehouse (BI dashboards, reporting)
CREATE OR REPLACE WAREHOUSE WH_COCO_ANALYTICS
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'BI dashboards and reporting workloads';

-- ============================================================================
-- STEP 3.2: Create Resource Monitors (Cost Control)
-- ============================================================================

-- Monitor for General Warehouse (20 credits/month)
CREATE OR REPLACE RESOURCE MONITOR COCO_MONITOR_GENERAL
    WITH CREDIT_QUOTA = 20
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Monitor for ETL Warehouse (40 credits/month)
CREATE OR REPLACE RESOURCE MONITOR COCO_MONITOR_ETL
    WITH CREDIT_QUOTA = 40
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Monitor for Analytics Warehouse (30 credits/month)
CREATE OR REPLACE RESOURCE MONITOR COCO_MONITOR_ANALYTICS
    WITH CREDIT_QUOTA = 30
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- ============================================================================
-- STEP 3.3: Assign Resource Monitors to Warehouses
-- ============================================================================
ALTER WAREHOUSE WH_COCO SET RESOURCE_MONITOR = COCO_MONITOR_GENERAL;
ALTER WAREHOUSE WH_COCO_ETL SET RESOURCE_MONITOR = COCO_MONITOR_ETL;
ALTER WAREHOUSE WH_COCO_ANALYTICS SET RESOURCE_MONITOR = COCO_MONITOR_ANALYTICS;

-- ============================================================================
-- STEP 3.4: Grant Warehouse Access to Roles
-- ============================================================================

-- General warehouse: All roles can use
GRANT USAGE ON WAREHOUSE WH_COCO TO ROLE HEALTHCARE_VIEWER;
GRANT USAGE ON WAREHOUSE WH_COCO TO ROLE HEALTHCARE_ANALYST;
GRANT USAGE ON WAREHOUSE WH_COCO TO ROLE HEALTHCARE_ENGINEER;
GRANT USAGE ON WAREHOUSE WH_COCO TO ROLE HEALTHCARE_SCIENTIST;
GRANT ALL ON WAREHOUSE WH_COCO TO ROLE HEALTHCARE_ADMIN;

-- ETL warehouse: Engineers and Admin only
GRANT USAGE ON WAREHOUSE WH_COCO_ETL TO ROLE HEALTHCARE_ENGINEER;
GRANT ALL ON WAREHOUSE WH_COCO_ETL TO ROLE HEALTHCARE_ADMIN;

-- Analytics warehouse: Analysts, Scientists, and Admin
GRANT USAGE ON WAREHOUSE WH_COCO_ANALYTICS TO ROLE HEALTHCARE_ANALYST;
GRANT USAGE ON WAREHOUSE WH_COCO_ANALYTICS TO ROLE HEALTHCARE_SCIENTIST;
GRANT ALL ON WAREHOUSE WH_COCO_ANALYTICS TO ROLE HEALTHCARE_ADMIN;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SHOW WAREHOUSES LIKE 'WH_COCO%';
SHOW RESOURCE MONITORS LIKE 'COCO%';
