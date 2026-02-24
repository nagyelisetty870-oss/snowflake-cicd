/*
================================================================================
HEALTHCARE DATA PLATFORM - 04 DATABASES & SCHEMAS (Medallion Architecture)
================================================================================
Execution Order: 4 of 7
Dependencies: 03_warehouses_monitors.sql
Purpose: Create 4-layer data architecture (Bronze → Silver → Gold → Platinum)

RUN AS: ACCOUNTADMIN

DATA ARCHITECTURE (Medallion Pattern):
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                       │
│                                                                              │
│  [Sources] → BRONZE → SILVER → GOLD → PLATINUM → [ML/AI]                    │
│              (Raw)   (Clean)  (BI)   (AI-Ready)                             │
└─────────────────────────────────────────────────────────────────────────────┘

DATABASE STRUCTURE:
┌──────────────────────┬─────────────┬────────────────────────────────────────┐
│ Database             │ Layer       │ Schemas                                │
├──────────────────────┼─────────────┼────────────────────────────────────────┤
│ HEALTHCARE_RAW_DB    │ Bronze      │ PATIENTS, CLAIMS, PROVIDERS, ENCOUNTERS│
│ HEALTHCARE_TRANSFORM │ Silver      │ STAGING, CLEANED, INTEGRATED           │
│ HEALTHCARE_ANALYTICS │ Gold        │ DIMENSIONS, FACTS, REPORTS, DASHBOARDS │
│ HEALTHCARE_AI_READY  │ Platinum    │ FEATURES, EMBEDDINGS, SEMANTIC         │
└──────────────────────┴─────────────┴────────────────────────────────────────┘
================================================================================
*/

-- ============================================================================
-- STEP 4.1: BRONZE LAYER - Raw Data (HEALTHCARE_RAW_DB)
-- ============================================================================
-- Landing zone for raw data ingestion, no transformations applied
CREATE OR REPLACE DATABASE HEALTHCARE_RAW_DB
    COMMENT = 'Bronze Layer: Raw data ingestion, no transformations';

CREATE OR REPLACE SCHEMA HEALTHCARE_RAW_DB.PATIENTS;
CREATE OR REPLACE SCHEMA HEALTHCARE_RAW_DB.CLAIMS;
CREATE OR REPLACE SCHEMA HEALTHCARE_RAW_DB.PROVIDERS;
CREATE OR REPLACE SCHEMA HEALTHCARE_RAW_DB.ENCOUNTERS;

-- ============================================================================
-- STEP 4.2: SILVER LAYER - Transformed Data (HEALTHCARE_TRANSFORM_DB)
-- ============================================================================
-- Cleaned, validated, and standardized data
CREATE OR REPLACE DATABASE HEALTHCARE_TRANSFORM_DB
    COMMENT = 'Silver Layer: Cleaned, validated, standardized data';

CREATE OR REPLACE SCHEMA HEALTHCARE_TRANSFORM_DB.STAGING;
CREATE OR REPLACE SCHEMA HEALTHCARE_TRANSFORM_DB.CLEANED;
CREATE OR REPLACE SCHEMA HEALTHCARE_TRANSFORM_DB.INTEGRATED;

-- ============================================================================
-- STEP 4.3: GOLD LAYER - Analytics Data (HEALTHCARE_ANALYTICS_DB)
-- ============================================================================
-- Business-ready dimensional models, aggregations, KPIs
CREATE OR REPLACE DATABASE HEALTHCARE_ANALYTICS_DB
    COMMENT = 'Gold Layer: Dimensional models, aggregations, KPIs';

CREATE OR REPLACE SCHEMA HEALTHCARE_ANALYTICS_DB.DIMENSIONS;
CREATE OR REPLACE SCHEMA HEALTHCARE_ANALYTICS_DB.FACTS;
CREATE OR REPLACE SCHEMA HEALTHCARE_ANALYTICS_DB.REPORTS;
CREATE OR REPLACE SCHEMA HEALTHCARE_ANALYTICS_DB.DASHBOARDS;

-- ============================================================================
-- STEP 4.4: PLATINUM LAYER - AI-Ready Data (HEALTHCARE_AI_READY_DB)
-- ============================================================================
-- ML features, vector embeddings, semantic models
CREATE OR REPLACE DATABASE HEALTHCARE_AI_READY_DB
    COMMENT = 'Platinum Layer: ML features, embeddings, semantic models';

CREATE OR REPLACE SCHEMA HEALTHCARE_AI_READY_DB.FEATURES;
CREATE OR REPLACE SCHEMA HEALTHCARE_AI_READY_DB.EMBEDDINGS;
CREATE OR REPLACE SCHEMA HEALTHCARE_AI_READY_DB.SEMANTIC;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SHOW DATABASES LIKE 'HEALTHCARE%';
SHOW SCHEMAS IN DATABASE HEALTHCARE_RAW_DB;
SHOW SCHEMAS IN DATABASE HEALTHCARE_TRANSFORM_DB;
SHOW SCHEMAS IN DATABASE HEALTHCARE_ANALYTICS_DB;
SHOW SCHEMAS IN DATABASE HEALTHCARE_AI_READY_DB;
