/*
================================================================================
HEALTHCARE DATA PLATFORM - 02 ROLES & HIERARCHY
================================================================================
Execution Order: 2 of 7
Dependencies: 01_account_setup.sql
Purpose: Create RBAC role hierarchy for data access control

RUN AS: ACCOUNTADMIN

ROLE HIERARCHY:
                    ACCOUNTADMIN
                         │
                  HEALTHCARE_ADMIN
                   /     |      \
        HEALTHCARE_   HEALTHCARE_  HEALTHCARE_
        ANALYST       ENGINEER     SCIENTIST
              \          |          /
               \         |         /
                 HEALTHCARE_VIEWER (Base role)
================================================================================
*/

-- ============================================================================
-- STEP 2.1: Create Base Roles
-- ============================================================================
-- Viewer: Read-only access to analytics (inherited by all other roles)
CREATE ROLE IF NOT EXISTS HEALTHCARE_VIEWER
    COMMENT = 'Base role: Read-only access to analytics data';

-- Analyst: BI/Analytics team - Gold layer access
CREATE ROLE IF NOT EXISTS HEALTHCARE_ANALYST
    COMMENT = 'BI/Analytics team: Read access to Gold layer (ANALYTICS_DB)';

-- Engineer: Data Engineering team - Bronze/Silver layer access
CREATE ROLE IF NOT EXISTS HEALTHCARE_ENGINEER
    COMMENT = 'Data Engineering: Read/Write access to Bronze/Silver layers';

-- Scientist: Data Science team - Platinum layer access
CREATE ROLE IF NOT EXISTS HEALTHCARE_SCIENTIST
    COMMENT = 'Data Science: Read/Write access to Platinum layer (AI_READY_DB)';

-- Admin: Full administrative access
CREATE ROLE IF NOT EXISTS HEALTHCARE_ADMIN
    COMMENT = 'Admin: Full access to all Healthcare platform resources';

-- ============================================================================
-- STEP 2.2: Build Role Hierarchy
-- ============================================================================
-- All roles inherit from VIEWER (base permissions)
GRANT ROLE HEALTHCARE_VIEWER TO ROLE HEALTHCARE_ANALYST;
GRANT ROLE HEALTHCARE_VIEWER TO ROLE HEALTHCARE_ENGINEER;
GRANT ROLE HEALTHCARE_VIEWER TO ROLE HEALTHCARE_SCIENTIST;

-- ADMIN inherits from all functional roles
GRANT ROLE HEALTHCARE_ANALYST TO ROLE HEALTHCARE_ADMIN;
GRANT ROLE HEALTHCARE_ENGINEER TO ROLE HEALTHCARE_ADMIN;
GRANT ROLE HEALTHCARE_SCIENTIST TO ROLE HEALTHCARE_ADMIN;

-- ADMIN reports to ACCOUNTADMIN
GRANT ROLE HEALTHCARE_ADMIN TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SHOW ROLES LIKE 'HEALTHCARE%';
SHOW GRANTS ON ROLE HEALTHCARE_ADMIN;
