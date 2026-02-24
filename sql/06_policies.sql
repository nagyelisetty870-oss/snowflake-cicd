/*
================================================================================
HEALTHCARE DATA PLATFORM - 06 POLICIES (Session & Network)
================================================================================
Execution Order: 6 of 7
Dependencies: 05_grants_permissions.sql
Purpose: Security policies for session management and network access

RUN AS: ACCOUNTADMIN
================================================================================
*/

-- ============================================================================
-- STEP 6.1: Session Policy (Auto-logout for security)
-- ============================================================================
-- Automatically logs out inactive users after 30 minutes
CREATE OR REPLACE SESSION POLICY HEALTHCARE_RAW_DB.PUBLIC.HEALTHCARE_SESSION_POLICY
    SESSION_IDLE_TIMEOUT_MINS = 30
    SESSION_UI_IDLE_TIMEOUT_MINS = 30
    COMMENT = 'Session timeout policy for Healthcare project';

-- ============================================================================
-- STEP 6.2: Apply Session Policy to Account
-- ============================================================================
ALTER ACCOUNT SET SESSION POLICY HEALTHCARE_RAW_DB.PUBLIC.HEALTHCARE_SESSION_POLICY;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SHOW SESSION POLICIES;
SHOW PARAMETERS LIKE 'SESSION%';
