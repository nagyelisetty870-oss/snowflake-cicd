/*
================================================================================
HEALTHCARE DATA PLATFORM - 01 ACCOUNT SETUP
================================================================================
Execution Order: 1 of 7
Dependencies: None (Run first as ACCOUNTADMIN)
Purpose: Network policies, security settings, cross-region Cortex

RUN AS: ACCOUNTADMIN
================================================================================
*/

-- ============================================================================
-- STEP 1.1: Enable Cross-Region Cortex AI
-- ============================================================================
-- Required for Cortex AI features across regions
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ============================================================================
-- STEP 1.2: Create Network Policy
-- ============================================================================
-- Controls which IP addresses can connect to Snowflake
-- NOTE: '0.0.0.0/0' allows all IPs - RESTRICT IN PRODUCTION
CREATE OR REPLACE NETWORK POLICY COCO_NETWORK_POLICY
    ALLOWED_IP_LIST = ('0.0.0.0/0')
    COMMENT = 'Healthcare platform network policy - restrict IPs in production';

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SHOW NETWORK POLICIES LIKE 'COCO%';
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION';

