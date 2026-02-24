/*
================================================================================
HEALTHCARE DATA PLATFORM - 07 SYNTHETIC DATA
================================================================================
Execution Order: 7 of 7
Dependencies: All previous scripts (01-06)
Purpose: Generate realistic synthetic healthcare data for development/testing

RUN AS: ACCOUNTADMIN or HEALTHCARE_ENGINEER
WAREHOUSE: WH_COCO_ETL

DATA GENERATION SUMMARY:
┌──────────────────────────────────┬───────────┬─────────────────────────────┐
│ Table                            │ Records   │ Description                 │
├──────────────────────────────────┼───────────┼─────────────────────────────┤
│ RAW_DB.PATIENTS.PATIENTS         │ 1,000     │ Patient demographics        │
│ RAW_DB.PROVIDERS.PROVIDERS       │ 100       │ Healthcare providers        │
│ RAW_DB.ENCOUNTERS.ENCOUNTERS     │ 5,000     │ Patient visits              │
│ RAW_DB.CLAIMS.CLAIMS             │ 8,000     │ Insurance claims            │
│ ANALYTICS_DB.DIMENSIONS.DIM_*    │ varies    │ Dimensional tables          │
│ ANALYTICS_DB.FACTS.FACT_*        │ varies    │ Fact tables                 │
│ AI_READY_DB.FEATURES.PATIENT_*   │ 1,000     │ ML feature store            │
└──────────────────────────────────┴───────────┴─────────────────────────────┘
================================================================================
*/

USE WAREHOUSE WH_COCO_ETL;

-- ============================================================================
-- STEP 7.1: BRONZE LAYER - Raw Patients Data
-- ============================================================================
CREATE OR REPLACE TABLE HEALTHCARE_RAW_DB.PATIENTS.PATIENTS (
    patient_id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(10),
    ssn_masked VARCHAR(11),
    email VARCHAR(100),
    phone VARCHAR(20),
    address_line1 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    insurance_id VARCHAR(20),
    primary_care_provider_id INTEGER,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR(50)
);

INSERT INTO HEALTHCARE_RAW_DB.PATIENTS.PATIENTS
SELECT 
    SEQ4() + 1 AS patient_id,
    ARRAY_CONSTRUCT('John','Jane','Michael','Sarah','David','Emily','Robert','Lisa','William','Jennifer')[UNIFORM(0,9,RANDOM())] AS first_name,
    ARRAY_CONSTRUCT('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Martinez','Wilson')[UNIFORM(0,9,RANDOM())] AS last_name,
    DATEADD(DAY, -UNIFORM(6570, 32850, RANDOM()), CURRENT_DATE()) AS date_of_birth,
    ARRAY_CONSTRUCT('Male','Female','Other')[UNIFORM(0,2,RANDOM())] AS gender,
    CONCAT('XXX-XX-', LPAD(UNIFORM(1000,9999,RANDOM())::VARCHAR, 4, '0')) AS ssn_masked,
    CONCAT(LOWER(ARRAY_CONSTRUCT('john','jane','michael','sarah','david')[UNIFORM(0,4,RANDOM())]), UNIFORM(100,999,RANDOM())::VARCHAR, '@email.com') AS email,
    CONCAT('(', LPAD(UNIFORM(200,999,RANDOM())::VARCHAR,3,'0'), ') ', LPAD(UNIFORM(100,999,RANDOM())::VARCHAR,3,'0'), '-', LPAD(UNIFORM(1000,9999,RANDOM())::VARCHAR,4,'0')) AS phone,
    CONCAT(UNIFORM(100,9999,RANDOM())::VARCHAR, ' ', ARRAY_CONSTRUCT('Main','Oak','Maple','Cedar','Pine')[UNIFORM(0,4,RANDOM())], ' ', ARRAY_CONSTRUCT('St','Ave','Blvd','Dr','Ln')[UNIFORM(0,4,RANDOM())]) AS address_line1,
    ARRAY_CONSTRUCT('New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','Austin')[UNIFORM(0,9,RANDOM())] AS city,
    ARRAY_CONSTRUCT('NY','CA','TX','FL','IL','PA','OH','GA','NC','MI')[UNIFORM(0,9,RANDOM())] AS state,
    LPAD(UNIFORM(10000,99999,RANDOM())::VARCHAR, 5, '0') AS zip_code,
    CONCAT('INS', LPAD(UNIFORM(100000,999999,RANDOM())::VARCHAR, 6, '0')) AS insurance_id,
    UNIFORM(1, 100, RANDOM()) AS primary_care_provider_id,
    DATEADD(DAY, -UNIFORM(0, 365, RANDOM()), CURRENT_TIMESTAMP()) AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    ARRAY_CONSTRUCT('EHR_SYSTEM_A','EHR_SYSTEM_B','LEGACY_IMPORT')[UNIFORM(0,2,RANDOM())] AS source_system
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- ============================================================================
-- STEP 7.2: BRONZE LAYER - Raw Providers Data
-- ============================================================================
CREATE OR REPLACE TABLE HEALTHCARE_RAW_DB.PROVIDERS.PROVIDERS (
    provider_id INTEGER,
    npi VARCHAR(10),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    specialty VARCHAR(100),
    facility_name VARCHAR(200),
    facility_type VARCHAR(50),
    address VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    phone VARCHAR(20),
    email VARCHAR(100),
    license_number VARCHAR(50),
    license_state VARCHAR(2),
    accepting_new_patients BOOLEAN,
    created_at TIMESTAMP_NTZ
);

INSERT INTO HEALTHCARE_RAW_DB.PROVIDERS.PROVIDERS
SELECT 
    SEQ4() + 1 AS provider_id,
    LPAD(UNIFORM(1000000000,1999999999,RANDOM())::VARCHAR, 10, '0') AS npi,
    ARRAY_CONSTRUCT('James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda','William','Elizabeth')[UNIFORM(0,9,RANDOM())] AS first_name,
    ARRAY_CONSTRUCT('Anderson','Thomas','Jackson','White','Harris','Martin','Thompson','Moore','Taylor','Lee')[UNIFORM(0,9,RANDOM())] AS last_name,
    ARRAY_CONSTRUCT('Family Medicine','Internal Medicine','Cardiology','Orthopedics','Pediatrics','Dermatology','Psychiatry','Oncology','Neurology','Emergency Medicine')[UNIFORM(0,9,RANDOM())] AS specialty,
    CONCAT(ARRAY_CONSTRUCT('City','Regional','University','Community','Memorial')[UNIFORM(0,4,RANDOM())], ' ', ARRAY_CONSTRUCT('Hospital','Medical Center','Health System','Clinic','Healthcare')[UNIFORM(0,4,RANDOM())]) AS facility_name,
    ARRAY_CONSTRUCT('Hospital','Clinic','Urgent Care','Specialty Center','Telehealth')[UNIFORM(0,4,RANDOM())] AS facility_type,
    CONCAT(UNIFORM(100,9999,RANDOM())::VARCHAR, ' Medical Center Dr') AS address,
    ARRAY_CONSTRUCT('Boston','Seattle','Denver','Miami','Atlanta','Portland','Nashville','Minneapolis','Detroit','Phoenix')[UNIFORM(0,9,RANDOM())] AS city,
    ARRAY_CONSTRUCT('MA','WA','CO','FL','GA','OR','TN','MN','MI','AZ')[UNIFORM(0,9,RANDOM())] AS state,
    LPAD(UNIFORM(10000,99999,RANDOM())::VARCHAR, 5, '0') AS zip_code,
    CONCAT('(', LPAD(UNIFORM(200,999,RANDOM())::VARCHAR,3,'0'), ') ', LPAD(UNIFORM(100,999,RANDOM())::VARCHAR,3,'0'), '-', LPAD(UNIFORM(1000,9999,RANDOM())::VARCHAR,4,'0')) AS phone,
    CONCAT('dr.', LOWER(ARRAY_CONSTRUCT('james','mary','robert','patricia','john')[UNIFORM(0,4,RANDOM())]), '@healthcare.org') AS email,
    CONCAT('MD', LPAD(UNIFORM(100000,999999,RANDOM())::VARCHAR, 6, '0')) AS license_number,
    ARRAY_CONSTRUCT('MA','WA','CO','FL','GA','OR','TN','MN','MI','AZ')[UNIFORM(0,9,RANDOM())] AS license_state,
    UNIFORM(0,1,RANDOM())::BOOLEAN AS accepting_new_patients,
    DATEADD(DAY, -UNIFORM(0, 730, RANDOM()), CURRENT_TIMESTAMP()) AS created_at
FROM TABLE(GENERATOR(ROWCOUNT => 100));

-- ============================================================================
-- STEP 7.3: BRONZE LAYER - Raw Encounters Data
-- ============================================================================
CREATE OR REPLACE TABLE HEALTHCARE_RAW_DB.ENCOUNTERS.ENCOUNTERS (
    encounter_id INTEGER,
    patient_id INTEGER,
    provider_id INTEGER,
    encounter_date TIMESTAMP_NTZ,
    encounter_type VARCHAR(50),
    chief_complaint VARCHAR(500),
    diagnosis_code VARCHAR(10),
    diagnosis_description VARCHAR(200),
    visit_duration_mins INTEGER,
    disposition VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP_NTZ
);

INSERT INTO HEALTHCARE_RAW_DB.ENCOUNTERS.ENCOUNTERS
SELECT 
    SEQ4() + 1 AS encounter_id,
    UNIFORM(1, 1000, RANDOM()) AS patient_id,
    UNIFORM(1, 100, RANDOM()) AS provider_id,
    DATEADD(MINUTE, UNIFORM(0, 525600, RANDOM()) * -1, CURRENT_TIMESTAMP()) AS encounter_date,
    ARRAY_CONSTRUCT('Office Visit','Emergency','Telehealth','Urgent Care','Preventive','Follow-up','Procedure','Lab Visit')[UNIFORM(0,7,RANDOM())] AS encounter_type,
    ARRAY_CONSTRUCT('Chest pain','Headache','Back pain','Cough','Fatigue','Fever','Shortness of breath','Abdominal pain','Joint pain','Skin rash')[UNIFORM(0,9,RANDOM())] AS chief_complaint,
    CONCAT(ARRAY_CONSTRUCT('I10','E11','J06','M54','K21','F32','J45','I25','N39','G43')[UNIFORM(0,9,RANDOM())], '.', UNIFORM(0,9,RANDOM())::VARCHAR) AS diagnosis_code,
    ARRAY_CONSTRUCT('Hypertension','Type 2 Diabetes','Upper Respiratory Infection','Low Back Pain','GERD','Major Depressive Disorder','Asthma','Coronary Artery Disease','Urinary Tract Infection','Migraine')[UNIFORM(0,9,RANDOM())] AS diagnosis_description,
    UNIFORM(15, 120, RANDOM()) AS visit_duration_mins,
    ARRAY_CONSTRUCT('Discharged','Admitted','Transferred','Left AMA','Referred')[UNIFORM(0,4,RANDOM())] AS disposition,
    CONCAT('Patient presented with ', ARRAY_CONSTRUCT('acute','chronic','mild','moderate','severe')[UNIFORM(0,4,RANDOM())], ' symptoms. Treatment plan initiated.') AS notes,
    CURRENT_TIMESTAMP() AS created_at
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

-- ============================================================================
-- STEP 7.4: BRONZE LAYER - Raw Claims Data
-- ============================================================================
CREATE OR REPLACE TABLE HEALTHCARE_RAW_DB.CLAIMS.CLAIMS (
    claim_id INTEGER,
    patient_id INTEGER,
    provider_id INTEGER,
    encounter_id INTEGER,
    claim_date DATE,
    service_date DATE,
    claim_type VARCHAR(50),
    diagnosis_code VARCHAR(10),
    procedure_code VARCHAR(10),
    procedure_description VARCHAR(200),
    billed_amount DECIMAL(10,2),
    allowed_amount DECIMAL(10,2),
    paid_amount DECIMAL(10,2),
    patient_responsibility DECIMAL(10,2),
    claim_status VARCHAR(30),
    payer_name VARCHAR(100),
    created_at TIMESTAMP_NTZ
);

select * from HEALTHCARE_RAW_DB.CLAIMS.CLAIMS;

INSERT INTO HEALTHCARE_RAW_DB.CLAIMS.CLAIMS
SELECT 
    SEQ4() + 1 AS claim_id,
    UNIFORM(1, 1000, RANDOM()) AS patient_id,
    UNIFORM(1, 100, RANDOM()) AS provider_id,
    UNIFORM(1, 5000, RANDOM()) AS encounter_id,
    DATEADD(DAY, -UNIFORM(0, 365, RANDOM()), CURRENT_DATE()) AS claim_date,
    DATEADD(DAY, -UNIFORM(0, 380, RANDOM()), CURRENT_DATE()) AS service_date,
    ARRAY_CONSTRUCT('Professional','Institutional','Dental','Vision','Pharmacy')[UNIFORM(0,4,RANDOM())] AS claim_type,
    CONCAT(ARRAY_CONSTRUCT('I10','E11','J06','M54','K21','F32','J45','I25','N39','G43')[UNIFORM(0,9,RANDOM())], '.', UNIFORM(0,9,RANDOM())::VARCHAR) AS diagnosis_code,
    CONCAT(UNIFORM(99201,99499,RANDOM())::VARCHAR) AS procedure_code,
    ARRAY_CONSTRUCT('Office Visit - New Patient','Office Visit - Established','Emergency Dept Visit','Preventive Care','Lab Work','X-Ray','MRI','CT Scan','Consultation','Procedure')[UNIFORM(0,9,RANDOM())] AS procedure_description,
    ROUND(UNIFORM(50, 5000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100, 2) AS billed_amount,
    ROUND(UNIFORM(40, 4000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100, 2) AS allowed_amount,
    ROUND(UNIFORM(30, 3500, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100, 2) AS paid_amount,
    ROUND(UNIFORM(0, 500, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100, 2) AS patient_responsibility,
    ARRAY_CONSTRUCT('Paid','Pending','Denied','Partially Paid','Under Review')[UNIFORM(0,4,RANDOM())] AS claim_status,
    ARRAY_CONSTRUCT('Blue Cross Blue Shield','Aetna','UnitedHealthcare','Cigna','Humana','Kaiser','Medicare','Medicaid','Anthem','Oscar')[UNIFORM(0,9,RANDOM())] AS payer_name,
    CURRENT_TIMESTAMP() AS created_at
FROM TABLE(GENERATOR(ROWCOUNT => 8000));

-- ============================================================================
-- STEP 7.5: GOLD LAYER - Dimension Tables
-- ============================================================================

-- DIM_PATIENT: Patient dimension with SCD Type 2 support
CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS_DB.DIMENSIONS.DIM_PATIENT AS
SELECT 
    patient_id,
    first_name,
    last_name,
    date_of_birth,
    DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) AS age,
    CASE 
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 18 THEN 'Pediatric'
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 40 THEN 'Young Adult'
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 65 THEN 'Middle Age'
        ELSE 'Senior'
    END AS age_group,
    gender,
    city,
    state,
    zip_code,
    insurance_id,
    created_at AS effective_from,
    '9999-12-31'::DATE AS effective_to,
    TRUE AS is_current
FROM HEALTHCARE_RAW_DB.PATIENTS.PATIENTS;

-- DIM_PROVIDER: Provider dimension
CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS_DB.DIMENSIONS.DIM_PROVIDER AS
SELECT 
    provider_id,
    npi,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    specialty,
    facility_name,
    facility_type,
    city,
    state,
    accepting_new_patients
FROM HEALTHCARE_RAW_DB.PROVIDERS.PROVIDERS;

-- DIM_DATE: Date dimension
CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS_DB.DIMENSIONS.DIM_DATE AS
SELECT 
    TO_DATE(DATEADD(DAY, SEQ4(), '2020-01-01')) AS date_key,
    YEAR(date_key) AS year,
    QUARTER(date_key) AS quarter,
    MONTH(date_key) AS month,
    MONTHNAME(date_key) AS month_name,
    WEEK(date_key) AS week_of_year,
    DAYOFWEEK(date_key) AS day_of_week,
    DAYNAME(date_key) AS day_name,
    DAYOFMONTH(date_key) AS day_of_month,
    CASE WHEN DAYOFWEEK(date_key) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CONCAT('Q', QUARTER(date_key), ' ', YEAR(date_key)) AS quarter_name,
    CONCAT(YEAR(date_key), '-', LPAD(MONTH(date_key)::VARCHAR, 2, '0')) AS year_month
FROM TABLE(GENERATOR(ROWCOUNT => 2192));

-- ============================================================================
-- STEP 7.6: GOLD LAYER - Fact Tables
-- ============================================================================

-- FACT_ENCOUNTERS: Encounter fact table
CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS_DB.FACTS.FACT_ENCOUNTERS AS
SELECT 
    e.encounter_id,
    e.patient_id,
    e.provider_id,
    e.encounter_date::DATE AS encounter_date_key,
    e.encounter_type,
    e.diagnosis_code,
    e.diagnosis_description,
    e.visit_duration_mins,
    e.disposition,
    1 AS encounter_count
FROM HEALTHCARE_RAW_DB.ENCOUNTERS.ENCOUNTERS e;

-- FACT_CLAIMS: Claims fact table
CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS_DB.FACTS.FACT_CLAIMS AS
SELECT 
    c.claim_id,
    c.patient_id,
    c.provider_id,
    c.encounter_id,
    c.claim_date AS claim_date_key,
    c.service_date AS service_date_key,
    c.claim_type,
    c.diagnosis_code,
    c.procedure_code,
    c.billed_amount,
    c.allowed_amount,
    c.paid_amount,
    c.patient_responsibility,
    c.claim_status,
    c.payer_name,
    1 AS claim_count
FROM HEALTHCARE_RAW_DB.CLAIMS.CLAIMS c;

-- ============================================================================
-- STEP 7.7: PLATINUM LAYER - ML Features
-- ============================================================================

-- Patient Risk Features for ML
CREATE OR REPLACE TABLE HEALTHCARE_AI_READY_DB.FEATURES.PATIENT_RISK_FEATURES AS
SELECT 
    p.patient_id,
    p.age,
    p.age_group,
    p.gender,
    p.state,
    COUNT(DISTINCT e.encounter_id) AS total_encounters,
    COUNT(DISTINCT CASE WHEN e.encounter_type = 'Emergency' THEN e.encounter_id END) AS emergency_visits,
    COUNT(DISTINCT e.diagnosis_code) AS unique_diagnoses,
    AVG(e.visit_duration_mins) AS avg_visit_duration,
    SUM(c.billed_amount) AS total_billed,
    SUM(c.paid_amount) AS total_paid,
    AVG(c.patient_responsibility) AS avg_patient_responsibility,
    DATEDIFF(DAY, MIN(e.encounter_date_key), MAX(e.encounter_date_key)) AS days_in_care,
    COUNT(DISTINCT c.payer_name) AS payer_count,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN e.encounter_type = 'Emergency' THEN e.encounter_id END) >= 3 THEN 'High'
        WHEN COUNT(DISTINCT CASE WHEN e.encounter_type = 'Emergency' THEN e.encounter_id END) >= 1 THEN 'Medium'
        ELSE 'Low'
    END AS risk_category,
    ROUND(
        (COUNT(DISTINCT CASE WHEN e.encounter_type = 'Emergency' THEN e.encounter_id END) * 0.3) +
        (COUNT(DISTINCT e.diagnosis_code) * 0.05) +
        (CASE WHEN p.age > 65 THEN 0.2 ELSE 0 END) +
        UNIFORM(0, 30, RANDOM()) / 100
    , 2) AS risk_score
FROM HEALTHCARE_ANALYTICS_DB.DIMENSIONS.DIM_PATIENT p
LEFT JOIN HEALTHCARE_ANALYTICS_DB.FACTS.FACT_ENCOUNTERS e ON p.patient_id = e.patient_id
LEFT JOIN HEALTHCARE_ANALYTICS_DB.FACTS.FACT_CLAIMS c ON p.patient_id = c.patient_id
GROUP BY p.patient_id, p.age, p.age_group, p.gender, p.state;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT 'PATIENTS' AS table_name, COUNT(*) AS row_count FROM HEALTHCARE_RAW_DB.PATIENTS.PATIENTS
UNION ALL
SELECT 'PROVIDERS', COUNT(*) FROM HEALTHCARE_RAW_DB.PROVIDERS.PROVIDERS
UNION ALL
SELECT 'ENCOUNTERS', COUNT(*) FROM HEALTHCARE_RAW_DB.ENCOUNTERS.ENCOUNTERS
UNION ALL
SELECT 'CLAIMS', COUNT(*) FROM HEALTHCARE_RAW_DB.CLAIMS.CLAIMS
UNION ALL
SELECT 'DIM_PATIENT', COUNT(*) FROM HEALTHCARE_ANALYTICS_DB.DIMENSIONS.DIM_PATIENT
UNION ALL
SELECT 'DIM_PROVIDER', COUNT(*) FROM HEALTHCARE_ANALYTICS_DB.DIMENSIONS.DIM_PROVIDER
UNION ALL
SELECT 'DIM_DATE', COUNT(*) FROM HEALTHCARE_ANALYTICS_DB.DIMENSIONS.DIM_DATE
UNION ALL
SELECT 'FACT_ENCOUNTERS', COUNT(*) FROM HEALTHCARE_ANALYTICS_DB.FACTS.FACT_ENCOUNTERS
UNION ALL
SELECT 'FACT_CLAIMS', COUNT(*) FROM HEALTHCARE_ANALYTICS_DB.FACTS.FACT_CLAIMS
UNION ALL
SELECT 'PATIENT_RISK_FEATURES', COUNT(*) FROM HEALTHCARE_AI_READY_DB.FEATURES.PATIENT_RISK_FEATURES;
