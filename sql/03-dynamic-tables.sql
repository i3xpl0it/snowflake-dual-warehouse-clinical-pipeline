-- ====================================================================================
-- Snowflake Dual-Warehouse Clinical Data Pipeline
-- File: 03-dynamic-tables.sql
-- Purpose: Create Dynamic Tables for automated CDC pipeline
-- December 2025 Features: Dynamic Tables, Automated refresh
-- ====================================================================================

USE DATABASE CLINICAL_DATA_PIPELINE;
USE WAREHOUSE CLINICAL_CDC_WH;

-- ====================================================================================
-- Step 1: Create Dynamic Tables in STAGING Schema
-- Dynamic Tables automatically refresh when source streams have new data
-- ====================================================================================

-- Dynamic Table for Patient Staging
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_PATIENTS
TARGET_LAG = '5 MINUTES'
WAREHOUSE = CLINICAL_CDC_WH
AS
SELECT 
    patient_id,
    UPPER(TRIM(first_name)) AS first_name,
    UPPER(TRIM(last_name)) AS last_name,
    date_of_birth,
    UPPER(gender) AS gender,
    CASE 
        WHEN ssn IS NOT NULL THEN CONCAT('***-**-', RIGHT(ssn, 4))
        ELSE NULL 
    END AS ssn_masked,
    phone,
    LOWER(TRIM(email)) AS email,
    address,
    city,
    UPPER(state) AS state,
    zip_code,
    insurance_id,
    CURRENT_TIMESTAMP() AS processed_timestamp,
    _cdc_timestamp,
    _cdc_operation
FROM RAW_DATA.PATIENTS_STREAM
WHERE _cdc_operation IN ('INSERT', 'UPDATE');

-- Dynamic Table for Encounter Staging
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_ENCOUNTERS
TARGET_LAG = '5 MINUTES'
WAREHOUSE = CLINICAL_CDC_WH
AS
SELECT 
    encounter_id,
    patient_id,
    encounter_type,
    encounter_date,
    facility_id,
    provider_id,
    primary_diagnosis_code,
    secondary_diagnosis_codes,
    encounter_status,
    admission_date,
    discharge_date,
    CASE 
        WHEN discharge_date IS NOT NULL AND admission_date IS NOT NULL
        THEN DATEDIFF(day, admission_date, discharge_date)
        ELSE NULL
    END AS length_of_stay_days,
    total_charges,
    CURRENT_TIMESTAMP() AS processed_timestamp,
    _cdc_timestamp,
    _cdc_operation
FROM RAW_DATA.ENCOUNTERS_STREAM
WHERE _cdc_operation IN ('INSERT', 'UPDATE');

-- Dynamic Table for Prescription Staging
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_PRESCRIPTIONS
TARGET_LAG = '5 MINUTES'
WAREHOUSE = CLINICAL_CDC_WH
AS
SELECT 
    prescription_id,
    patient_id,
    encounter_id,
    UPPER(TRIM(medication_name)) AS medication_name,
    medication_code,
    dosage,
    frequency,
    start_date,
    end_date,
    CASE 
        WHEN end_date IS NOT NULL AND start_date IS NOT NULL
        THEN DATEDIFF(day, start_date, end_date)
        ELSE NULL
    END AS treatment_duration_days,
    prescribing_provider_id,
    pharmacy_id,
    refills_remaining,
    status,
    CURRENT_TIMESTAMP() AS processed_timestamp,
    _cdc_timestamp,
    _cdc_operation
FROM RAW_DATA.PRESCRIPTIONS_STREAM
WHERE _cdc_operation IN ('INSERT', 'UPDATE');

-- Dynamic Table for Lab Results Staging
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_LAB_RESULTS
TARGET_LAG = '5 MINUTES'
WAREHOUSE = CLINICAL_CDC_WH
AS
SELECT 
    lab_result_id,
    patient_id,
    encounter_id,
    UPPER(TRIM(test_name)) AS test_name,
    test_code,
    result_value,
    result_unit,
    reference_range,
    abnormal_flag,
    result_date,
    ordering_provider_id,
    lab_facility_id,
    CASE 
        WHEN abnormal_flag IN ('H', 'L', 'A') THEN TRUE
        ELSE FALSE
    END AS is_abnormal,
    CURRENT_TIMESTAMP() AS processed_timestamp,
    _cdc_timestamp,
    _cdc_operation
FROM RAW_DATA.LAB_RESULTS_STREAM
WHERE _cdc_operation IN ('INSERT', 'UPDATE');

-- Dynamic Table for Vital Signs Staging
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_VITAL_SIGNS
TARGET_LAG = '5 MINUTES'
WAREHOUSE = CLINICAL_CDC_WH
AS
SELECT 
    vital_sign_id,
    patient_id,
    encounter_id,
    measurement_date,
    blood_pressure_systolic,
    blood_pressure_diastolic,
    heart_rate,
    temperature,
    respiratory_rate,
    oxygen_saturation,
    weight_kg,
    height_cm,
    bmi,
    -- Clinical flags
    CASE 
        WHEN blood_pressure_systolic >= 140 OR blood_pressure_diastolic >= 90 
        THEN 'HYPERTENSIVE'
        WHEN blood_pressure_systolic < 90 OR blood_pressure_diastolic < 60 
        THEN 'HYPOTENSIVE'
        ELSE 'NORMAL'
    END AS bp_status,
    CASE 
        WHEN temperature > 38.0 THEN 'FEVER'
        WHEN temperature < 36.0 THEN 'HYPOTHERMIA'
        ELSE 'NORMAL'
    END AS temp_status,
    CURRENT_TIMESTAMP() AS processed_timestamp,
    _cdc_timestamp,
    _cdc_operation
FROM RAW_DATA.VITAL_SIGNS_STREAM
WHERE _cdc_operation IN ('INSERT', 'UPDATE');

-- ====================================================================================
-- Step 2: Create Materialized Views for Performance
-- ====================================================================================

-- Patient Summary Materialized View
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_PATIENT_SUMMARY
TARGET_LAG = '15 MINUTES'
WAREHOUSE = CLINICAL_CDC_WH
AS
SELECT 
    p.patient_id,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    DATEDIFF(year, p.date_of_birth, CURRENT_DATE()) AS age,
    p.gender,
    p.state,
    COUNT(DISTINCT e.encounter_id) AS total_encounters,
    COUNT(DISTINCT rx.prescription_id) AS total_prescriptions,
    COUNT(DISTINCT lr.lab_result_id) AS total_lab_results,
    MAX(e.encounter_date) AS last_encounter_date,
    SUM(e.total_charges) AS lifetime_charges,
    CURRENT_TIMESTAMP() AS summary_updated_at
FROM STAGING.DT_PATIENTS p
LEFT JOIN STAGING.DT_ENCOUNTERS e ON p.patient_id = e.patient_id
LEFT JOIN STAGING.DT_PRESCRIPTIONS rx ON p.patient_id = rx.patient_id
LEFT JOIN STAGING.DT_LAB_RESULTS lr ON p.patient_id = lr.patient_id
GROUP BY 
    p.patient_id,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    p.gender,
    p.state;

-- ====================================================================================
-- Step 3: Grant Permissions
-- ====================================================================================

GRANT SELECT ON ALL TABLES IN SCHEMA STAGING TO ROLE CLINICAL_DATA_ENGINEER;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA STAGING TO ROLE CLINICAL_DATA_ENGINEER;

-- ====================================================================================
-- Step 4: Monitor Dynamic Tables
-- ====================================================================================

-- View Dynamic Table refresh history
SELECT 
    name,
    target_lag,
    warehouse_name,
    refresh_mode,
    last_refresh_time,
    next_refresh_time
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
WHERE schema_name = 'STAGING'
ORDER BY name;

SELECT 'Dynamic Tables setup completed successfully' AS status;
