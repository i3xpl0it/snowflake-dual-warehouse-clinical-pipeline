-- ====================================================================================
-- Snowflake Dual-Warehouse Clinical Data Pipeline
-- File: 04-interactive-tables.sql
-- Purpose: Create Interactive Tables for analytics using Interactive Warehouse
-- December 2025 Features: Interactive Tables, Optimized for low-latency queries
-- ====================================================================================

USE DATABASE CLINICAL_DATA_PIPELINE;
USE WAREHOUSE CLINICAL_INTERACTIVE_WH;

-- ====================================================================================
-- Step 1: Create Analytics Tables in ANALYTICS Schema
-- Using Interactive Warehouse for fast, concurrent queries
-- ====================================================================================

-- Patient Analytics Table
CREATE OR REPLACE TABLE ANALYTICS.PATIENTS_FACT AS
SELECT 
    patient_id,
    first_name,
    last_name,
    date_of_birth,
    DATEDIFF(year, date_of_birth, CURRENT_DATE()) AS current_age,
    CASE 
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 18 THEN 'Pediatric'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 18 AND 64 THEN 'Adult'
        ELSE 'Senior'
    END AS age_group,
    gender,
    state,
    email,
    phone,
    CURRENT_TIMESTAMP() AS created_at
FROM STAGING.DT_PATIENTS
WHERE _cdc_operation != 'DELETE';

-- Encounter Analytics Table
CREATE OR REPLACE TABLE ANALYTICS.ENCOUNTERS_FACT AS
SELECT 
    e.encounter_id,
    e.patient_id,
    e.encounter_type,
    e.encounter_date,
    YEAR(e.encounter_date) AS encounter_year,
    QUARTER(e.encounter_date) AS encounter_quarter,
    MONTH(e.encounter_date) AS encounter_month,
    DAYOFWEEK(e.encounter_date) AS encounter_day_of_week,
    e.facility_id,
    e.provider_id,
    e.primary_diagnosis_code,
    e.encounter_status,
    e.admission_date,
    e.discharge_date,
    e.length_of_stay_days,
    e.total_charges,
    CURRENT_TIMESTAMP() AS created_at
FROM STAGING.DT_ENCOUNTERS e
WHERE e._cdc_operation != 'DELETE';

-- Prescription Analytics Table
CREATE OR REPLACE TABLE ANALYTICS.PRESCRIPTIONS_FACT AS
SELECT 
    rx.prescription_id,
    rx.patient_id,
    rx.encounter_id,
    rx.medication_name,
    rx.medication_code,
    rx.dosage,
    rx.frequency,
    rx.start_date,
    rx.end_date,
    rx.treatment_duration_days,
    rx.prescribing_provider_id,
    rx.pharmacy_id,
    rx.refills_remaining,
    rx.status,
    CURRENT_TIMESTAMP() AS created_at
FROM STAGING.DT_PRESCRIPTIONS rx
WHERE rx._cdc_operation != 'DELETE';

-- Lab Results Analytics Table
CREATE OR REPLACE TABLE ANALYTICS.LAB_RESULTS_FACT AS
SELECT 
    lr.lab_result_id,
    lr.patient_id,
    lr.encounter_id,
    lr.test_name,
    lr.test_code,
    lr.result_value,
    lr.result_unit,
    lr.reference_range,
    lr.abnormal_flag,
    lr.is_abnormal,
    lr.result_date,
    YEAR(lr.result_date) AS result_year,
    QUARTER(lr.result_date) AS result_quarter,
    lr.ordering_provider_id,
    lr.lab_facility_id,
    CURRENT_TIMESTAMP() AS created_at
FROM STAGING.DT_LAB_RESULTS lr
WHERE lr._cdc_operation != 'DELETE';

-- Vital Signs Analytics Table
CREATE OR REPLACE TABLE ANALYTICS.VITAL_SIGNS_FACT AS
SELECT 
    vs.vital_sign_id,
    vs.patient_id,
    vs.encounter_id,
    vs.measurement_date,
    vs.blood_pressure_systolic,
    vs.blood_pressure_diastolic,
    vs.heart_rate,
    vs.temperature,
    vs.respiratory_rate,
    vs.oxygen_saturation,
    vs.weight_kg,
    vs.height_cm,
    vs.bmi,
    vs.bp_status,
    vs.temp_status,
    CASE 
        WHEN vs.bmi < 18.5 THEN 'Underweight'
        WHEN vs.bmi BETWEEN 18.5 AND 24.9 THEN 'Normal'
        WHEN vs.bmi BETWEEN 25 AND 29.9 THEN 'Overweight'
        ELSE 'Obese'
    END AS bmi_category,
    CURRENT_TIMESTAMP() AS created_at
FROM STAGING.DT_VITAL_SIGNS vs
WHERE vs._cdc_operation != 'DELETE';

-- ====================================================================================
-- Step 2: Create Business Intelligence Views
-- ====================================================================================

-- Patient 360 View
CREATE OR REPLACE VIEW ANALYTICS.VW_PATIENT_360 AS
SELECT 
    p.patient_id,
    p.first_name,
    p.last_name,
    p.current_age,
    p.age_group,
    p.gender,
    p.state,
    COUNT(DISTINCT e.encounter_id) AS total_encounters,
    COUNT(DISTINCT CASE WHEN e.encounter_date >= DATEADD(year, -1, CURRENT_DATE()) 
                        THEN e.encounter_id END) AS encounters_last_year,
    MAX(e.encounter_date) AS last_encounter_date,
    SUM(e.total_charges) AS lifetime_total_charges,
    COUNT(DISTINCT rx.prescription_id) AS total_prescriptions,
    COUNT(DISTINCT lr.lab_result_id) AS total_lab_tests,
    COUNT(DISTINCT CASE WHEN lr.is_abnormal = TRUE THEN lr.lab_result_id END) AS abnormal_lab_tests,
    AVG(vs.bmi) AS avg_bmi,
    AVG(vs.blood_pressure_systolic) AS avg_systolic_bp,
    AVG(vs.blood_pressure_diastolic) AS avg_diastolic_bp
FROM ANALYTICS.PATIENTS_FACT p
LEFT JOIN ANALYTICS.ENCOUNTERS_FACT e ON p.patient_id = e.patient_id
LEFT JOIN ANALYTICS.PRESCRIPTIONS_FACT rx ON p.patient_id = rx.patient_id
LEFT JOIN ANALYTICS.LAB_RESULTS_FACT lr ON p.patient_id = lr.patient_id
LEFT JOIN ANALYTICS.VITAL_SIGNS_FACT vs ON p.patient_id = vs.patient_id
GROUP BY 
    p.patient_id, p.first_name, p.last_name, p.current_age, 
    p.age_group, p.gender, p.state;

-- High-Risk Patient View
CREATE OR REPLACE VIEW ANALYTICS.VW_HIGH_RISK_PATIENTS AS
SELECT 
    p.*,
    'Multiple abnormal lab results' AS risk_factor
FROM ANALYTICS.VW_PATIENT_360 p
WHERE p.abnormal_lab_tests >= 5
   OR p.avg_systolic_bp > 140
   OR p.avg_diastolic_bp > 90
   OR p.avg_bmi > 30;

-- Monthly Encounter Metrics View
CREATE OR REPLACE VIEW ANALYTICS.VW_MONTHLY_ENCOUNTER_METRICS AS
SELECT 
    encounter_year,
    encounter_quarter,
    encounter_month,
    encounter_type,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT patient_id) AS unique_patients,
    AVG(length_of_stay_days) AS avg_length_of_stay,
    SUM(total_charges) AS total_charges,
    AVG(total_charges) AS avg_charges_per_encounter
FROM ANALYTICS.ENCOUNTERS_FACT
GROUP BY 
    encounter_year, encounter_quarter, encounter_month, encounter_type
ORDER BY 
    encounter_year DESC, encounter_month DESC;

-- Top Medications View
CREATE OR REPLACE VIEW ANALYTICS.VW_TOP_MEDICATIONS AS
SELECT 
    medication_name,
    medication_code,
    COUNT(DISTINCT patient_id) AS unique_patients,
    COUNT(*) AS total_prescriptions,
    AVG(treatment_duration_days) AS avg_treatment_days
FROM ANALYTICS.PRESCRIPTIONS_FACT
WHERE status = 'ACTIVE'
GROUP BY medication_name, medication_code
ORDER BY total_prescriptions DESC
LIMIT 100;

-- ====================================================================================
-- Step 3: Create Clustered Tables for Performance
-- ====================================================================================

ALTER TABLE ANALYTICS.ENCOUNTERS_FACT 
    CLUSTER BY (patient_id, encounter_date);

ALTER TABLE ANALYTICS.LAB_RESULTS_FACT 
    CLUSTER BY (patient_id, result_date);

ALTER TABLE ANALYTICS.PRESCRIPTIONS_FACT 
    CLUSTER BY (patient_id, start_date);

-- ====================================================================================
-- Step 4: Grant Permissions
-- ====================================================================================

GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE CLINICAL_DATA_ENGINEER;
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS TO ROLE CLINICAL_DATA_ENGINEER;

-- ====================================================================================
-- Step 5: Verify Setup
-- ====================================================================================

-- Check row counts
SELECT 'PATIENTS_FACT' AS table_name, COUNT(*) AS row_count FROM ANALYTICS.PATIENTS_FACT
UNION ALL
SELECT 'ENCOUNTERS_FACT', COUNT(*) FROM ANALYTICS.ENCOUNTERS_FACT
UNION ALL
SELECT 'PRESCRIPTIONS_FACT', COUNT(*) FROM ANALYTICS.PRESCRIPTIONS_FACT
UNION ALL
SELECT 'LAB_RESULTS_FACT', COUNT(*) FROM ANALYTICS.LAB_RESULTS_FACT
UNION ALL
SELECT 'VITAL_SIGNS_FACT', COUNT(*) FROM ANALYTICS.VITAL_SIGNS_FACT;

SELECT 'Interactive Tables setup completed successfully' AS status;
