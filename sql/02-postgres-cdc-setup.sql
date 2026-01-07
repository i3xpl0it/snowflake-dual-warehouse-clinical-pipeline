-- ====================================================================================
-- Snowflake Dual-Warehouse Clinical Data Pipeline
-- File: 02-postgres-cdc-setup.sql
-- Purpose: Configure PostgreSQL CDC streams using External Volume
-- December 2025 Features: External Volume CDC, Change Data Capture streams
-- ====================================================================================

USE DATABASE CLINICAL_DATA_PIPELINE;
USE WAREHOUSE CLINICAL_CDC_WH;

-- ====================================================================================
-- Step 1: Create External Volume Connection to PostgreSQL
-- ====================================================================================

-- External Volume for PostgreSQL CDC (configured in 01-setup-environment.sql)
-- Connection details should be provided via external configuration

-- ====================================================================================
-- Step 2: Create Raw Data Tables from PostgreSQL CDC
-- ====================================================================================

-- Patient Demographics Table (from EHR system)
CREATE OR REPLACE TABLE RAW_DATA.PATIENTS (
    patient_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    ssn STRING,
    phone STRING,
    email STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    insurance_id STRING,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _cdc_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _cdc_operation STRING
)
COMMENT = 'Patient demographics from PostgreSQL EHR via CDC';

-- Clinical Encounters Table
CREATE OR REPLACE TABLE RAW_DATA.ENCOUNTERS (
    encounter_id STRING,
    patient_id STRING,
    encounter_type STRING,
    encounter_date TIMESTAMP_NTZ,
    facility_id STRING,
    provider_id STRING,
    primary_diagnosis_code STRING,
    secondary_diagnosis_codes ARRAY,
    encounter_status STRING,
    admission_date TIMESTAMP_NTZ,
    discharge_date TIMESTAMP_NTZ,
    total_charges NUMBER(18,2),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _cdc_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _cdc_operation STRING
)
COMMENT = 'Clinical encounters from PostgreSQL EHR via CDC';

-- Prescriptions Table
CREATE OR REPLACE TABLE RAW_DATA.PRESCRIPTIONS (
    prescription_id STRING,
    patient_id STRING,
    encounter_id STRING,
    medication_name STRING,
    medication_code STRING,
    dosage STRING,
    frequency STRING,
    start_date DATE,
    end_date DATE,
    prescribing_provider_id STRING,
    pharmacy_id STRING,
    refills_remaining INTEGER,
    status STRING,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _cdc_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _cdc_operation STRING
)
COMMENT = 'Prescriptions from PostgreSQL EHR via CDC';

-- Lab Results Table
CREATE OR REPLACE TABLE RAW_DATA.LAB_RESULTS (
    lab_result_id STRING,
    patient_id STRING,
    encounter_id STRING,
    test_name STRING,
    test_code STRING,
    result_value STRING,
    result_unit STRING,
    reference_range STRING,
    abnormal_flag STRING,
    result_date TIMESTAMP_NTZ,
    ordering_provider_id STRING,
    lab_facility_id STRING,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _cdc_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _cdc_operation STRING
)
COMMENT = 'Lab results from PostgreSQL EHR via CDC';

-- Vital Signs Table
CREATE OR REPLACE TABLE RAW_DATA.VITAL_SIGNS (
    vital_sign_id STRING,
    patient_id STRING,
    encounter_id STRING,
    measurement_date TIMESTAMP_NTZ,
    blood_pressure_systolic INTEGER,
    blood_pressure_diastolic INTEGER,
    heart_rate INTEGER,
    temperature NUMBER(4,1),
    respiratory_rate INTEGER,
    oxygen_saturation NUMBER(5,2),
    weight_kg NUMBER(6,2),
    height_cm NUMBER(6,2),
    bmi NUMBER(5,2),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _cdc_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _cdc_operation STRING
)
COMMENT = 'Vital signs from PostgreSQL EHR via CDC';

-- ====================================================================================
-- Step 3: Create CDC Streams on Raw Tables
-- ====================================================================================

-- Stream for tracking changes to Patients
CREATE OR REPLACE STREAM RAW_DATA.PATIENTS_STREAM 
    ON TABLE RAW_DATA.PATIENTS
    COMMENT = 'Change data capture stream for patient demographics';

-- Stream for tracking changes to Encounters
CREATE OR REPLACE STREAM RAW_DATA.ENCOUNTERS_STREAM 
    ON TABLE RAW_DATA.ENCOUNTERS
    COMMENT = 'Change data capture stream for clinical encounters';

-- Stream for tracking changes to Prescriptions
CREATE OR REPLACE STREAM RAW_DATA.PRESCRIPTIONS_STREAM 
    ON TABLE RAW_DATA.PRESCRIPTIONS
    COMMENT = 'Change data capture stream for prescriptions';

-- Stream for tracking changes to Lab Results
CREATE OR REPLACE STREAM RAW_DATA.LAB_RESULTS_STREAM 
    ON TABLE RAW_DATA.LAB_RESULTS
    COMMENT = 'Change data capture stream for lab results';

-- Stream for tracking changes to Vital Signs
CREATE OR REPLACE STREAM RAW_DATA.VITAL_SIGNS_STREAM 
    ON TABLE RAW_DATA.VITAL_SIGNS
    COMMENT = 'Change data capture stream for vital signs';

-- ====================================================================================
-- Step 4: Grant Permissions
-- ====================================================================================

GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE CLINICAL_DATA_ENGINEER;
GRANT SELECT ON ALL STREAMS IN SCHEMA RAW_DATA TO ROLE CLINICAL_DATA_ENGINEER;

-- ====================================================================================
-- Step 5: Verify Setup
-- ====================================================================================

-- Check streams
SHOW STREAMS IN SCHEMA RAW_DATA;

-- Verify table structures
DESCRIBE TABLE RAW_DATA.PATIENTS;
DESCRIBE TABLE RAW_DATA.ENCOUNTERS;

SELECT 'PostgreSQL CDC setup completed successfully' AS status;
