/*******************************************************************************
 * Snowflake WORM Backups - Healthcare Compliance Project
 * File: 02-healthcare-data.sql
 * Phase: 2 - Sample Healthcare Data
 * 
 * Description:
 *   Creates realistic healthcare data structures and sample data
 *   - Patient demographics table (HIPAA PHI)
 *   - Clinical encounters table
 *   - Lab results table
 *   - 1000+ sample records for testing
 *
 * Author: i3xpl0it
 * Created: January 4, 2026
 * Snowflake Version: 9.39+ (WORM Backups GA)
 * 
 * Prerequisites:
 *   - Phase 1 completed (01-setup-prerequisites.sql)
 *   - compliance_admin role access
 *
 * Estimated Runtime: 3-5 minutes
 ******************************************************************************/

-- Use compliance_admin role for data operations
USE ROLE compliance_admin;
USE DATABASE healthcare_prod;
USE WAREHOUSE compliance_wh;

-------------------------------------------------------------------------------
-- STEP 1: Create Patient Demographics Table
-------------------------------------------------------------------------------

USE SCHEMA patient_data;

CREATE OR REPLACE TABLE patients (
    patient_id VARCHAR(50) PRIMARY KEY,
    mrn VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender VARCHAR(20),
    ssn VARCHAR(11),
    address VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    phone VARCHAR(20),
    email VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(100) DEFAULT CURRENT_USER()
)
COMMENT = 'Patient demographics - Contains PHI (Protected Health Information)';

SELECT 'Step 1 Complete: patients table created' AS status;

-------------------------------------------------------------------------------
-- STEP 2: Create Clinical Encounters Table
-------------------------------------------------------------------------------

USE SCHEMA clinical_data;

CREATE OR REPLACE TABLE encounters (
    encounter_id VARCHAR(50) PRIMARY KEY,
    patient_id VARCHAR(50) NOT NULL,
    encounter_date TIMESTAMP_NTZ NOT NULL,
    encounter_type VARCHAR(50),
    chief_complaint VARCHAR(500),
    diagnosis_code VARCHAR(20),
    diagnosis_description VARCHAR(500),
    provider_name VARCHAR(100),
    facility_name VARCHAR(100),
    admission_date TIMESTAMP_NTZ,
    discharge_date TIMESTAMP_NTZ,
    length_of_stay_days NUMBER(5,2),
    total_charges NUMBER(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(100) DEFAULT CURRENT_USER()
)
COMMENT = 'Clinical encounters and visits';

SELECT 'Step 2 Complete: encounters table created' AS status;

-------------------------------------------------------------------------------
-- STEP 3: Create Lab Results Table
-------------------------------------------------------------------------------

CREATE OR REPLACE TABLE lab_results (
    lab_result_id VARCHAR(50) PRIMARY KEY,
    encounter_id VARCHAR(50),
    patient_id VARCHAR(50) NOT NULL,
    test_name VARCHAR(200) NOT NULL,
    test_code VARCHAR(20),
    result_value VARCHAR(100),
    result_unit VARCHAR(50),
    reference_range VARCHAR(100),
    abnormal_flag VARCHAR(10),
    result_date TIMESTAMP_NTZ NOT NULL,
    performing_lab VARCHAR(100),
    ordering_provider VARCHAR(100),
    status VARCHAR(20),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(100) DEFAULT CURRENT_USER()
)
COMMENT = 'Laboratory test results';

SELECT 'Step 3 Complete: lab_results table created' AS status;

-------------------------------------------------------------------------------
-- STEP 4: Insert Sample Patient Data (1000 patients)
-------------------------------------------------------------------------------

USE SCHEMA patient_data;

INSERT INTO patients (
    patient_id, mrn, first_name, last_name, date_of_birth, 
    gender, ssn, address, city, state, zip_code, phone, email
)
SELECT
    'PAT-' || LPAD(SEQ4(), 6, '0') AS patient_id,
    'MRN' || LPAD(SEQ4(), 8, '0') AS mrn,
    CASE (UNIFORM(1, 10, RANDOM()) % 10)
        WHEN 0 THEN 'John' WHEN 1 THEN 'Jane' WHEN 2 THEN 'Michael'
        WHEN 3 THEN 'Sarah' WHEN 4 THEN 'David' WHEN 5 THEN 'Mary'
        WHEN 6 THEN 'Robert' WHEN 7 THEN 'Lisa' WHEN 8 THEN 'James'
        ELSE 'Jennifer'
    END AS first_name,
    CASE (UNIFORM(1, 10, RANDOM()) % 10)
        WHEN 0 THEN 'Smith' WHEN 1 THEN 'Johnson' WHEN 2 THEN 'Williams'
        WHEN 3 THEN 'Brown' WHEN 4 THEN 'Jones' WHEN 5 THEN 'Garcia'
        WHEN 6 THEN 'Miller' WHEN 7 THEN 'Davis' WHEN 8 THEN 'Rodriguez'
        ELSE 'Martinez'
    END AS last_name,
    DATEADD(day, -UNIFORM(18*365, 80*365, RANDOM()), CURRENT_DATE()) AS date_of_birth,
    CASE (UNIFORM(1, 2, RANDOM()) % 2) WHEN 0 THEN 'Male' ELSE 'Female' END AS gender,
    LPAD(UNIFORM(100000000, 999999999, RANDOM())::VARCHAR, 9, '0') AS ssn,
    UNIFORM(100, 9999, RANDOM()) || ' Main Street' AS address,
    CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN 'New York' WHEN 1 THEN 'Los Angeles' 
        WHEN 2 THEN 'Chicago' WHEN 3 THEN 'Houston' ELSE 'Boston'
    END AS city,
    CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN 'NY' WHEN 1 THEN 'CA' 
        WHEN 2 THEN 'IL' WHEN 3 THEN 'TX' ELSE 'MA'
    END AS state,
    LPAD(UNIFORM(10000, 99999, RANDOM())::VARCHAR, 5, '0') AS zip_code,
    '(' || LPAD(UNIFORM(200, 999, RANDOM())::VARCHAR, 3, '0') || ') ' ||
    LPAD(UNIFORM(100, 999, RANDOM())::VARCHAR, 3, '0') || '-' ||
    LPAD(UNIFORM(1000, 9999, RANDOM())::VARCHAR, 4, '0') AS phone,
    LOWER(first_name) || '.' || LOWER(last_name) || '@email.com' AS email
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

SELECT 'Step 4 Complete: ' || COUNT(*) || ' patient records inserted' AS status
FROM patients;

-------------------------------------------------------------------------------
-- STEP 5: Insert Sample Encounter Data
-------------------------------------------------------------------------------

USE SCHEMA clinical_data;

INSERT INTO encounters (
    encounter_id, patient_id, encounter_date, encounter_type,
    chief_complaint, diagnosis_code, diagnosis_description,
    provider_name, facility_name, status, total_charges
)
SELECT
    'ENC-' || LPAD(SEQ4(), 8, '0') AS encounter_id,
    'PAT-' || LPAD(UNIFORM(1, 1000, RANDOM()), 6, '0') AS patient_id,
    DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_TIMESTAMP()) AS encounter_date,
    CASE (UNIFORM(1, 4, RANDOM()) % 4)
        WHEN 0 THEN 'Emergency'
        WHEN 1 THEN 'Outpatient'
        WHEN 2 THEN 'Inpatient'
        ELSE 'Urgent Care'
    END AS encounter_type,
    CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN 'Chest pain'
        WHEN 1 THEN 'Shortness of breath'
        WHEN 2 THEN 'Fever and cough'
        WHEN 3 THEN 'Abdominal pain'
        ELSE 'Headache'
    END AS chief_complaint,
    CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN 'I10'
        WHEN 1 THEN 'E11.9'
        WHEN 2 THEN 'J44.0'
        WHEN 3 THEN 'I25.10'
        ELSE 'K21.9'
    END AS diagnosis_code,
    CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN 'Essential hypertension'
        WHEN 1 THEN 'Type 2 diabetes mellitus'
        WHEN 2 THEN 'COPD'
        WHEN 3 THEN 'Coronary artery disease'
        ELSE 'GERD'
    END AS diagnosis_description,
    'Dr. ' || CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN 'Anderson'
        WHEN 1 THEN 'Thompson'
        WHEN 2 THEN 'White'
        WHEN 3 THEN 'Harris'
        ELSE 'Martin'
    END AS provider_name,
    CASE (UNIFORM(1, 3, RANDOM()) % 3)
        WHEN 0 THEN 'City General Hospital'
        WHEN 1 THEN 'Memorial Medical Center'
        ELSE 'Community Health Clinic'
    END AS facility_name,
    'Completed' AS status,
    UNIFORM(500, 15000, RANDOM()) AS total_charges
FROM TABLE(GENERATOR(ROWCOUNT => 2500));

SELECT 'Step 5 Complete: ' || COUNT(*) || ' encounter records inserted' AS status
FROM encounters;

-------------------------------------------------------------------------------
-- STEP 6: Insert Sample Lab Results
-------------------------------------------------------------------------------

INSERT INTO lab_results (
    lab_result_id, encounter_id, patient_id, test_name, test_code,
    result_value, result_unit, reference_range, abnormal_flag,
    result_date, performing_lab, ordering_provider, status
)
SELECT
    'LAB-' || LPAD(SEQ4(), 10, '0') AS lab_result_id,
    'ENC-' || LPAD(UNIFORM(1, 2500, RANDOM()), 8, '0') AS encounter_id,
    'PAT-' || LPAD(UNIFORM(1, 1000, RANDOM()), 6, '0') AS patient_id,
    CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN 'Hemoglobin'
        WHEN 1 THEN 'White Blood Cell Count'
        WHEN 2 THEN 'Glucose'
        WHEN 3 THEN 'Creatinine'
        ELSE 'Cholesterol, Total'
    END AS test_name,
    CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN '718-7'
        WHEN 1 THEN '6690-2'
        WHEN 2 THEN '2345-7'
        WHEN 3 THEN '2160-0'
        ELSE '2093-3'
    END AS test_code,
    UNIFORM(10, 200, RANDOM())::VARCHAR AS result_value,
    CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN 'g/dL'
        WHEN 1 THEN 'K/uL'
        WHEN 2 THEN 'mg/dL'
        WHEN 3 THEN 'mg/dL'
        ELSE 'mg/dL'
    END AS result_unit,
    CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN '12-16 g/dL'
        WHEN 1 THEN '4.5-11.0 K/uL'
        WHEN 2 THEN '70-100 mg/dL'
        WHEN 3 THEN '0.7-1.3 mg/dL'
        ELSE '<200 mg/dL'
    END AS reference_range,
    CASE (UNIFORM(1, 4, RANDOM()) % 4)
        WHEN 0 THEN 'Normal'
        WHEN 1 THEN 'High'
        WHEN 2 THEN 'Low'
        ELSE 'Normal'
    END AS abnormal_flag,
    DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_TIMESTAMP()) AS result_date,
    CASE (UNIFORM(1, 3, RANDOM()) % 3)
        WHEN 0 THEN 'Quest Diagnostics'
        WHEN 1 THEN 'LabCorp'
        ELSE 'Hospital Lab'
    END AS performing_lab,
    'Dr. ' || CASE (UNIFORM(1, 5, RANDOM()) % 5)
        WHEN 0 THEN 'Anderson'
        WHEN 1 THEN 'Thompson'
        WHEN 2 THEN 'White'
        WHEN 3 THEN 'Harris'
        ELSE 'Martin'
    END AS ordering_provider,
    'Final' AS status
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

SELECT 'Step 6 Complete: ' || COUNT(*) || ' lab result records inserted' AS status
FROM lab_results;

-------------------------------------------------------------------------------
-- VERIFICATION QUERIES
-------------------------------------------------------------------------------

-- Verify patient data
SELECT 'Patients:' AS table_name, COUNT(*) AS record_count FROM patient_data.patients
UNION ALL
SELECT 'Encounters:' AS table_name, COUNT(*) AS record_count FROM clinical_data.encounters
UNION ALL
SELECT 'Lab Results:' AS table_name, COUNT(*) AS record_count FROM clinical_data.lab_results;

-- Sample patient record
SELECT * FROM patient_data.patients LIMIT 5;

-- Sample encounter record
SELECT * FROM clinical_data.encounters LIMIT 5;

-- Sample lab result
SELECT * FROM clinical_data.lab_results LIMIT 5;

-------------------------------------------------------------------------------
-- PHASE 2 SUMMARY
-------------------------------------------------------------------------------

SELECT '
╔════════════════════════════════════════════════════════════════╗
║                  PHASE 2 DATA LOAD COMPLETE                    ║
╠════════════════════════════════════════════════════════════════╣
║  ✓ 1,000 Patient Records
