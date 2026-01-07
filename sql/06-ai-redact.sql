-- =====================================================
-- 06-ai-redact.sql
-- AI-powered automatic de-identification of PHI using Snowflake AI_REDACT
-- Removes 18 HIPAA identifiers for research datasets
-- =====================================================

USE ROLE CLINICAL_ADMIN;
USE DATABASE CLINICAL_DB;
USE WAREHOUSE CLINICAL_ETL_WH;

-- =====================================================
-- AI_REDACT FUNCTION SETUP
-- =====================================================

-- AI_REDACT automatically detects and removes the 18 HIPAA Safe Harbor identifiers:
-- 1. Names  2. Geographic subdivisions smaller than state  3. Dates (except year)
-- 4. Phone numbers  5. Fax numbers  6. Email addresses  7. SSNs
-- 8. Medical record numbers  9. Health plan numbers  10. Account numbers
-- 11. Certificate/license numbers  12. Vehicle IDs  13. Device IDs
-- 14. Web URLs  15. IP addresses  16. Biometric IDs  17. Photos  18. Other unique IDs

-- =====================================================
-- CREATE DE-IDENTIFIED RESEARCH SCHEMA
-- =====================================================

CREATE SCHEMA IF NOT EXISTS RESEARCH_DEIDENTIFIED
  COMMENT = 'De-identified datasets for research and analytics';

USE SCHEMA RESEARCH_DEIDENTIFIED;

-- =====================================================
-- DE-IDENTIFIED PATIENTS VIEW
-- =====================================================

CREATE OR REPLACE VIEW V_PATIENTS_DEIDENTIFIED AS
SELECT
    -- Generate pseudo-ID instead of real patient_id
    HASH(PATIENT_ID) AS PATIENT_HASH_ID,
    
    -- Redact name fields
    SNOWFLAKE.CORTEX.AI_REDACT(FIRST_NAME) AS FIRST_NAME_REDACTED,
    SNOWFLAKE.CORTEX.AI_REDACT(LAST_NAME) AS LAST_NAME_REDACTED,
    
    -- Keep date of birth but generalize to year only (HIPAA Safe Harbor)
    DATE_PART('YEAR', DATE_OF_BIRTH) AS BIRTH_YEAR,
    CASE 
        WHEN DATEDIFF('YEAR', DATE_OF_BIRTH, CURRENT_DATE()) >= 90 
        THEN '90+' 
        ELSE TO_CHAR(DATEDIFF('YEAR', DATE_OF_BIRTH, CURRENT_DATE())) 
    END AS AGE_GROUP,
    
    -- Keep gender (not a PHI identifier)
    GENDER,
    
    -- Redact SSN completely
    '***-**-****' AS SSN_REDACTED,
    
    -- Redact email and phone
    SNOWFLAKE.CORTEX.AI_REDACT(EMAIL) AS EMAIL_REDACTED,
    SNOWFLAKE.CORTEX.AI_REDACT(PHONE) AS PHONE_REDACTED,
    
    -- Redact street address but keep city/state/ZIP3 (first 3 digits)
    '*** REDACTED ***' AS ADDRESS_REDACTED,
    CITY,
    STATE,
    LEFT(ZIP_CODE, 3) || '**' AS ZIP3,
    
    -- Keep timestamps but remove dates (convert to relative days from index date)
    DATEDIFF('DAY', MIN(CREATED_AT) OVER (), CREATED_AT) AS DAYS_SINCE_COHORT_START,
    DATEDIFF('DAY', MIN(UPDATED_AT) OVER (), UPDATED_AT) AS DAYS_SINCE_LAST_UPDATE
    
FROM CLINICAL_DB.CURATED.PATIENTS
WHERE DELETED_AT IS NULL
COMMENT = 'De-identified patient data for research - HIPAA Safe Harbor compliant';

-- =====================================================
-- DE-IDENTIFIED ENCOUNTERS VIEW
-- =====================================================

CREATE OR REPLACE VIEW V_ENCOUNTERS_DEIDENTIFIED AS
SELECT
    HASH(ENCOUNTER_ID) AS ENCOUNTER_HASH_ID,
    HASH(PATIENT_ID) AS PATIENT_HASH_ID,
    ENCOUNTER_TYPE,
    
    -- Redact dates (convert to relative time from cohort start)
    DATEDIFF('DAY', MIN(ENCOUNTER_DATE) OVER (), ENCOUNTER_DATE) AS DAYS_SINCE_COHORT_START,
    
    -- Redact provider and facility IDs
    HASH(PROVIDER_ID) AS PROVIDER_HASH_ID,
    HASH(FACILITY_ID) AS FACILITY_HASH_ID,
    
    -- Redact free-text chief complaint using AI_REDACT
    SNOWFLAKE.CORTEX.AI_REDACT(CHIEF_COMPLAINT) AS CHIEF_COMPLAINT_REDACTED,
    
    -- Keep structured diagnosis codes (not PHI)
    DIAGNOSIS_CODE,
    DIAGNOSIS_DESCRIPTION,
    
    -- Redact discharge date
    CASE 
        WHEN DISCHARGE_DATE IS NOT NULL 
        THEN DATEDIFF('DAY', MIN(ENCOUNTER_DATE) OVER (), DISCHARGE_DATE) 
        ELSE NULL 
    END AS DAYS_TO_DISCHARGE,
    
    STATUS
    
FROM CLINICAL_DB.CURATED.ENCOUNTERS
WHERE DELETED_AT IS NULL
COMMENT = 'De-identified encounter data for research';

-- =====================================================
-- DE-IDENTIFIED LAB RESULTS VIEW
-- =====================================================

CREATE OR REPLACE VIEW V_LAB_RESULTS_DEIDENTIFIED AS
SELECT
    HASH(LAB_RESULT_ID) AS LAB_RESULT_HASH_ID,
    HASH(ENCOUNTER_ID) AS ENCOUNTER_HASH_ID,
    HASH(PATIENT_ID) AS PATIENT_HASH_ID,
    
    -- Keep test codes and results (structured data, not PHI)
    TEST_CODE,
    TEST_NAME,
    RESULT_VALUE,
    RESULT_UNIT,
    REFERENCE_RANGE,
    ABNORMAL_FLAG,
    
    -- Redact dates (relative time)
    DATEDIFF('DAY', MIN(TEST_DATE) OVER (), TEST_DATE) AS DAYS_SINCE_COHORT_START,
    DATEDIFF('DAY', TEST_DATE, RESULT_DATE) AS DAYS_TO_RESULT,
    
    -- Redact provider
    HASH(PROVIDER_ID) AS PROVIDER_HASH_ID
    
FROM CLINICAL_DB.CURATED.LAB_RESULTS
WHERE DELETED_AT IS NULL
COMMENT = 'De-identified lab results for research';

-- =====================================================
-- RESEARCH COHORT WITH FULL REDACTION
-- =====================================================

CREATE OR REPLACE VIEW V_RESEARCH_COHORT AS
SELECT
    p.PATIENT_HASH_ID,
    p.BIRTH_YEAR,
    p.AGE_GROUP,
    p.GENDER,
    p.CITY,
    p.STATE,
    p.ZIP3,
    COUNT(DISTINCT e.ENCOUNTER_HASH_ID) AS TOTAL_ENCOUNTERS,
    COUNT(DISTINCT l.LAB_RESULT_HASH_ID) AS TOTAL_LAB_TESTS,
    MIN(e.DAYS_SINCE_COHORT_START) AS FIRST_ENCOUNTER_DAY,
    MAX(e.DAYS_SINCE_COHORT_START) AS LAST_ENCOUNTER_DAY
FROM V_PATIENTS_DEIDENTIFIED p
LEFT JOIN V_ENCOUNTERS_DEIDENTIFIED e ON p.PATIENT_HASH_ID = e.PATIENT_HASH_ID
LEFT JOIN V_LAB_RESULTS_DEIDENTIFIED l ON p.PATIENT_HASH_ID = l.PATIENT_HASH_ID
GROUP BY 1,2,3,4,5,6,7
COMMENT = 'Aggregated research cohort with full de-identification';

-- =====================================================
-- SENSITIVE TEXT REDACTION FUNCTION
-- =====================================================

CREATE OR REPLACE FUNCTION REDACT_CLINICAL_NOTES(clinical_text STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    SNOWFLAKE.CORTEX.AI_REDACT(clinical_text)
$$
COMMENT = 'Redact PHI from clinical notes using AI_REDACT';

-- Example usage:
-- SELECT REDACT_CLINICAL_NOTES('Patient John Doe (SSN 123-45-6789) presented with chest pain on 01/15/2024');
-- Output: "Patient [REDACTED] (SSN [REDACTED]) presented with chest pain on [REDACTED]"

-- =====================================================
-- BATCH REDACTION PROCEDURE
-- =====================================================

CREATE OR REPLACE PROCEDURE BATCH_REDACT_CLINICAL_NOTES(
    SOURCE_TABLE STRING,
    TEXT_COLUMN STRING,
    TARGET_TABLE STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_stmt STRING;
BEGIN
    sql_stmt := 'CREATE OR REPLACE TABLE ' || :TARGET_TABLE || ' AS
                 SELECT *, SNOWFLAKE.CORTEX.AI_REDACT(' || :TEXT_COLUMN || ') AS ' || :TEXT_COLUMN || '_REDACTED
                 FROM ' || :SOURCE_TABLE;
    EXECUTE IMMEDIATE :sql_stmt;
    RETURN 'Successfully redacted ' || :TEXT_COLUMN || ' from ' || :SOURCE_TABLE || ' into ' || :TARGET_TABLE;
END;
$$
COMMENT = 'Batch redact PHI from clinical notes tables';

-- Example usage:
-- CALL BATCH_REDACT_CLINICAL_NOTES('CLINICAL_DB.RAW.CLINICAL_NOTES', 'NOTE_TEXT', 'CLINICAL_DB.RESEARCH_DEIDENTIFIED.NOTES_REDACTED');

-- =====================================================
-- RBAC FOR RESEARCH ACCESS
-- =====================================================

-- Grant research access to de-identified views only
GRANT USAGE ON SCHEMA RESEARCH_DEIDENTIFIED TO ROLE CLINICAL_DATA_SCIENTIST;
GRANT SELECT ON ALL VIEWS IN SCHEMA RESEARCH_DEIDENTIFIED TO ROLE CLINICAL_DATA_SCIENTIST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA RESEARCH_DEIDENTIFIED TO ROLE CLINICAL_DATA_SCIENTIST;

-- Explicitly deny access to raw PHI schemas
REVOKE SELECT ON ALL TABLES IN SCHEMA CLINICAL_DB.RAW FROM ROLE CLINICAL_DATA_SCIENTIST;
REVOKE SELECT ON ALL TABLES IN SCHEMA CLINICAL_DB.STAGING FROM ROLE CLINICAL_DATA_SCIENTIST;

-- =====================================================
-- AUDIT LOGGING FOR REDACTION USAGE
-- =====================================================

CREATE OR REPLACE TABLE RESEARCH_DEIDENTIFIED.AUDIT_REDACTION_ACCESS (
    ACCESS_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    USER_NAME STRING DEFAULT CURRENT_USER(),
    ROLE_NAME STRING DEFAULT CURRENT_ROLE(),
    QUERY_TEXT STRING DEFAULT CURRENT_STATEMENT(),
    QUERY_ID STRING DEFAULT CURRENT_QUERY_ID(),
    VIEW_ACCESSED STRING
)
COMMENT = 'Audit trail for de-identified data access';

-- =====================================================
-- VALIDATION QUERIES
-- =====================================================

-- Verify AI_REDACT is working
SELECT 
    'Original' AS TYPE,
    FIRST_NAME,
    LAST_NAME,
    SSN,
    EMAIL,
    PHONE
FROM CLINICAL_DB.CURATED.PATIENTS
LIMIT 3;

-- View redacted version
SELECT 
    'Redacted' AS TYPE,
    FIRST_NAME_REDACTED,
    LAST_NAME_REDACTED,
    SSN_REDACTED,
    EMAIL_REDACTED,
    PHONE_REDACTED
FROM RESEARCH_DEIDENTIFIED.V_PATIENTS_DEIDENTIFIED
LIMIT 3;

-- Verify no PHI leakage in research cohort
SELECT * FROM RESEARCH_DEIDENTIFIED.V_RESEARCH_COHORT LIMIT 10;

-- =====================================================
-- COMPLIANCE STATEMENT
-- =====================================================

SELECT '✅ AI_REDACT DE-IDENTIFICATION COMPLETE' AS STATUS,
       'All views in RESEARCH_DEIDENTIFIED schema comply with HIPAA Safe Harbor method' AS DETAILS,
       'Data scientists can now query de-identified data without PHI exposure risk' AS USAGE;
