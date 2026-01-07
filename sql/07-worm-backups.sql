-- =====================================================
-- 07-worm-backups.sql
-- Write-Once-Read-Many (WORM) immutable backups for HIPAA/FDA compliance
-- 7-year retention with tamper-proof audit trails
-- =====================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLINICAL_DB;
USE WAREHOUSE CLINICAL_ETL_WH;

-- =====================================================
-- WORM BACKUP STORAGE INTEGRATION
-- =====================================================

-- Create external storage integration for WORM backups (AWS S3 Example)
CREATE STORAGE INTEGRATION IF NOT EXISTS CLINICAL_WORM_BACKUP_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-worm-backup-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://clinical-worm-backups/');

-- Retrieve IAM details for AWS trust policy configuration
DESC STORAGE INTEGRATION CLINICAL_WORM_BACKUP_INTEGRATION;

-- =====================================================
-- WORM BACKUP SCHEMA
-- =====================================================

CREATE SCHEMA IF NOT EXISTS BACKUP_WORM
  COMMENT = 'Immutable WORM backups for regulatory compliance (HIPAA/FDA 21 CFR Part 11)';

USE SCHEMA BACKUP_WORM;

-- =====================================================
-- EXTERNAL STAGE WITH WORM PROTECTION
-- =====================================================

CREATE STAGE IF NOT EXISTS WORM_BACKUP_STAGE
  STORAGE_INTEGRATION = CLINICAL_WORM_BACKUP_INTEGRATION
  URL = 's3://clinical-worm-backups/daily-snapshots/'
  FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = SNAPPY
  )
  COMMENT = 'WORM-protected external stage for immutable backups';

-- =====================================================
-- IMMUTABLE BACKUP TABLES (Read-Only After Creation)
-- =====================================================

-- Patients Backup Table with WORM Protection
CREATE TABLE IF NOT EXISTS PATIENTS_BACKUP_WORM (
    BACKUP_ID STRING DEFAULT UUID_STRING(),
    BACKUP_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    BACKUP_USER STRING DEFAULT CURRENT_USER(),
    RETENTION_UNTIL DATE DEFAULT DATEADD('YEAR', 7, CURRENT_DATE()),
    PATIENT_ID STRING
