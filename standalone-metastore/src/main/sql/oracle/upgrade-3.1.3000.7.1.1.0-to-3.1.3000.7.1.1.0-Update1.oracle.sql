-- HIVE-22995
ALTER TABLE DBS ADD DB_MANAGED_LOCATION_URL VARCHAR2(4000) NULL;

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.1.1.0-Update1', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.1.1.0-Update1' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.1.1.0 to 3.1.3000.7.1.1.0-Update1' AS Status from dual;
