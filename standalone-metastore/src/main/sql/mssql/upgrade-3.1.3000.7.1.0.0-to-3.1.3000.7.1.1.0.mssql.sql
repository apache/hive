
SELECT 'Upgrading MetaStore schema from 3.1.3000.7.1.0.0 to 3.1.3000.7.1.1.0' AS MESSAGE;


ALTER TABLE "SCHEDULED_QUERIES" ADD COLUMN "ACTIVE_EXECUTION_ID" bigint;

-- HIVE-22995
ALTER TABLE DBS ADD DB_MANAGED_LOCATION_URI nvarchar(4000);

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.1.1.0', VERSION_COMMENT='Hive release version 3.1.3000.7.1.1.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.1.0.0 to 3.1.3000.7.1.1.0' AS MESSAGE;
