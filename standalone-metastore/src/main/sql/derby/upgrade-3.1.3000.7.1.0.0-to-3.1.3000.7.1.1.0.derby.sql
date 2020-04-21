-- HIVE-22872
ALTER TABLE "SCHEDULED_QUERIES" ADD "ACTIVE_EXECUTION_ID" bigint;

-- These lines need to be last.  Insert any changes above.
UPDATE "APP".CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.1.1.0', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.1.1.0' where VER_ID=1;
