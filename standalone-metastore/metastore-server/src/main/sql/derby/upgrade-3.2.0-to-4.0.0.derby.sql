-- Upgrade MetaStore schema from 3.2.0 to 4.0.0
-- HIVE-19416
ALTER TABLE "APP"."TBLS" ADD WRITE_ID bigint DEFAULT 0;
ALTER TABLE "APP"."PARTITIONS" ADD WRITE_ID bigint DEFAULT 0;

-- This needs to be the last thing done.  Insert any changes above this line.
UPDATE "APP".VERSION SET SCHEMA_VERSION='4.0.0', VERSION_COMMENT='Hive release version 4.0.0' where VER_ID=1;

