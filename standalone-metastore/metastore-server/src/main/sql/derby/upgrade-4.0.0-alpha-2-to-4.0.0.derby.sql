-- Upgrade MetaStore schema from 4.0.0-alpha-2 to 4.0.0

-- HIVE-26221
ALTER TABLE "APP"."TAB_COL_STATS" ADD HISTOGRAM BLOB;
ALTER TABLE "APP"."PART_COL_STATS" ADD HISTOGRAM BLOB;

-- This needs to be the last thing done.  Insert any changes above this line.
UPDATE "APP".VERSION SET SCHEMA_VERSION='4.0.0', VERSION_COMMENT='Hive release version 4.0.0' where VER_ID=1;