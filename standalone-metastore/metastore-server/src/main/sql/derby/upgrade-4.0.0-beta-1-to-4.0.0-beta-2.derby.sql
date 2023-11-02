-- HIVE-24815 Remove "IDXS" Table from Metastore Schema
DROP TABLE "APP"."INDEX_PARAMS";
DROP TABLE "APP"."IDXS";

-- This needs to be the last thing done.  Insert any changes above this line.
UPDATE "APP".VERSION SET SCHEMA_VERSION='4.0.0-beta-2', VERSION_COMMENT='Hive release version 4.0.0-beta-2' where VER_ID=1;
