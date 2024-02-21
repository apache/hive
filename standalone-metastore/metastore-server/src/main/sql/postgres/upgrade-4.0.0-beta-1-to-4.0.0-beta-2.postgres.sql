SELECT 'Upgrading MetaStore schema from 4.0.0-beta-1 to 4.0.0-beta-2';

-- HIVE-24815: Remove "IDXS" Table from Metastore Schema
DROP TABLE IF EXISTS "INDEX_PARAMS";
DROP TABLE IF EXISTS "IDXS";

-- HIVE-27827
ALTER TABLE ONLY "PARTITIONS" DROP CONSTRAINT "UNIQUEPARTITION";
ALTER TABLE ONLY "PARTITIONS" ADD CONSTRAINT "UNIQUEPARTITION" UNIQUE ("TBL_ID", "PART_NAME");
DROP INDEX "PARTITIONS_N49";

-- These lines need to be last. Insert any changes above.
UPDATE "VERSION" SET "SCHEMA_VERSION"='4.0.0-beta-2', "VERSION_COMMENT"='Hive release version 4.0.0-beta-2' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 4.0.0-beta-1 to 4.0.0-beta-2';
