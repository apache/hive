SELECT 'Upgrading MetaStore schema from 4.0.0-alpha-2 to 4.0.0';

-- HIVE-26221
ALTER TABLE "TAB_COL_STATS" ADD "HISTOGRAM" bytea;
ALTER TABLE "PART_COL_STATS" ADD "HISTOGRAM" bytea;

-- HIVE-26719
ALTER TABLE "COMPACTION_QUEUE" ADD "CQ_NUMBER_OF_BUCKETS" INTEGER;
ALTER TABLE "COMPLETED_COMPACTIONS" ADD "CQ_NUMBER_OF_BUCKETS" INTEGER;

-- These lines need to be last. Insert any changes above.
UPDATE "VERSION" SET "SCHEMA_VERSION"='4.0.0', "VERSION_COMMENT"='Hive release version 4.0.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 4.0.0-alpha-2 to 4.0.0';
