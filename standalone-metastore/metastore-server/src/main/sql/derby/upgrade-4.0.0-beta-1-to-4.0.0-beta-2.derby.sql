-- HIVE-27493
CREATE INDEX "APP"."PARTITION_KEY_VALS_IDX" ON "APP"."PARTITION_KEY_VALS"("PART_KEY_VAL");

-- This needs to be the last thing done.  Insert any changes above this line.
UPDATE "APP".VERSION SET SCHEMA_VERSION='4.0.0-beta-2', VERSION_COMMENT='Hive release version 4.0.0-beta-2' where VER_ID=1;
