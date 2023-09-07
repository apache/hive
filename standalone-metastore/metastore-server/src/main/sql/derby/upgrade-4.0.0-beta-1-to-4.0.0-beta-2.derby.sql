-- HIVE-27499
CREATE UNIQUE INDEX "APP"."NOTIFICATION_LOG_UNIQUE_DB" ON "APP"."NOTIFICATION_LOG" ("DB_NAME", "EVENT_ID");

-- This needs to be the last thing done.  Insert any changes above this line.
UPDATE "APP".VERSION SET SCHEMA_VERSION='4.0.0-beta-2', VERSION_COMMENT='Hive release version 4.0.0-beta-2' where VER_ID=1;
