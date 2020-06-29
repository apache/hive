--Create table replication metrics
CREATE TABLE "REPLICATION_METRICS" (
  "RM_SCHEDULED_EXECUTION_ID" number PRIMARY KEY,
  "RM_POLICY" varchar2(256) NOT NULL,
  "RM_DUMP_EXECUTION_ID" number NOT NULL,
  "RM_METADATA" varchar2(4000),
  "RM_PROGRESS" varchar2(4000),
  "RM_START_TIME" integer NOT NULL
);

--Create indexes for the replication metrics table
CREATE INDEX POLICY_IDX ON "REPLICATION_METRICS" ("RM_POLICY");
CREATE INDEX DUMP_IDX ON "REPLICATION_METRICS" ("RM_DUMP_EXECUTION_ID");

-- These lines need to be last.  Insert any changes above.
UPDATE "APP".CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.1.0-Update1', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.1.0-Update1' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.1.1.0-Update1 to 3.1.3000.7.2.1.0-Update1' AS MESSAGE;
