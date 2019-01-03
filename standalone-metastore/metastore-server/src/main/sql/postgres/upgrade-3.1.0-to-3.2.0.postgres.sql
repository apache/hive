SELECT 'Upgrading MetaStore schema from 3.1.0 to 3.2.0';

-- HIVE-19267
CREATE TABLE "TXN_WRITE_NOTIFICATION_LOG" (
  "WNL_ID" bigint NOT NULL,
  "WNL_TXNID" bigint NOT NULL,
  "WNL_WRITEID" bigint NOT NULL,
  "WNL_DATABASE" varchar(128) NOT NULL,
  "WNL_TABLE" varchar(128) NOT NULL,
  "WNL_PARTITION" varchar(767) NOT NULL,
  "WNL_TABLE_OBJ" text NOT NULL,
  "WNL_PARTITION_OBJ" text,
  "WNL_FILES" text,
  "WNL_EVENT_TIME" integer NOT NULL,
  PRIMARY KEY ("WNL_TXNID", "WNL_DATABASE", "WNL_TABLE", "WNL_PARTITION")
);
INSERT INTO "SEQUENCE_TABLE" ("SEQUENCE_NAME", "NEXT_VAL") VALUES ('org.apache.hadoop.hive.metastore.model.MTxnWriteNotificationLog', 1);

-- HIVE-20221
alter table "PARTITION_PARAMS" alter column "PARAM_VALUE" type text using cast("PARAM_VALUE" as text);

-- HIVE-21077
ALTER TABLE "DBS" ADD "CREATE_TIME" BIGINT;
ALTER TABLE "CTLGS" ADD "CREATE_TIME" BIGINT;

-- These lines need to be last.  Insert any changes above.
UPDATE "VERSION" SET "SCHEMA_VERSION"='3.2.0', "VERSION_COMMENT"='Hive release version 3.2.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.0 to 3.2.0';

