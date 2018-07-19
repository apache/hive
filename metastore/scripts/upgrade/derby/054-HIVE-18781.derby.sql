
CREATE TABLE REPL_TXN_MAP (
  RTM_REPL_POLICY varchar(256) NOT NULL,
  RTM_SRC_TXN_ID bigint NOT NULL,
  RTM_TARGET_TXN_ID bigint NOT NULL,
  PRIMARY KEY (RTM_REPL_POLICY, RTM_SRC_TXN_ID)
);

INSERT INTO "APP"."SEQUENCE_TABLE" ("SEQUENCE_NAME", "NEXT_VAL") SELECT * FROM (VALUES ('org.apache.hadoop.hive.metastore.model.MNotificationLog', 1)) tmp_table WHERE NOT EXISTS ( SELECT "NEXT_VAL" FROM "APP"."SEQUENCE_TABLE" WHERE "SEQUENCE_NAME" = 'org.apache.hadoop.hive.metastore.model.MNotificationLog');
