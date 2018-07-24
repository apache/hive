SELECT 'Upgrading MetaStore schema from 3.1.0 to 3.2.0' AS Status from dual;

-- HIVE-19267
CREATE TABLE TXN_WRITE_NOTIFICATION_LOG (
  WNL_ID number(19) NOT NULL,
  WNL_TXNID number(19) NOT NULL,
  WNL_WRITEID number(19) NOT NULL,
  WNL_DATABASE varchar(128) NOT NULL,
  WNL_TABLE varchar(128) NOT NULL,
  WNL_PARTITION varchar(767) NOT NULL,
  WNL_TABLE_OBJ clob NOT NULL,
  WNL_PARTITION_OBJ clob,
  WNL_FILES clob,
  WNL_EVENT_TIME number(10) NOT NULL,
  PRIMARY KEY (WNL_TXNID, WNL_DATABASE, WNL_TABLE, WNL_PARTITION)
);
INSERT INTO SEQUENCE_TABLE (SEQUENCE_NAME, NEXT_VAL) VALUES ('org.apache.hadoop.hive.metastore.model.MTxnWriteNotificationLog', 1);

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='3.2.0', VERSION_COMMENT='Hive release version 3.2.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.0 to 3.2.0' AS Status from dual;
