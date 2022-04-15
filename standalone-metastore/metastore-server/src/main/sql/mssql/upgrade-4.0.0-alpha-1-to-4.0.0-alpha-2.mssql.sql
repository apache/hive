SELECT 'Upgrading MetaStore schema from  4.0.0-alpha-1 to 4.0.0-alpha-2' AS MESSAGE;

-- HIVE-26144
ALTER TABLE TXN_COMPONENTS ADD TC_ID bigint NOT NULL IDENTITY(1,1) PRIMARY KEY;
ALTER TABLE COMPLETED_TXN_COMPONENTS ADD CTC_ID bigint NOT NULL IDENTITY(1,1) PRIMARY KEY;
CREATE INDEX IND_WNL_ID ON TXN_WRITE_NOTIFICATION_LOG(WNL_ID);

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='4.0.0-alpha-2', VERSION_COMMENT='Hive release version 4.0.0-alpha-2' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 4.0.0-alpha-1 to 4.0.0-alpha-2' AS MESSAGE;
