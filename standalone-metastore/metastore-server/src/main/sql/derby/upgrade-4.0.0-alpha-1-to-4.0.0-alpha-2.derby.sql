-- Upgrade MetaStore schema from 4.0.0-alpha-1 to 4.0.0-alpha-2

-- HIVE-26280
ALTER TABLE COMPLETED_COMPACTIONS ADD CC_NEXT_TXN_ID bigint;
ALTER TABLE COMPLETED_COMPACTIONS ADD CC_TXN_ID bigint;
ALTER TABLE COMPLETED_COMPACTIONS ADD CC_COMMIT_TIME bigint;

-- This needs to be the last thing done.  Insert any changes above this line.
UPDATE "APP".VERSION SET SCHEMA_VERSION='4.0.0-alpha-2', VERSION_COMMENT='Hive release version 4.0.0-alpha-2' where VER_ID=1;
