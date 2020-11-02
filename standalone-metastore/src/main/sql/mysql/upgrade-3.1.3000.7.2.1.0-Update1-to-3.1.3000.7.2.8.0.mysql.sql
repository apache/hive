-- HIVE-23107
ALTER TABLE COMPACTION_QUEUE ADD CQ_NEXT_TXN_ID bigint;

-- HIVE-24291
ALTER TABLE COMPACTION_QUEUE ADD CQ_TXN_ID bigint;

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.8.0', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.8.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.1.0-Update1 to 3.1.3000.7.2.8.0';