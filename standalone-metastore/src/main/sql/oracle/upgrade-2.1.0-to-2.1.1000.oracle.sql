SELECT 'Upgrading MetaStore schema from 2.1.0 to 2.1.1000' AS Status from dual;

-- @038-HIVE-10562.oracle.sql;
ALTER TABLE NOTIFICATION_LOG ADD MESSAGE_FORMAT VARCHAR(16) NULL;

UPDATE VERSION SET SCHEMA_VERSION='2.1.1000', VERSION_COMMENT='Hive release version 2.1.1000' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.0 to 2.1.1000' AS Status from dual;
