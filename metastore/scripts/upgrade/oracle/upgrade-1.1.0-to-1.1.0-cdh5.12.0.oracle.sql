-- Upgrade MetaStore schema from 1.1.0 to 1.1.0-cdh5.12.0

@041-HIVE-16556.oracle.sql;

ALTER TABLE VERSION ADD SCHEMA_VERSION_V2 VARCHAR2(255);
UPDATE VERSION SET SCHEMA_VERSION='1.1.0', VERSION_COMMENT='Hive release version 1.1.0-cdh5.12.0', SCHEMA_VERSION_V2='1.1.0-cdh5.12.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 1.1.0 to 1.1.0-cdh5.12.0' AS Status from dual;
