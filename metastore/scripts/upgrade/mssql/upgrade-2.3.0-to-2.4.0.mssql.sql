SELECT 'Upgrading MetaStore schema from 2.3.0 to 2.4.0' AS MESSAGE;

:r 026-HIVE-19372.mssql.sql

UPDATE VERSION SET SCHEMA_VERSION='2.4.0', VERSION_COMMENT='Hive release version 2.4.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.3.0 to 2.4.0' AS MESSAGE;
