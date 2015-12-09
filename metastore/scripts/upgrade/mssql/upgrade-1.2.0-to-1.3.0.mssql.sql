SELECT 'Upgrading MetaStore schema from 1.2.0 to 1.3.0' AS MESSAGE;

:r 007-HIVE-11970.mssql.sql;

UPDATE VERSION SET SCHEMA_VERSION='1.3.0', VERSION_COMMENT='Hive release version 1.3.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 1.2.0 to 1.3.0' AS MESSAGE;
