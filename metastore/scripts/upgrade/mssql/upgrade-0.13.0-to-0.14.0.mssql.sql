SELECT 'Upgrading MetaStore schema from 0.13.0 to 0.14.0' AS MESSAGE;

:r 002-HIVE-7784.mssql.sql
:r 003-HIVE-8239.mssql.sql
:r 004-HIVE-8550.mssql.sql

UPDATE VERSION SET SCHEMA_VERSION='0.14.0', VERSION_COMMENT='Hive release version 0.14.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 0.13.0 to 0.14.0' AS MESSAGE;
