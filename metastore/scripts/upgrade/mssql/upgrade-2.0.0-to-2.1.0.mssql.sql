SELECT 'Upgrading MetaStore schema from 2.0.0 to 2.1.0' AS MESSAGE;

:r 019-HIVE-13076.mssql.sql
:r 020-HIVE-13395.mssql.sql
:r 021-HIVE-13354.mssql.sql

UPDATE VERSION SET SCHEMA_VERSION='2.1.0', VERSION_COMMENT='Hive release version 2.1.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.0.0 to 2.1.0' AS MESSAGE;
