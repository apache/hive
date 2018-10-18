SELECT 'Upgrading MetaStore schema from 2.1.0 to 2.2.0' AS MESSAGE;

:r 022-HIVE-14496.mssql.sql
:r 023-HIVE-10562.mssql.sql
:r 024-HIVE-12274.mssql.sql

UPDATE VERSION SET SCHEMA_VERSION='2.2.0', VERSION_COMMENT='Hive release version 2.2.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.0 to 2.2.0' AS MESSAGE;
