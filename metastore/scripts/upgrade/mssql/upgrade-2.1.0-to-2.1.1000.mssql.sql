SELECT 'Upgrading MetaStore schema from 2.1.0 to 2.1.1000' AS MESSAGE;

:r 023-HIVE-10562.mssql.sql

UPDATE VERSION SET SCHEMA_VERSION='2.1.1000', VERSION_COMMENT='Hive release version 2.1.1000' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.0 to 2.1.1000' AS MESSAGE;
