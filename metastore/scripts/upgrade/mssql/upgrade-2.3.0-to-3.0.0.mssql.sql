SELECT 'Upgrading MetaStore schema from 2.3.0 to 3.0.0' AS MESSAGE;

:r 026-HIVE-16556.mssql.sql
:r 027-HIVE-16575.mssql.sql
:r 028-HIVE-16922.mssql.sql
:r 029-HIVE-16997.mssql.sql

UPDATE VERSION SET SCHEMA_VERSION='3.0.0', VERSION_COMMENT='Hive release version 3.0.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.3.0 to 3.0.0' AS MESSAGE;
