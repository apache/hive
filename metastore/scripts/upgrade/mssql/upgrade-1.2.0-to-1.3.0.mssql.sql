SELECT 'Upgrading MetaStore schema from 1.2.0 to 1.3.0' AS MESSAGE;

:r 007-HIVE-11970.mssql.sql
:r 008-HIVE-12807.mssql.sql
:r 009-HIVE-12814.mssql.sql
:r 010-HIVE-12816.mssql.sql
:r 011-HIVE-12818.mssql.sql
:r 012-HIVE-12819.mssql.sql
:r 013-HIVE-12821.mssql.sql
:r 014-HIVE-12822.mssql.sql
:r 015-HIVE-12823.mssql.sql
:r 016-HIVE-12831.mssql.sql
:r 017-HIVE-12832.mssql.sql
:r 020-HIVE-13395.mssql.sql
:r 021-HIVE-13354.mssql.sql

UPDATE VERSION SET SCHEMA_VERSION='1.3.0', VERSION_COMMENT='Hive release version 1.3.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 1.2.0 to 1.3.0' AS MESSAGE;
