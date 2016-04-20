SELECT 'Upgrading MetaStore schema from 2.0.0 to 2.1.0' AS ' ';

SOURCE 033-HIVE-12892.mysql.sql;
SOURCE 034-HIVE-13076.mysql.sql;

UPDATE VERSION SET SCHEMA_VERSION='2.1.0', VERSION_COMMENT='Hive release version 2.1.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.0.0 to 2.1.0' AS ' ';

