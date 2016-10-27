SELECT 'Upgrading MetaStore schema from 2.1.0 to 2.2.0' AS ' ';

SOURCE 037-HIVE-14637.mysql.sql;

UPDATE VERSION SET SCHEMA_VERSION='2.2.0', VERSION_COMMENT='Hive release version 2.2.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.0 to 2.2.0' AS ' ';

