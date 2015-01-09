SELECT 'Upgrading MetaStore schema from 0.14.0 to 0.15.0' AS ' ';

SOURCE 020-HIVE-9296.mysql.sql;

UPDATE VERSION SET SCHEMA_VERSION='0.14.0', VERSION_COMMENT='Hive release version 0.15.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 0.14.0 to 0.15.0' AS ' ';

