SELECT 'Upgrading MetaStore schema from 0.12.0 to 0.13.0' AS ' ';

SOURCE 016-HIVE-6386.mysql.sql;
SOURCE 017-HIVE-6458.mysql.sql;
SOURCE 018-HIVE-6757.mysql.sql;
SOURCE hive-txn-schema-0.13.0.mysql.sql;

UPDATE VERSION SET SCHEMA_VERSION='0.13.0', VERSION_COMMENT='Hive release version 0.13.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 0.12.0 to 0.13.0' AS ' ';
