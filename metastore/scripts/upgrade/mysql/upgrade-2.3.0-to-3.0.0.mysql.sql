SELECT 'Upgrading MetaStore schema from 2.3.0 to 3.0.0' AS ' ';

SOURCE 041-HIVE-16556.mysql.sql;
SOURCE 042-HIVE-16575.mysql.sql;
SOURCE 043-HIVE-16922.mysql.sql;
SOURCE 044-HIVE-16997.mysql.sql;

UPDATE VERSION SET SCHEMA_VERSION='3.0.0', VERSION_COMMENT='Hive release version 3.0.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.3.0 to 3.0.0' AS ' ';

