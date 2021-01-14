SELECT 'Upgrading MetaStore schema from 2.0.0 to 2.1.0' AS ' ';

SOURCE 034-HIVE-13076.mysql.sql;
SOURCE 035-HIVE-13395.mysql.sql;
SOURCE 036-HIVE-13354.mysql.sql;
SOURCE 050-HIVE-23211.mysql.sql

UPDATE VERSION SET SCHEMA_VERSION='2.1.0', VERSION_COMMENT='Hive release version 2.1.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.0.0 to 2.1.0' AS ' ';

