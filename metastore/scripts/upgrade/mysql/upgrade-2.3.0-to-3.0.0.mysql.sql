SELECT 'Upgrading MetaStore schema from 2.3.0 to 3.0.0' AS ' ';

SOURCE 041-HIVE-16556.mysql.sql;
SOURCE 042-HIVE-16575.mysql.sql;
SOURCE 043-HIVE-16922.mysql.sql;
SOURCE 044-HIVE-16997.mysql.sql;
SOURCE 045-HIVE-16886.mysql.sql;
SOURCE 046-HIVE-17566.mysql.sql;
SOURCE 047-HIVE-18202.mysql.sql;
SOURCE 048-HIVE-14498.mysql.sql;

UPDATE VERSION SET SCHEMA_VERSION='3.0.0', VERSION_COMMENT='Hive release version 3.0.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.3.0 to 3.0.0' AS ' ';
