SELECT '< Upgrading MetaStore schema from 0.5.0 to 0.6.0 >' AS ' ';
SOURCE 001-HIVE-972.mysql.sql;
SOURCE 002-HIVE-1068.mysql.sql;
SOURCE 003-HIVE-675.mysql.sql;
SOURCE 004-HIVE-1364.mysql.sql;
SELECT '< Finished upgrading MetaStore schema from 0.5.0 to 0.6.0 >' AS ' ';
