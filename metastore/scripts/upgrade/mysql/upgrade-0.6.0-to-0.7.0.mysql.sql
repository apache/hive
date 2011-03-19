SELECT 'Upgrading MetaStore schema from 0.6.0 to 0.7.0' AS ' ';
SOURCE 005-HIVE-417.mysql.sql;
SOURCE 006-HIVE-1823.mysql.sql;
SOURCE 007-HIVE-78.mysql.sql;
SELECT 'Finished upgrading MetaStore schema from 0.6.0 to 0.7.0' AS ' ';
