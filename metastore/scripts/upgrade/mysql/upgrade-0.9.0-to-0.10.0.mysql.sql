SELECT 'Upgrading MetaStore schema from 0.9.0 to 0.10.0' AS ' ';
SOURCE 010-HIVE-3072.mysql.sql;
SOURCE 011-HIVE-3649.mysql.sql;
SOURCE 012-HIVE-1362.mysql.sql;
SELECT 'Finished upgrading MetaStore schema from 0.9.0 to 0.10.0' AS ' ';
