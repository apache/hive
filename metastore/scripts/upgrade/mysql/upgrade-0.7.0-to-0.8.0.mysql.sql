SELECT 'Upgrading MetaStore schema from 0.7.0 to 0.8.0' AS ' ';
SOURCE 008-HIVE-2246.mysql.sql;
SOURCE 009-HIVE-2215.mysql.sql;
SELECT 'Finished upgrading MetaStore schema from 0.7.0 to 0.8.0' AS ' ';
