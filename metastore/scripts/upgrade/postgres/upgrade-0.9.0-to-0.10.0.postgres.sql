SELECT 'Upgrading MetaStore schema from 0.9.0 to 0.10.0';
\i 010-HIVE-3072.postgres.sql;
\i 011-HIVE-3649.postgres.sql;
\i 012-HIVE-1362.postgres.sql;
SELECT 'Finished upgrading MetaStore schema from 0.9.0 to 0.10.0';
