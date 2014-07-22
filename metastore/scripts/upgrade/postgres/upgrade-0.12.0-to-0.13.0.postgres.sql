SELECT 'Upgrading MetaStore schema from 0.12.0 to 0.13.0';

\i 015-HIVE-5700.postgres.sql;
\i 016-HIVE-6386.postgres.sql;
\i 017-HIVE-6458.postgres.sql;
\i 018-HIVE-6757.postgres.sql;
\i hive-txn-schema-0.13.0.postgres.sql;

UPDATE "VERSION" SET "SCHEMA_VERSION"='0.13.0', "VERSION_COMMENT"='Hive release version 0.13.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 0.12.0 to 0.13.0';


