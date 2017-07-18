SELECT 'Upgrading MetaStore schema from 2.3.0 to 3.0.0';

\i 040-HIVE-16556.postgres.sql;
\i 041-HIVE-16575.postgres.sql;
\i 042-HIVE-16922.postgres.sql;

UPDATE "VERSION" SET "SCHEMA_VERSION"='3.0.0', "VERSION_COMMENT"='Hive release version 3.0.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 2.3.0 to 3.0.0';

