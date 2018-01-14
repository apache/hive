SELECT 'Upgrading MetaStore schema from 2.3.0 to 3.0.0';

\i 040-HIVE-16556.postgres.sql;
\i 041-HIVE-16575.postgres.sql;
\i 042-HIVE-16922.postgres.sql;
\i 043-HIVE-16997.postgres.sql;
\i 044-HIVE-16886.postgres.sql;
\i 045-HIVE-17566.postgres.sql;
\i 046-HIVE-18202.postgres.sql;
\i 047-HIVE-14498.postgres.sql;

UPDATE "VERSION" SET "SCHEMA_VERSION"='3.0.0', "VERSION_COMMENT"='Hive release version 3.0.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 2.3.0 to 3.0.0';

