SELECT 'Upgrading MetaStore schema from 2.1.1000 to 2.1.2000';

\i 043-HIVE-16997.postgres.sql;
\i 044-HIVE-16886.postgres.sql;

UPDATE "VERSION" SET "SCHEMA_VERSION"='2.1.2000', "VERSION_COMMENT"='Hive release version 2.1.2000' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.1000 to 2.1.2000';

