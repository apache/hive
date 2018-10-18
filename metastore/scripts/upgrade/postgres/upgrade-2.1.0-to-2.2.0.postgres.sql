SELECT 'Upgrading MetaStore schema from 2.1.0 to 2.2.0';

\i 036-HIVE-14496.postgres.sql;
\i 037-HIVE-10562.postgres.sql;
\i 038-HIVE-12274.postgres.sql;

UPDATE "VERSION" SET "SCHEMA_VERSION"='2.2.0', "VERSION_COMMENT"='Hive release version 2.2.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.0 to 2.2.0';

