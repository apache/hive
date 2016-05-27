SELECT 'Upgrading MetaStore schema from 1.2.0 to 1.3.0';

\i 021-HIVE-11970.postgres.sql;
\i 022-HIVE-12807.postgres.sql;
\i 023-HIVE-12814.postgres.sql;
\i 024-HIVE-12816.postgres.sql;
\i 025-HIVE-12818.postgres.sql;
\i 026-HIVE-12819.postgres.sql;
\i 027-HIVE-12821.postgres.sql;
\i 028-HIVE-12822.postgres.sql;
\i 029-HIVE-12823.postgres.sql;
\i 030-HIVE-12831.postgres.sql;
\i 031-HIVE-12832.postgres.sql;
\i 034-HIVE-13395.postgres.sql;
\i 035-HIVE-13354.postgres.sql;

UPDATE "VERSION" SET "SCHEMA_VERSION"='1.3.0', "VERSION_COMMENT"='Hive release version 1.3.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 1.2.0 to 1.3.0';


--
-- Name: PCS_STATS_IDX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--


