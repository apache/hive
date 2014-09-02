SELECT 'Upgrading MetaStore schema from 0.13.0 to 0.14.0';

\i 019-HIVE-7784.postgres.sql;

UPDATE "VERSION" SET "SCHEMA_VERSION"='0.14.0', "VERSION_COMMENT"='Hive release version 0.14.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 0.13.0 to 0.14.0';


--
-- Name: PCS_STATS_IDX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--


