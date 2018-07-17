SELECT 'Upgrading MetaStore schema from 1.2.1000 to 2.0.0';

-- \i 021-HIVE-11970.postgres.sql;
ALTER TABLE "COLUMNS_V2" ALTER "COLUMN_NAME" TYPE character varying(1000);
ALTER TABLE "PART_COL_PRIVS" ALTER "COLUMN_NAME" TYPE character varying(1000);
ALTER TABLE "TBL_COL_PRIVS" ALTER "COLUMN_NAME" TYPE character varying(1000);
ALTER TABLE "SORT_COLS" ALTER "COLUMN_NAME" TYPE character varying(1000);
ALTER TABLE "TAB_COL_STATS" ALTER "COLUMN_NAME" TYPE character varying(1000);
ALTER TABLE "PART_COL_STATS" ALTER  "COLUMN_NAME" TYPE character varying(1000);

UPDATE "VERSION" SET "SCHEMA_VERSION"='2.0.0', "VERSION_COMMENT"='Hive release version 2.0.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 1.2.1000 to 2.0.0';


--
-- Name: PCS_STATS_IDX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--
