SELECT 'Upgrading MetaStore schema from 2.1.0 to 2.2.0';

--\i 036-HIVE-14496.postgres.sql;
-- Step 1: Add the column allowing null
ALTER TABLE "TBLS" ADD COLUMN "IS_REWRITE_ENABLED" boolean NULL;

 -- Step 2: Replace the null with default value (false)
UPDATE "TBLS" SET "IS_REWRITE_ENABLED" = false;

-- Step 3: Alter the column to disallow null values
ALTER TABLE "TBLS" ALTER COLUMN "IS_REWRITE_ENABLED" SET NOT NULL;
ALTER TABLE "TBLS" ALTER COLUMN "IS_REWRITE_ENABLED" SET DEFAULT false;

--\i 037-HIVE-10562.postgres.sql;
ALTER TABLE "NOTIFICATION_LOG" ADD COLUMN "MESSAGE_FORMAT" VARCHAR(16) NULL;

--\i 038-HIVE-12274.postgres.sql;
alter table "SERDE_PARAMS" alter column "PARAM_VALUE" type text using cast("PARAM_VALUE" as text);
alter table "TABLE_PARAMS" alter column "PARAM_VALUE" type text using cast("PARAM_VALUE" as text);
alter table "SD_PARAMS" alter column "PARAM_VALUE" type text using cast("PARAM_VALUE" as text);
alter table "COLUMNS_V2" alter column "TYPE_NAME" type text using cast("TYPE_NAME" as text);

alter table "TBLS" ALTER COLUMN "TBL_NAME" TYPE varchar(256);
alter table "NOTIFICATION_LOG" alter column "TBL_NAME" TYPE varchar(256);
alter table "PARTITION_EVENTS" alter column "TBL_NAME" TYPE varchar(256);
alter table "TAB_COL_STATS" alter column "TABLE_NAME" TYPE varchar(256);
alter table "PART_COL_STATS" alter column "TABLE_NAME" TYPE varchar(256);
alter table COMPLETED_TXN_COMPONENTS alter column CTC_TABLE TYPE varchar(256);

alter table "COLUMNS_V2" alter column "COLUMN_NAME" TYPE varchar(767);
alter table "PART_COL_PRIVS" alter column "COLUMN_NAME" TYPE varchar(767);
alter table "TBL_COL_PRIVS" alter column "COLUMN_NAME" TYPE varchar(767);
alter table "SORT_COLS" alter column "COLUMN_NAME" TYPE varchar(767);
alter table "TAB_COL_STATS" alter column "COLUMN_NAME" TYPE varchar(767);
alter table "PART_COL_STATS" alter column "COLUMN_NAME" TYPE varchar(767);

UPDATE "VERSION" SET "SCHEMA_VERSION"='2.2.0', "VERSION_COMMENT"='Hive release version 2.2.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.0 to 2.2.0';

