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
