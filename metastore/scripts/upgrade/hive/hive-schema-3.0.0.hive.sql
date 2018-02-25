-- HIVE system db

DROP DATABASE IF EXISTS SYS CASCADE;
CREATE DATABASE SYS;

USE SYS;

CREATE TABLE IF NOT EXISTS `BUCKETING_COLS` (
  `SD_ID` bigint,
  `BUCKET_COL_NAME` string,
  `INTEGER_IDX` int,
  CONSTRAINT `SYS_PK_BUCKETING_COLS` PRIMARY KEY (`SD_ID`,`INTEGER_IDX`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SD_ID\",
  \"BUCKET_COL_NAME\",
  \"INTEGER_IDX\"
FROM
  \"BUCKETING_COLS\""
);

CREATE TABLE IF NOT EXISTS `CDS` (
  `CD_ID` bigint,
  CONSTRAINT `SYS_PK_CDS` PRIMARY KEY (`CD_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"CD_ID\"
FROM
  \"CDS\""
);

CREATE TABLE IF NOT EXISTS `COLUMNS_V2` (
  `CD_ID` bigint,
  `COMMENT` string,
  `COLUMN_NAME` string,
  `TYPE_NAME` string,
  `INTEGER_IDX` int,
  CONSTRAINT `SYS_PK_COLUMN_V2` PRIMARY KEY (`CD_ID`,`COLUMN_NAME`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"CD_ID\",
  \"COMMENT\",
  \"COLUMN_NAME\",
  \"TYPE_NAME\",
  \"INTEGER_IDX\"
FROM
  \"COLUMNS_V2\""
);

CREATE TABLE IF NOT EXISTS `DATABASE_PARAMS` (
  `DB_ID` bigint,
  `PARAM_KEY` string,
  `PARAM_VALUE` string,
  CONSTRAINT `SYS_PK_DATABASE_PARAMS` PRIMARY KEY (`DB_ID`,`PARAM_KEY`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"DB_ID\",
  \"PARAM_KEY\",
  \"PARAM_VALUE\"
FROM
  \"DATABASE_PARAMS\""
);

CREATE TABLE IF NOT EXISTS `DBS` (
  `DB_ID` bigint,
  `DB_LOCATION_URI` string,
  `NAME` string,
  `OWNER_NAME` string,
  `OWNER_TYPE` string,
  CONSTRAINT `SYS_PK_DBS` PRIMARY KEY (`DB_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"DB_ID\",
  \"DB_LOCATION_URI\",
  \"NAME\",
  \"OWNER_NAME\",
  \"OWNER_TYPE\"
FROM
  DBS"
);

CREATE TABLE IF NOT EXISTS `DB_PRIVS` (
  `DB_GRANT_ID` bigint,
  `CREATE_TIME` int,
  `DB_ID` bigint,
  `GRANT_OPTION` int,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `DB_PRIV` string,
  CONSTRAINT `SYS_PK_DB_PRIVS` PRIMARY KEY (`DB_GRANT_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"DB_GRANT_ID\",
  \"CREATE_TIME\",
  \"DB_ID\",
  \"GRANT_OPTION\",
  \"GRANTOR\",
  \"GRANTOR_TYPE\",
  \"PRINCIPAL_NAME\",
  \"PRINCIPAL_TYPE\",
  \"DB_PRIV\"
FROM
  \"DB_PRIVS\""
);

CREATE TABLE IF NOT EXISTS `GLOBAL_PRIVS` (
  `USER_GRANT_ID` bigint,
  `CREATE_TIME` int,
  `GRANT_OPTION` string,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `USER_PRIV` string,
  CONSTRAINT `SYS_PK_GLOBAL_PRIVS` PRIMARY KEY (`USER_GRANT_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"USER_GRANT_ID\",
  \"CREATE_TIME\",
  \"GRANT_OPTION\",
  \"GRANTOR\",
  \"GRANTOR_TYPE\",
  \"PRINCIPAL_NAME\",
  \"PRINCIPAL_TYPE\",
  \"USER_PRIV\"
FROM
  \"GLOBAL_PRIVS\""
);

CREATE TABLE IF NOT EXISTS `IDXS` (
  `INDEX_ID` bigint,
  `CREATE_TIME` int,
  `DEFERRED_REBUILD` boolean,
  `INDEX_HANDLER_CLASS` string,
  `INDEX_NAME` string,
  `INDEX_TBL_ID` bigint,
  `LAST_ACCESS_TIME` int,
  `ORIG_TBL_ID` bigint,
  `SD_ID` bigint,
  CONSTRAINT `SYS_PK_IDXS` PRIMARY KEY (`INDEX_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"INDEX_ID\",
  \"CREATE_TIME\",
  \"DEFERRED_REBUILD\",
  \"INDEX_HANDLER_CLASS\",
  \"INDEX_NAME\",
  \"INDEX_TBL_ID\",
  \"LAST_ACCESS_TIME\",
  \"ORIG_TBL_ID\",
  \"SD_ID\"
FROM
  \"IDXS\""
);

CREATE TABLE IF NOT EXISTS `INDEX_PARAMS` (
  `INDEX_ID` bigint,
  `PARAM_KEY` string,
  `PARAM_VALUE` string,
  CONSTRAINT `SYS_PK_INDEX_PARAMS` PRIMARY KEY (`INDEX_ID`,`PARAM_KEY`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"INDEX_ID\",
  \"PARAM_KEY\",
  \"PARAM_VALUE\"
FROM
  \"INDEX_PARAMS\""
);

CREATE TABLE IF NOT EXISTS `PARTITIONS` (
  `PART_ID` bigint,
  `CREATE_TIME` int,
  `LAST_ACCESS_TIME` int,
  `PART_NAME` string,
  `SD_ID` bigint,
  `TBL_ID` bigint,
  CONSTRAINT `SYS_PK_PARTITIONS` PRIMARY KEY (`PART_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"PART_ID\",
  \"CREATE_TIME\",
  \"LAST_ACCESS_TIME\",
  \"PART_NAME\",
  \"SD_ID\",
  \"TBL_ID\"
FROM
  \"PARTITIONS\""
);

CREATE TABLE IF NOT EXISTS `PARTITION_KEYS` (
  `TBL_ID` bigint,
  `PKEY_COMMENT` string,
  `PKEY_NAME` string,
  `PKEY_TYPE` string,
  `INTEGER_IDX` int,
  CONSTRAINT `SYS_PK_PARTITION_KEYS` PRIMARY KEY (`TBL_ID`,`PKEY_NAME`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"TBL_ID\",
  \"PKEY_COMMENT\",
  \"PKEY_NAME\",
  \"PKEY_TYPE\",
  \"INTEGER_IDX\"
FROM
  \"PARTITION_KEYS\""
);

CREATE TABLE IF NOT EXISTS `PARTITION_KEY_VALS` (
  `PART_ID` bigint,
  `PART_KEY_VAL` string,
  `INTEGER_IDX` int,
  CONSTRAINT `SYS_PK_PARTITION_KEY_VALS` PRIMARY KEY (`PART_ID`,`INTEGER_IDX`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"PART_ID\",
  \"PART_KEY_VAL\",
  \"INTEGER_IDX\"
FROM
  \"PARTITION_KEY_VALS\""
);

CREATE TABLE IF NOT EXISTS `PARTITION_PARAMS` (
  `PART_ID` bigint,
  `PARAM_KEY` string,
  `PARAM_VALUE` string,
  CONSTRAINT `SYS_PK_PARTITION_PARAMS` PRIMARY KEY (`PART_ID`,`PARAM_KEY`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"PART_ID\",
  \"PARAM_KEY\",
  \"PARAM_VALUE\"
FROM
  \"PARTITION_PARAMS\""
);

CREATE TABLE IF NOT EXISTS `PART_COL_PRIVS` (
  `PART_COLUMN_GRANT_ID` bigint,
  `COLUMN_NAME` string,
  `CREATE_TIME` int,
  `GRANT_OPTION` int,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PART_ID` bigint,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `PART_COL_PRIV` string,
  CONSTRAINT `SYS_PK_PART_COL_PRIVS` PRIMARY KEY (`PART_COLUMN_GRANT_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"PART_COLUMN_GRANT_ID\",
  \"COLUMN_NAME\",
  \"CREATE_TIME\",
  \"GRANT_OPTION\",
  \"GRANTOR\",
  \"GRANTOR_TYPE\",
  \"PART_ID\",
  \"PRINCIPAL_NAME\",
  \"PRINCIPAL_TYPE\",
  \"PART_COL_PRIV\"
FROM
  \"PART_COL_PRIVS\""
);

CREATE TABLE IF NOT EXISTS `PART_PRIVS` (
  `PART_GRANT_ID` bigint,
  `CREATE_TIME` int,
  `GRANT_OPTION` int,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PART_ID` bigint,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `PART_PRIV` string,
  CONSTRAINT `SYS_PK_PART_PRIVS` PRIMARY KEY (`PART_GRANT_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"PART_GRANT_ID\",
  \"CREATE_TIME\",
  \"GRANT_OPTION\",
  \"GRANTOR\",
  \"GRANTOR_TYPE\",
  \"PART_ID\",
  \"PRINCIPAL_NAME\",
  \"PRINCIPAL_TYPE\",
  \"PART_PRIV\"
FROM
  \"PART_PRIVS\""
);

CREATE TABLE IF NOT EXISTS `ROLES` (
  `ROLE_ID` bigint,
  `CREATE_TIME` int,
  `OWNER_NAME` string,
  `ROLE_NAME` string,
  CONSTRAINT `SYS_PK_ROLES` PRIMARY KEY (`ROLE_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"ROLE_ID\",
  \"CREATE_TIME\",
  \"OWNER_NAME\",
  \"ROLE_NAME\"
FROM
  \"ROLES\""
);

CREATE TABLE IF NOT EXISTS `ROLE_MAP` (
  `ROLE_GRANT_ID` bigint,
  `ADD_TIME` int,
  `GRANT_OPTION` int,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `ROLE_ID` bigint,
  CONSTRAINT `SYS_PK_ROLE_MAP` PRIMARY KEY (`ROLE_GRANT_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"ROLE_GRANT_ID\",
  \"ADD_TIME\",
  \"GRANT_OPTION\",
  \"GRANTOR\",
  \"GRANTOR_TYPE\",
  \"PRINCIPAL_NAME\",
  \"PRINCIPAL_TYPE\",
  \"ROLE_ID\"
FROM
  \"ROLE_MAP\""
);

CREATE TABLE IF NOT EXISTS `SDS` (
  `SD_ID` bigint,
  `CD_ID` bigint,
  `INPUT_FORMAT` string,
  `IS_COMPRESSED` boolean,
  `IS_STOREDASSUBDIRECTORIES` boolean,
  `LOCATION` string,
  `NUM_BUCKETS` int,
  `OUTPUT_FORMAT` string,
  `SERDE_ID` bigint,
  CONSTRAINT `SYS_PK_SDS` PRIMARY KEY (`SD_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SD_ID\",
  \"CD_ID\",
  \"INPUT_FORMAT\",
  \"IS_COMPRESSED\",
  \"IS_STOREDASSUBDIRECTORIES\",
  \"LOCATION\",
  \"NUM_BUCKETS\",
  \"OUTPUT_FORMAT\",
  \"SERDE_ID\"
FROM
  \"SDS\""
);

CREATE TABLE IF NOT EXISTS `SD_PARAMS` (
  `SD_ID` bigint,
  `PARAM_KEY` string,
  `PARAM_VALUE` string,
  CONSTRAINT `SYS_PK_SD_PARAMS` PRIMARY KEY (`SD_ID`,`PARAM_KEY`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SD_ID\",
  \"PARAM_KEY\",
  \"PARAM_VALUE\"
FROM
  \"SD_PARAMS\""
);

CREATE TABLE IF NOT EXISTS `SEQUENCE_TABLE` (
  `SEQUENCE_NAME` string,
  `NEXT_VAL` bigint,
  CONSTRAINT `SYS_PK_SEQUENCE_TABLE` PRIMARY KEY (`SEQUENCE_NAME`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SEQUENCE_NAME\",
  \"NEXT_VAL\"
FROM
  \"SEQUENCE_TABLE\""
);

CREATE TABLE IF NOT EXISTS `SERDES` (
  `SERDE_ID` bigint,
  `NAME` string,
  `SLIB` string,
  CONSTRAINT `SYS_PK_SERDES` PRIMARY KEY (`SERDE_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SERDE_ID\",
  \"NAME\",
  \"SLIB\"
FROM
  \"SERDES\""
);

CREATE TABLE IF NOT EXISTS `SERDE_PARAMS` (
  `SERDE_ID` bigint,
  `PARAM_KEY` string,
  `PARAM_VALUE` string,
  CONSTRAINT `SYS_PK_SERDE_PARAMS` PRIMARY KEY (`SERDE_ID`,`PARAM_KEY`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SERDE_ID\",
  \"PARAM_KEY\",
  \"PARAM_VALUE\"
FROM
  \"SERDE_PARAMS\""
);

CREATE TABLE IF NOT EXISTS `SKEWED_COL_NAMES` (
  `SD_ID` bigint,
  `SKEWED_COL_NAME` string,
  `INTEGER_IDX` int,
  CONSTRAINT `SYS_PK_SKEWED_COL_NAMES` PRIMARY KEY (`SD_ID`,`INTEGER_IDX`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SD_ID\",
  \"SKEWED_COL_NAME\",
  \"INTEGER_IDX\"
FROM
  \"SKEWED_COL_NAMES\""
);

CREATE TABLE IF NOT EXISTS `SKEWED_COL_VALUE_LOC_MAP` (
  `SD_ID` bigint,
  `STRING_LIST_ID_KID` bigint,
  `LOCATION` string,
  CONSTRAINT `SYS_PK_COL_VALUE_LOC_MAP` PRIMARY KEY (`SD_ID`,`STRING_LIST_ID_KID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SD_ID\",
  \"STRING_LIST_ID_KID\",
  \"LOCATION\"
FROM
  \"SKEWED_COL_VALUE_LOC_MAP\""
);

CREATE TABLE IF NOT EXISTS `SKEWED_STRING_LIST` (
  `STRING_LIST_ID` bigint,
  CONSTRAINT `SYS_PK_SKEWED_STRING_LIST` PRIMARY KEY (`STRING_LIST_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"STRING_LIST_ID\"
FROM
  \"SKEWED_STRING_LIST\""
);

CREATE TABLE IF NOT EXISTS `SKEWED_STRING_LIST_VALUES` (
  `STRING_LIST_ID` bigint,
  `STRING_LIST_VALUE` string,
  `INTEGER_IDX` int,
  CONSTRAINT `SYS_PK_SKEWED_STRING_LIST_VALUES` PRIMARY KEY (`STRING_LIST_ID`,`INTEGER_IDX`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"STRING_LIST_ID\",
  \"STRING_LIST_VALUE\",
  \"INTEGER_IDX\"
FROM
  \"SKEWED_STRING_LIST_VALUES\""
);

CREATE TABLE IF NOT EXISTS `SKEWED_VALUES` (
  `SD_ID_OID` bigint,
  `STRING_LIST_ID_EID` bigint,
  `INTEGER_IDX` int,
  CONSTRAINT `SYS_PK_SKEWED_VALUES` PRIMARY KEY (`SD_ID_OID`,`INTEGER_IDX`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SD_ID_OID\",
  \"STRING_LIST_ID_EID\",
  \"INTEGER_IDX\"
FROM
  \"SKEWED_VALUES\""
);

CREATE TABLE IF NOT EXISTS `SORT_COLS` (
  `SD_ID` bigint,
  `COLUMN_NAME` string,
  `ORDER` int,
  `INTEGER_IDX` int,
  CONSTRAINT `SYS_PK_SORT_COLS` PRIMARY KEY (`SD_ID`,`INTEGER_IDX`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SD_ID\",
  \"COLUMN_NAME\",
  \"ORDER\",
  \"INTEGER_IDX\"
FROM
  \"SORT_COLS\""
);

CREATE TABLE IF NOT EXISTS `TABLE_PARAMS` (
  `TBL_ID` bigint,
  `PARAM_KEY` string,
  `PARAM_VALUE` string,
  CONSTRAINT `SYS_PK_TABLE_PARAMS` PRIMARY KEY (`TBL_ID`,`PARAM_KEY`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"TBL_ID\",
  \"PARAM_KEY\",
  \"PARAM_VALUE\"
FROM
  \"TABLE_PARAMS\""
);

CREATE TABLE IF NOT EXISTS `TBLS` (
  `TBL_ID` bigint,
  `CREATE_TIME` int,
  `DB_ID` bigint,
  `LAST_ACCESS_TIME` int,
  `OWNER` string,
  `RETENTION` int,
  `SD_ID` bigint,
  `TBL_NAME` string,
  `TBL_TYPE` string,
  `VIEW_EXPANDED_TEXT` string,
  `VIEW_ORIGINAL_TEXT` string,
  `IS_REWRITE_ENABLED` boolean,
  CONSTRAINT `SYS_PK_TBLS` PRIMARY KEY (`TBL_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"TBL_ID\",
  \"CREATE_TIME\",
  \"DB_ID\",
  \"LAST_ACCESS_TIME\",
  \"OWNER\",
  \"RETENTION\",
  \"SD_ID\",
  \"TBL_NAME\",
  \"TBL_TYPE\",
  \"VIEW_EXPANDED_TEXT\",
  \"VIEW_ORIGINAL_TEXT\",
  \"IS_REWRITE_ENABLED\"
FROM TBLS"
);

CREATE TABLE IF NOT EXISTS `MV_CREATION_METADATA` (
  `MV_CREATION_METADATA_ID` bigint,
  `DB_NAME` string,
  `TBL_NAME` string,
  `TXN_LIST` string,
  CONSTRAINT `SYS_PK_MV_CREATION_METADATA` PRIMARY KEY (`MV_CREATION_METADATA_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"MV_CREATION_METADATA_ID\",
  \"DB_NAME\",
  \"TBL_NAME\",
  \"TXN_LIST\"
FROM MV_CREATION_METADATA"
);

CREATE TABLE IF NOT EXISTS `MV_TABLES_USED` (
  `MV_CREATION_METADATA_ID` bigint,
  `TBL_ID` bigint
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"MV_CREATION_METADATA_ID\",
  \"TBL_ID\"
FROM MV_TABLES_USED"
);

CREATE TABLE IF NOT EXISTS `TBL_COL_PRIVS` (
  `TBL_COLUMN_GRANT_ID` bigint,
  `COLUMN_NAME` string,
  `CREATE_TIME` int,
  `GRANT_OPTION` int,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `TBL_COL_PRIV` string,
  `TBL_ID` bigint,
  CONSTRAINT `SYS_PK_TBL_COL_PRIVS` PRIMARY KEY (`TBL_COLUMN_GRANT_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"TBL_COLUMN_GRANT_ID\",
  \"COLUMN_NAME\",
  \"CREATE_TIME\",
  \"GRANT_OPTION\",
  \"GRANTOR\",
  \"GRANTOR_TYPE\",
  \"PRINCIPAL_NAME\",
  \"PRINCIPAL_TYPE\",
  \"TBL_COL_PRIV\",
  \"TBL_ID\"
FROM
  \"TBL_COL_PRIVS\""
);

CREATE TABLE IF NOT EXISTS `TBL_PRIVS` (
  `TBL_GRANT_ID` bigint,
  `CREATE_TIME` int,
  `GRANT_OPTION` int,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `TBL_PRIV` string,
  `TBL_ID` bigint,
  CONSTRAINT `SYS_PK_TBL_PRIVS` PRIMARY KEY (`TBL_GRANT_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"TBL_GRANT_ID\",
  \"CREATE_TIME\",
  \"GRANT_OPTION\",
  \"GRANTOR\",
  \"GRANTOR_TYPE\",
  \"PRINCIPAL_NAME\",
  \"PRINCIPAL_TYPE\",
  \"TBL_PRIV\",
  \"TBL_ID\"
FROM
  \"TBL_PRIVS\""
);

CREATE TABLE IF NOT EXISTS `TAB_COL_STATS` (
 `CS_ID` bigint,
 `DB_NAME` string,
 `TABLE_NAME` string,
 `COLUMN_NAME` string,
 `COLUMN_TYPE` string,
 `TBL_ID` bigint,
 `LONG_LOW_VALUE` bigint,
 `LONG_HIGH_VALUE` bigint,
 `DOUBLE_HIGH_VALUE` double,
 `DOUBLE_LOW_VALUE` double,
 `BIG_DECIMAL_LOW_VALUE` string,
 `BIG_DECIMAL_HIGH_VALUE` string,
 `NUM_NULLS` bigint,
 `NUM_DISTINCTS` bigint,
 `AVG_COL_LEN` double,
 `MAX_COL_LEN` bigint,
 `NUM_TRUES` bigint,
 `NUM_FALSES` bigint,
 `LAST_ANALYZED` bigint,
  CONSTRAINT `SYS_PK_TAB_COL_STATS` PRIMARY KEY (`CS_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
 \"CS_ID\",
 \"DB_NAME\",
 \"TABLE_NAME\",
 \"COLUMN_NAME\",
 \"COLUMN_TYPE\",
 \"TBL_ID\",
 \"LONG_LOW_VALUE\",
 \"LONG_HIGH_VALUE\",
 \"DOUBLE_HIGH_VALUE\",
 \"DOUBLE_LOW_VALUE\",
 \"BIG_DECIMAL_LOW_VALUE\",
 \"BIG_DECIMAL_HIGH_VALUE\",
 \"NUM_NULLS\",
 \"NUM_DISTINCTS\",
 \"AVG_COL_LEN\",
 \"MAX_COL_LEN\",
 \"NUM_TRUES\",
 \"NUM_FALSES\",
 \"LAST_ANALYZED\"
FROM
  \"TAB_COL_STATS\""
);

CREATE TABLE IF NOT EXISTS `PART_COL_STATS` (
 `CS_ID` bigint,
 `DB_NAME` string,
 `TABLE_NAME` string,
 `PARTITION_NAME` string,
 `COLUMN_NAME` string,
 `COLUMN_TYPE` string,
 `PART_ID` bigint,
 `LONG_LOW_VALUE` bigint,
 `LONG_HIGH_VALUE` bigint,
 `DOUBLE_HIGH_VALUE` double,
 `DOUBLE_LOW_VALUE` double,
 `BIG_DECIMAL_LOW_VALUE` string,
 `BIG_DECIMAL_HIGH_VALUE` string,
 `NUM_NULLS` bigint,
 `NUM_DISTINCTS` bigint,
 `AVG_COL_LEN` double,
 `MAX_COL_LEN` bigint,
 `NUM_TRUES` bigint,
 `NUM_FALSES` bigint,
 `LAST_ANALYZED` bigint,
  CONSTRAINT `SYS_PK_PART_COL_STATS` PRIMARY KEY (`CS_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
 \"CS_ID\",
 \"DB_NAME\",
 \"TABLE_NAME\",
 \"PARTITION_NAME\",
 \"COLUMN_NAME\",
 \"COLUMN_TYPE\",
 \"PART_ID\",
 \"LONG_LOW_VALUE\",
 \"LONG_HIGH_VALUE\",
 \"DOUBLE_HIGH_VALUE\",
 \"DOUBLE_LOW_VALUE\",
 \"BIG_DECIMAL_LOW_VALUE\",
 \"BIG_DECIMAL_HIGH_VALUE\",
 \"NUM_NULLS\",
 \"NUM_DISTINCTS\",
 \"AVG_COL_LEN\",
 \"MAX_COL_LEN\",
 \"NUM_TRUES\",
 \"NUM_FALSES\",
 \"LAST_ANALYZED\"
FROM
  \"PART_COL_STATS\""
);

CREATE TABLE IF NOT EXISTS `VERSION` (
  `VER_ID` BIGINT,
  `SCHEMA_VERSION` string,
  `VERSION_COMMENT` string,
  CONSTRAINT `SYS_PK_VERSION` PRIMARY KEY (`VER_ID`) DISABLE
);

INSERT INTO `VERSION` VALUES (1, '3.0.0', 'Hive release version 3.0.0');

CREATE TABLE IF NOT EXISTS `DB_VERSION` (
  `VER_ID` BIGINT,
  `SCHEMA_VERSION` string,
  `VERSION_COMMENT` string,
  CONSTRAINT `SYS_PK_DB_VERSION` PRIMARY KEY (`VER_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"VER_ID\",
  \"SCHEMA_VERSION\",
  \"VERSION_COMMENT\"
FROM
  \"VERSION\""
);

CREATE TABLE IF NOT EXISTS `FUNCS` (
  `FUNC_ID` bigint,
  `CLASS_NAME` string,
  `CREATE_TIME` int,
  `DB_ID` bigint,
  `FUNC_NAME` string,
  `FUNC_TYPE` int,
  `OWNER_NAME` string,
  `OWNER_TYPE` string,
  CONSTRAINT `SYS_PK_FUNCS` PRIMARY KEY (`FUNC_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"FUNC_ID\",
  \"CLASS_NAME\",
  \"CREATE_TIME\",
  \"DB_ID\",
  \"FUNC_NAME\",
  \"FUNC_TYPE\",
  \"OWNER_NAME\",
  \"OWNER_TYPE\"
FROM
  \"FUNCS\""
);

-- CREATE TABLE IF NOT EXISTS `FUNC_RU` (
--   `FUNC_ID` bigint,
--   `RESOURCE_TYPE` int,
--   `RESOURCE_URI` string,
--   `INTEGER_IDX` int,
--   CONSTRAINT `SYS_PK_FUNCS_RU` PRIMARY KEY (`FUNC_ID`, `INTEGER_IDX`) DISABLE
-- )
-- STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
-- TBLPROPERTIES (
-- "hive.sql.database.type" = "METASTORE",
-- "hive.sql.query" = "SELECT * FROM FUNCS_RU"
-- );

CREATE TABLE IF NOT EXISTS `KEY_CONSTRAINTS`
(
  `CHILD_CD_ID` bigint,
  `CHILD_INTEGER_IDX` int,
  `CHILD_TBL_ID` bigint,
  `PARENT_CD_ID` bigint,
  `PARENT_INTEGER_IDX` int,
  `PARENT_TBL_ID` bigint,
  `POSITION` bigint,
  `CONSTRAINT_NAME` string,
  `CONSTRAINT_TYPE` string,
  `UPDATE_RULE` string,
  `DELETE_RULE` string,
  `ENABLE_VALIDATE_RELY` int,
  CONSTRAINT `SYS_PK_KEY_CONSTRAINTS` PRIMARY KEY (`CONSTRAINT_NAME`, `POSITION`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"CHILD_CD_ID\",
  \"CHILD_INTEGER_IDX\",
  \"CHILD_TBL_ID\",
  \"PARENT_CD_ID\",
  \"PARENT_INTEGER_IDX\",
  \"PARENT_TBL_ID\",
  \"POSITION\",
  \"CONSTRAINT_NAME\",
  \"CONSTRAINT_TYPE\",
  \"UPDATE_RULE\",
  \"DELETE_RULE\",
  \"ENABLE_VALIDATE_RELY\"
FROM
  \"KEY_CONSTRAINTS\""
);

CREATE VIEW `TABLE_STATS_VIEW` AS
SELECT
  `TBL_ID`,
  max(CASE `PARAM_KEY` WHEN 'COLUMN_STATS_ACCURATE' THEN `PARAM_VALUE` END) AS COLUMN_STATS_ACCURATE,
  max(CASE `PARAM_KEY` WHEN 'numFiles' THEN `PARAM_VALUE` END) AS NUM_FILES,
  max(CASE `PARAM_KEY` WHEN 'numRows' THEN `PARAM_VALUE` END) AS NUM_ROWS,
  max(CASE `PARAM_KEY` WHEN 'rawDataSize' THEN `PARAM_VALUE` END) AS RAW_DATA_SIZE,
  max(CASE `PARAM_KEY` WHEN 'totalSize' THEN `PARAM_VALUE` END) AS TOTAL_SIZE,
  max(CASE `PARAM_KEY` WHEN 'transient_lastDdlTime' THEN `PARAM_VALUE` END) AS TRANSIENT_LAST_DDL_TIME
FROM `TABLE_PARAMS` GROUP BY `TBL_ID`;

CREATE VIEW `PARTITION_STATS_VIEW` AS
SELECT
  `PART_ID`,
  max(CASE `PARAM_KEY` WHEN 'COLUMN_STATS_ACCURATE' THEN `PARAM_VALUE` END) AS COLUMN_STATS_ACCURATE,
  max(CASE `PARAM_KEY` WHEN 'numFiles' THEN `PARAM_VALUE` END) AS NUM_FILES,
  max(CASE `PARAM_KEY` WHEN 'numRows' THEN `PARAM_VALUE` END) AS NUM_ROWS,
  max(CASE `PARAM_KEY` WHEN 'rawDataSize' THEN `PARAM_VALUE` END) AS RAW_DATA_SIZE,
  max(CASE `PARAM_KEY` WHEN 'totalSize' THEN `PARAM_VALUE` END) AS TOTAL_SIZE,
  max(CASE `PARAM_KEY` WHEN 'transient_lastDdlTime' THEN `PARAM_VALUE` END) AS TRANSIENT_LAST_DDL_TIME
FROM `PARTITION_PARAMS` GROUP BY `PART_ID`;

CREATE TABLE IF NOT EXISTS `WM_RESOURCEPLANS` (
  `NAME` string,
  `STATUS` string,
  `QUERY_PARALLELISM` int,
  `DEFAULT_POOL_PATH` string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"WM_RESOURCEPLAN\".\"NAME\",
  \"STATUS\",
  \"WM_RESOURCEPLAN\".\"QUERY_PARALLELISM\",
  \"WM_POOL\".\"PATH\"
FROM
  \"WM_RESOURCEPLAN\" LEFT OUTER JOIN \"WM_POOL\" ON \"WM_RESOURCEPLAN\".\"DEFAULT_POOL_ID\" = \"WM_POOL\".\"POOL_ID\""
);

CREATE TABLE IF NOT EXISTS `WM_TRIGGERS` (
  `RP_NAME` string,
  `NAME` string,
  `TRIGGER_EXPRESSION` string,
  `ACTION_EXPRESSION` string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  r.NAME RP_NAME,
  t.NAME NAME,
  TRIGGER_EXPRESSION,
  ACTION_EXPRESSION
FROM
  WM_TRIGGER t
JOIN
  WM_RESOURCEPLAN r
ON
  t.RP_ID = r.RP_ID"
);

CREATE TABLE IF NOT EXISTS `WM_POOLS` (
  `RP_NAME` string,
  `PATH` string,
  `ALLOC_FRACTION` double,
  `QUERY_PARALLELISM` int,
  `SCHEDULING_POLICY` string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  WM_RESOURCEPLAN.NAME,
  WM_POOL.PATH,
  WM_POOL.ALLOC_FRACTION,
  WM_POOL.QUERY_PARALLELISM,
  WM_POOL.SCHEDULING_POLICY
FROM
  WM_POOL
JOIN
  WM_RESOURCEPLAN
ON
  WM_POOL.RP_ID = WM_RESOURCEPLAN.RP_ID"
);

CREATE TABLE IF NOT EXISTS `WM_POOLS_TO_TRIGGERS` (
  `RP_NAME` string,
  `POOL_PATH` string,
  `TRIGGER_NAME` string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  WM_RESOURCEPLAN.NAME RP_NAME,
  WM_POOL.PATH POOL_PATH,
  WM_TRIGGER.NAME TRIGGER_NAME
FROM WM_POOL_TO_TRIGGER
  JOIN WM_POOL ON WM_POOL_TO_TRIGGER.POOL_ID = WM_POOL.POOL_ID
  JOIN WM_TRIGGER ON WM_POOL_TO_TRIGGER.TRIGGER_ID = WM_TRIGGER.TRIGGER_ID
  JOIN WM_RESOURCEPLAN ON WM_POOL.RP_ID = WM_RESOURCEPLAN.RP_ID
UNION
SELECT
  WM_RESOURCEPLAN.NAME RP_NAME,
  '<unmanaged queries>' POOL_PATH,
  WM_TRIGGER.NAME TRIGGER_NAME
FROM WM_TRIGGER
  JOIN WM_RESOURCEPLAN ON WM_TRIGGER.RP_ID = WM_RESOURCEPLAN.RP_ID
WHERE WM_TRIGGER.IS_IN_UNMANAGED = 1
"
);

CREATE TABLE IF NOT EXISTS `WM_MAPPINGS` (
  `RP_NAME` string,
  `ENTITY_TYPE` string,
  `ENTITY_NAME` string,
  `POOL_PATH` string,
  `ORDERING` int
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  WM_RESOURCEPLAN.NAME,
  ENTITY_TYPE,
  ENTITY_NAME,
  case when WM_POOL.PATH is null then '<unmanaged>' else WM_POOL.PATH end,
  ORDERING
FROM WM_MAPPING
JOIN WM_RESOURCEPLAN ON WM_MAPPING.RP_ID = WM_RESOURCEPLAN.RP_ID
LEFT OUTER JOIN WM_POOL ON WM_POOL.POOL_ID = WM_MAPPING.POOL_ID
"
);

DROP DATABASE IF EXISTS INFORMATION_SCHEMA CASCADE;
CREATE DATABASE INFORMATION_SCHEMA;

USE INFORMATION_SCHEMA;

CREATE VIEW IF NOT EXISTS `SCHEMATA`
(
  `CATALOG_NAME`,
  `SCHEMA_NAME`,
  `SCHEMA_OWNER`,
  `DEFAULT_CHARACTER_SET_CATALOG`,
  `DEFAULT_CHARACTER_SET_SCHEMA`,
  `DEFAULT_CHARACTER_SET_NAME`,
  `SQL_PATH`
) AS
SELECT
  'default',
  `NAME`,
  `OWNER_NAME`,
  cast(null as string),
  cast(null as string),
  cast(null as string),
  `DB_LOCATION_URI`
FROM
  sys.DBS;

CREATE VIEW IF NOT EXISTS `TABLES`
(
  `TABLE_CATALOG`,
  `TABLE_SCHEMA`,
  `TABLE_NAME`,
  `TABLE_TYPE`,
  `SELF_REFERENCING_COLUMN_NAME`,
  `REFERENCE_GENERATION`,
  `USER_DEFINED_TYPE_CATALOG`,
  `USER_DEFINED_TYPE_SCHEMA`,
  `USER_DEFINED_TYPE_NAME`,
  `IS_INSERTABLE_INTO`,
  `IS_TYPED`,
  `COMMIT_ACTION`
) AS
SELECT
  'default',
  D.NAME,
  T.TBL_NAME,
  IF(length(T.VIEW_ORIGINAL_TEXT) > 0, 'VIEW', 'BASE_TABLE'),
  cast(null as string),
  cast(null as string),
  cast(null as string),
  cast(null as string),
  cast(null as string),
  IF(length(T.VIEW_ORIGINAL_TEXT) > 0, 'NO', 'YES'),
  'NO',
  cast(null as string)
FROM
  `sys`.`TBLS` T, `sys`.`DBS` D
WHERE
  D.`DB_ID` = T.`DB_ID`;

CREATE VIEW IF NOT EXISTS `TABLE_PRIVILEGES`
(
  `GRANTOR`,
  `GRANTEE`,
  `TABLE_CATALOG`,
  `TABLE_SCHEMA`,
  `TABLE_NAME`,
  `PRIVILEGE_TYPE`,
  `IS_GRANTABLE`,
  `WITH_HIERARCHY`
) AS
SELECT
  `GRANTOR`,
  `PRINCIPAL_NAME`,
  'default',
  D.`NAME`,
  T.`TBL_NAME`,
  P.`TBL_PRIV`,
  IF (P.`GRANT_OPTION` == 0, 'NO', 'YES'),
  'NO'
FROM
  sys.`TBL_PRIVS` P,
  sys.`TBLS` T,
  sys.`DBS` D
WHERE
  P.TBL_ID = T.TBL_ID
  AND T.DB_ID = D.DB_ID;

CREATE VIEW IF NOT EXISTS `COLUMNS`
(
  `TABLE_CATALOG`,
  `TABLE_SCHEMA`,
  `TABLE_NAME`,
  `COLUMN_NAME`,
  `ORDINAL_POSITION`,
  `COLUMN_DEFAULT`,
  `IS_NULLABLE`,
  `DATA_TYPE`,
  `CHARACTER_MAXIMUM_LENGTH`,
  `CHARACTER_OCTET_LENGTH`,
  `NUMERIC_PRECISION`,
  `NUMERIC_PRECISION_RADIX`,
  `NUMERIC_SCALE`,
  `DATETIME_PRECISION`,
  `INTERVAL_TYPE`,
  `INTERVAL_PRECISION`,
  `CHARACTER_SET_CATALOG`,
  `CHARACTER_SET_SCHEMA`,
  `CHARACTER_SET_NAME`,
  `COLLATION_CATALOG`,
  `COLLATION_SCHEMA`,
  `COLLATION_NAME`,
  `UDT_CATALOG`,
  `UDT_SCHEMA`,
  `UDT_NAME`,
  `SCOPE_CATALOG`,
  `SCOPE_SCHEMA`,
  `SCOPE_NAME`,
  `MAXIMUM_CARDINALITY`,
  `DTD_IDENTIFIER`,
  `IS_SELF_REFERENCING`,
  `IS_IDENTITY`,
  `IDENTITY_GENERATION`,
  `IDENTITY_START`,
  `IDENTITY_INCREMENT`,
  `IDENTITY_MAXIMUM`,
  `IDENTITY_MINIMUM`,
  `IDENTITY_CYCLE`,
  `IS_GENERATED`,
  `GENERATION_EXPRESSION`,
  `IS_SYSTEM_TIME_PERIOD_START`,
  `IS_SYSTEM_TIME_PERIOD_END`,
  `SYSTEM_TIME_PERIOD_TIMESTAMP_GENERATION`,
  `IS_UPDATABLE`,
  `DECLARED_DATA_TYPE`,
  `DECLARED_NUMERIC_PRECISION`,
  `DECLARED_NUMERIC_SCALE`
) AS
SELECT
  'default',
  D.NAME,
  T.TBL_NAME,
  C.COLUMN_NAME,
  C.INTEGER_IDX,
  cast (null as string),
  'YES',
  C.TYPE_NAME as TYPE_NAME,
  CASE WHEN lower(C.TYPE_NAME) like 'varchar%' THEN cast(regexp_extract(upper(C.TYPE_NAME), '^VARCHAR\\s*\\((\\d+)\\s*\\)$', 1) as int)
       WHEN lower(C.TYPE_NAME) like 'char%'    THEN cast(regexp_extract(upper(C.TYPE_NAME),    '^CHAR\\s*\\((\\d+)\\s*\\)$', 1) as int)
       ELSE null END,
  CASE WHEN lower(C.TYPE_NAME) like 'varchar%' THEN cast(regexp_extract(upper(C.TYPE_NAME), '^VARCHAR\\s*\\((\\d+)\\s*\\)$', 1) as int)
       WHEN lower(C.TYPE_NAME) like 'char%'    THEN cast(regexp_extract(upper(C.TYPE_NAME),    '^CHAR\\s*\\((\\d+)\\s*\\)$', 1) as int)
       ELSE null END,
  CASE WHEN lower(C.TYPE_NAME) = 'bigint' THEN 19
       WHEN lower(C.TYPE_NAME) = 'int' THEN 10
       WHEN lower(C.TYPE_NAME) = 'smallint' THEN 5
       WHEN lower(C.TYPE_NAME) = 'tinyint' THEN 3
       WHEN lower(C.TYPE_NAME) = 'float' THEN 23
       WHEN lower(C.TYPE_NAME) = 'double' THEN 53
       WHEN lower(C.TYPE_NAME) like 'decimal%' THEN regexp_extract(upper(C.TYPE_NAME), '^DECIMAL\\s*\\((\\d+)',1)
       WHEN lower(C.TYPE_NAME) like 'numeric%' THEN regexp_extract(upper(C.TYPE_NAME), '^NUMERIC\\s*\\((\\d+)',1)
       ELSE null END,
  CASE WHEN lower(C.TYPE_NAME) = 'bigint' THEN 10
       WHEN lower(C.TYPE_NAME) = 'int' THEN 10
       WHEN lower(C.TYPE_NAME) = 'smallint' THEN 10
       WHEN lower(C.TYPE_NAME) = 'tinyint' THEN 10
       WHEN lower(C.TYPE_NAME) = 'float' THEN 2
       WHEN lower(C.TYPE_NAME) = 'double' THEN 2
       WHEN lower(C.TYPE_NAME) like 'decimal%' THEN 10
       WHEN lower(C.TYPE_NAME) like 'numeric%' THEN 10
       ELSE null END,
  CASE WHEN lower(C.TYPE_NAME) like 'decimal%' THEN regexp_extract(upper(C.TYPE_NAME), '^DECIMAL\\s*\\((\\d+),(\\d+)',2)
       WHEN lower(C.TYPE_NAME) like 'numeric%' THEN regexp_extract(upper(C.TYPE_NAME), '^NUMERIC\\s*\\((\\d+),(\\d+)',2)
       ELSE null END,
  CASE WHEN lower(C.TYPE_NAME) = 'date' THEN 0
       WHEN lower(C.TYPE_NAME) = 'timestamp' THEN 9
       ELSE null END,
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  C.CD_ID,
  'NO',
  'NO',
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  cast (null as string),
  'NEVER',
  cast (null as string),
  'NO',
  'NO',
  cast (null as string),
  'YES',
  C.TYPE_NAME as DECLARED_DATA_TYPE,
  CASE WHEN lower(C.TYPE_NAME) = 'bigint' THEN 19
       WHEN lower(C.TYPE_NAME) = 'int' THEN 10
       WHEN lower(C.TYPE_NAME) = 'smallint' THEN 5
       WHEN lower(C.TYPE_NAME) = 'tinyint' THEN 3
       WHEN lower(C.TYPE_NAME) = 'float' THEN 23
       WHEN lower(C.TYPE_NAME) = 'double' THEN 53
       WHEN lower(C.TYPE_NAME) like 'decimal%' THEN regexp_extract(upper(C.TYPE_NAME), '^DECIMAL\\s*\\((\\d+)',1)
       WHEN lower(C.TYPE_NAME) like 'numeric%' THEN regexp_extract(upper(C.TYPE_NAME), '^NUMERIC\\s*\\((\\d+)',1)
       ELSE null END,
  CASE WHEN lower(C.TYPE_NAME) = 'bigint' THEN 10
       WHEN lower(C.TYPE_NAME) = 'int' THEN 10
       WHEN lower(C.TYPE_NAME) = 'smallint' THEN 10
       WHEN lower(C.TYPE_NAME) = 'tinyint' THEN 10
       WHEN lower(C.TYPE_NAME) = 'float' THEN 2
       WHEN lower(C.TYPE_NAME) = 'double' THEN 2
       WHEN lower(C.TYPE_NAME) like 'decimal%' THEN 10
       WHEN lower(C.TYPE_NAME) like 'numeric%' THEN 10
       ELSE null END
FROM
  sys.`COLUMNS_V2` C,
  sys.`SDS` S,
  sys.`TBLS` T,
  sys.`DBS` D
WHERE
  S.`SD_ID` = T.`SD_ID`
  AND T.`DB_ID` = D.`DB_ID`
  AND C.`CD_ID` = S.`CD_ID`;

CREATE VIEW IF NOT EXISTS `COLUMN_PRIVILEGES`
(
  `GRANTOR`,
  `GRANTEE`,
  `TABLE_CATALOG`,
  `TABLE_SCHEMA`,
  `TABLE_NAME`,
  `COLUMN_NAME`,
  `PRIVILEGE_TYPE`,
  `IS_GRANTABLE`
) AS
SELECT
  `GRANTOR`,
  `PRINCIPAL_NAME`,
  'default',
  D.`NAME`,
  T.`TBL_NAME`,
  C.`COLUMN_NAME`,
  P.`TBL_COL_PRIV`,
  IF (P.`GRANT_OPTION` == 0, 'NO', 'YES')
FROM
  sys.`TBL_COL_PRIVS` P,
  sys.`TBLS` T,
  sys.`DBS` D,
  sys.`COLUMNS_V2` C,
  sys.`SDS` S
WHERE
  S.`SD_ID` = T.`SD_ID`
  AND T.`DB_ID` = D.`DB_ID`
  AND P.`TBL_ID` = T.`TBL_ID`
  AND P.`COLUMN_NAME` = C.`COLUMN_NAME`
  AND C.`CD_ID` = S.`CD_ID`;

CREATE VIEW IF NOT EXISTS `VIEWS`
(
  `TABLE_CATALOG`,
  `TABLE_SCHEMA`,
  `TABLE_NAME`,
  `VIEW_DEFINITION`,
  `CHECK_OPTION`,
  `IS_UPDATABLE`,
  `IS_INSERTABLE_INTO`,
  `IS_TRIGGER_UPDATABLE`,
  `IS_TRIGGER_DELETABLE`,
  `IS_TRIGGER_INSERTABLE_INTO`
) AS
SELECT
  'default',
  D.NAME,
  T.TBL_NAME,
  T.VIEW_ORIGINAL_TEXT,
  CAST(NULL as string),
  false,
  false,
  false,
  false,
  false
FROM
  `sys`.`DBS` D,
  `sys`.`TBLS` T
WHERE
   D.`DB_ID` = T.`DB_ID` AND
   length(T.VIEW_ORIGINAL_TEXT) > 0;
