-- HIVE system db

CREATE DATABASE IF NOT EXISTS SYS;

USE SYS;

CREATE EXTERNAL TABLE IF NOT EXISTS `BUCKETING_COLS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `CDS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `COLUMNS_V2` (
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


CREATE EXTERNAL TABLE IF NOT EXISTS `DATABASE_PARAMS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `DBS` (
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
  \"DBS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `DB_PRIVS` (
  `DB_GRANT_ID` bigint,
  `CREATE_TIME` int,
  `DB_ID` bigint,
  `GRANT_OPTION` int,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `DB_PRIV` string,
  `AUTHORIZER` string,
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
  \"DB_PRIV\",
  \"AUTHORIZER\"
FROM
  \"DB_PRIVS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `GLOBAL_PRIVS` (
  `USER_GRANT_ID` bigint,
  `CREATE_TIME` int,
  `GRANT_OPTION` string,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `USER_PRIV` string,
  `AUTHORIZER` string,
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
  \"USER_PRIV\",
  \"AUTHORIZER\"
FROM
  \"GLOBAL_PRIVS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `PARTITIONS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `PARTITION_KEYS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `PARTITION_KEY_VALS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `PARTITION_PARAMS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `PART_COL_PRIVS` (
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
  `AUTHORIZER` string,
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
  \"PART_COL_PRIV\",
  \"AUTHORIZER\"
FROM
  \"PART_COL_PRIVS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `PART_PRIVS` (
  `PART_GRANT_ID` bigint,
  `CREATE_TIME` int,
  `GRANT_OPTION` int,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PART_ID` bigint,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `PART_PRIV` string,
  `AUTHORIZER` string,
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
  \"PART_PRIV\",
  \"AUTHORIZER\"
FROM
  \"PART_PRIVS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `ROLES` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `ROLE_MAP` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `SDS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `SD_PARAMS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `SEQUENCE_TABLE` (
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


CREATE EXTERNAL TABLE IF NOT EXISTS `SERDES` (
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


CREATE EXTERNAL TABLE IF NOT EXISTS `SERDE_PARAMS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `SKEWED_COL_NAMES` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `SKEWED_COL_VALUE_LOC_MAP` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `SKEWED_STRING_LIST` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `SKEWED_STRING_LIST_VALUES` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `SKEWED_VALUES` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `SORT_COLS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `TABLE_PARAMS` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `TBLS` (
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
FROM \"TBLS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `MV_CREATION_METADATA` (
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
FROM \"MV_CREATION_METADATA\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `MV_TABLES_USED` (
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
FROM \"MV_TABLES_USED\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `TBL_COL_PRIVS` (
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
  `AUTHORIZER` string,
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
  \"TBL_ID\",
  \"AUTHORIZER\"
FROM
  \"TBL_COL_PRIVS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `TBL_PRIVS` (
  `TBL_GRANT_ID` bigint,
  `CREATE_TIME` int,
  `GRANT_OPTION` int,
  `GRANTOR` string,
  `GRANTOR_TYPE` string,
  `PRINCIPAL_NAME` string,
  `PRINCIPAL_TYPE` string,
  `TBL_PRIV` string,
  `TBL_ID` bigint,
  `AUTHORIZER` string,
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
  \"TBL_ID\",
  \"AUTHORIZER\"
FROM
  \"TBL_PRIVS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `TAB_COL_STATS` (
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
 `ENGINE` string,
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
 \"LAST_ANALYZED\",
 \"ENGINE\"
FROM
  \"TAB_COL_STATS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `PART_COL_STATS` (
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
 `ENGINE` string,
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
 \"LAST_ANALYZED\",
 \"ENGINE\"
FROM
  \"PART_COL_STATS\""
);

CREATE OR REPLACE VIEW `VERSION` AS SELECT 1 AS `VER_ID`, '4.0.0' AS `SCHEMA_VERSION`,
  'Hive release version 4.0.0' AS `VERSION_COMMENT`;

CREATE EXTERNAL TABLE IF NOT EXISTS `DB_VERSION` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `FUNCS` (
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

-- CREATE EXTERNAL TABLE IF NOT EXISTS `FUNC_RU` (
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

CREATE EXTERNAL TABLE IF NOT EXISTS `KEY_CONSTRAINTS`
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
  `DEFAULT_VALUE` string,
  CONSTRAINT `SYS_PK_KEY_CONSTRAINTS` PRIMARY KEY (`PARENT_TBL_ID`, `CONSTRAINT_NAME`, `POSITION`) DISABLE
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
  \"ENABLE_VALIDATE_RELY\",
  \"DEFAULT_VALUE\"
FROM
  \"KEY_CONSTRAINTS\""
);

CREATE OR REPLACE VIEW `TABLE_STATS_VIEW` AS
SELECT
  `TBL_ID`,
  max(CASE `PARAM_KEY` WHEN 'COLUMN_STATS_ACCURATE' THEN `PARAM_VALUE` END) AS COLUMN_STATS_ACCURATE,
  max(CASE `PARAM_KEY` WHEN 'numFiles' THEN `PARAM_VALUE` END) AS NUM_FILES,
  max(CASE `PARAM_KEY` WHEN 'numRows' THEN `PARAM_VALUE` END) AS NUM_ROWS,
  max(CASE `PARAM_KEY` WHEN 'rawDataSize' THEN `PARAM_VALUE` END) AS RAW_DATA_SIZE,
  max(CASE `PARAM_KEY` WHEN 'totalSize' THEN `PARAM_VALUE` END) AS TOTAL_SIZE,
  max(CASE `PARAM_KEY` WHEN 'transient_lastDdlTime' THEN `PARAM_VALUE` END) AS TRANSIENT_LAST_DDL_TIME
FROM `TABLE_PARAMS` GROUP BY `TBL_ID`;

CREATE OR REPLACE VIEW `PARTITION_STATS_VIEW` AS
SELECT
  `PART_ID`,
  max(CASE `PARAM_KEY` WHEN 'COLUMN_STATS_ACCURATE' THEN `PARAM_VALUE` END) AS COLUMN_STATS_ACCURATE,
  max(CASE `PARAM_KEY` WHEN 'numFiles' THEN `PARAM_VALUE` END) AS NUM_FILES,
  max(CASE `PARAM_KEY` WHEN 'numRows' THEN `PARAM_VALUE` END) AS NUM_ROWS,
  max(CASE `PARAM_KEY` WHEN 'rawDataSize' THEN `PARAM_VALUE` END) AS RAW_DATA_SIZE,
  max(CASE `PARAM_KEY` WHEN 'totalSize' THEN `PARAM_VALUE` END) AS TOTAL_SIZE,
  max(CASE `PARAM_KEY` WHEN 'transient_lastDdlTime' THEN `PARAM_VALUE` END) AS TRANSIENT_LAST_DDL_TIME
FROM `PARTITION_PARAMS` GROUP BY `PART_ID`;

CREATE EXTERNAL TABLE IF NOT EXISTS `WM_RESOURCEPLANS` (
  `NAME` string,
  `NS` string,
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
  case when \"WM_RESOURCEPLAN\".\"NS\" is null then 'default' else \"WM_RESOURCEPLAN\".\"NS\" end AS NS,
  \"STATUS\",
  \"WM_RESOURCEPLAN\".\"QUERY_PARALLELISM\",
  \"WM_POOL\".\"PATH\"
FROM
  \"WM_RESOURCEPLAN\" LEFT OUTER JOIN \"WM_POOL\" ON \"WM_RESOURCEPLAN\".\"DEFAULT_POOL_ID\" = \"WM_POOL\".\"POOL_ID\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `WM_TRIGGERS` (
  `RP_NAME` string,
  `NS` string,
  `NAME` string,
  `TRIGGER_EXPRESSION` string,
  `ACTION_EXPRESSION` string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  r.\"NAME\" AS RP_NAME,
  case when r.\"NS\" is null then 'default' else r.\"NS\" end,
  t.\"NAME\" AS NAME,
  \"TRIGGER_EXPRESSION\",
  \"ACTION_EXPRESSION\"
FROM
  \"WM_TRIGGER\" t
JOIN
  \"WM_RESOURCEPLAN\" r
ON
  t.\"RP_ID\" = r.\"RP_ID\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `WM_POOLS` (
  `RP_NAME` string,
  `NS` string,
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
  \"WM_RESOURCEPLAN\".\"NAME\",
  case when \"WM_RESOURCEPLAN\".\"NS\" is null then 'default' else \"WM_RESOURCEPLAN\".\"NS\" end AS NS,
  \"WM_POOL\".\"PATH\",
  \"WM_POOL\".\"ALLOC_FRACTION\",
  \"WM_POOL\".\"QUERY_PARALLELISM\",
  \"WM_POOL\".\"SCHEDULING_POLICY\"
FROM
  \"WM_POOL\"
JOIN
  \"WM_RESOURCEPLAN\"
ON
  \"WM_POOL\".\"RP_ID\" = \"WM_RESOURCEPLAN\".\"RP_ID\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `WM_POOLS_TO_TRIGGERS` (
  `RP_NAME` string,
  `NS` string,
  `POOL_PATH` string,
  `TRIGGER_NAME` string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"WM_RESOURCEPLAN\".\"NAME\" AS RP_NAME,
  case when \"WM_RESOURCEPLAN\".\"NS\" is null then 'default' else \"WM_RESOURCEPLAN\".\"NS\" end AS NS,
  \"WM_POOL\".\"PATH\" AS POOL_PATH,
  \"WM_TRIGGER\".\"NAME\" AS TRIGGER_NAME
FROM \"WM_POOL_TO_TRIGGER\"
  JOIN \"WM_POOL\" ON \"WM_POOL_TO_TRIGGER\".\"POOL_ID\" = \"WM_POOL\".\"POOL_ID\"
  JOIN \"WM_TRIGGER\" ON \"WM_POOL_TO_TRIGGER\".\"TRIGGER_ID\" = \"WM_TRIGGER\".\"TRIGGER_ID\"
  JOIN \"WM_RESOURCEPLAN\" ON \"WM_POOL\".\"RP_ID\" = \"WM_RESOURCEPLAN\".\"RP_ID\"
UNION
SELECT
  \"WM_RESOURCEPLAN\".\"NAME\" AS RP_NAME,
  case when \"WM_RESOURCEPLAN\".\"NS\" is null then 'default' else \"WM_RESOURCEPLAN\".\"NS\" end AS NS,
  '<unmanaged queries>' AS POOL_PATH,
  \"WM_TRIGGER\".\"NAME\" AS TRIGGER_NAME
FROM \"WM_TRIGGER\"
  JOIN \"WM_RESOURCEPLAN\" ON \"WM_TRIGGER\".\"RP_ID\" = \"WM_RESOURCEPLAN\".\"RP_ID\"
WHERE CAST(\"WM_TRIGGER\".\"IS_IN_UNMANAGED\" AS CHAR) IN ('1', 't')
"
);

CREATE EXTERNAL TABLE IF NOT EXISTS `WM_MAPPINGS` (
  `RP_NAME` string,
  `NS` string,
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
  \"WM_RESOURCEPLAN\".\"NAME\",
  case when \"WM_RESOURCEPLAN\".\"NS\" is null then 'default' else \"WM_RESOURCEPLAN\".\"NS\" end AS NS,
  \"ENTITY_TYPE\",
  \"ENTITY_NAME\",
  case when \"WM_POOL\".\"PATH\" is null then '<unmanaged>' else \"WM_POOL\".\"PATH\" end,
  \"ORDERING\"
FROM \"WM_MAPPING\"
JOIN \"WM_RESOURCEPLAN\" ON \"WM_MAPPING\".\"RP_ID\" = \"WM_RESOURCEPLAN\".\"RP_ID\"
LEFT OUTER JOIN \"WM_POOL\" ON \"WM_POOL\".\"POOL_ID\" = \"WM_MAPPING\".\"POOL_ID\"
"
);

CREATE EXTERNAL TABLE IF NOT EXISTS `COMPACTION_QUEUE` (
  `CQ_ID` bigint,
  `CQ_DATABASE` string,
  `CQ_TABLE` string,
  `CQ_PARTITION` string,
  `CQ_STATE` string,
  `CQ_TYPE` string,
  `CQ_TBLPROPERTIES` string,
  `CQ_WORKER_ID` string,
  `CQ_ENQUEUE_TIME` bigint,
  `CQ_START` bigint,
  `CQ_RUN_AS` string,
  `CQ_HIGHEST_WRITE_ID` bigint,
  `CQ_HADOOP_JOB_ID` string,
  `CQ_ERROR_MESSAGE` string,
  `CQ_NEXT_TXN_ID` bigint,
  `CQ_TXN_ID` bigint,
  `CQ_COMMIT_TIME` bigint,
  `CQ_INITIATOR_ID` string,
  `CQ_INITIATOR_VERSION` string,
  `CQ_WORKER_VERSION` string,
  `CQ_CLEANER_START` bigint,
  `CQ_POOL_NAME` string,
  `CQ_NUMBER_OF_BUCKETS` string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"COMPACTION_QUEUE\".\"CQ_ID\",
  \"COMPACTION_QUEUE\".\"CQ_DATABASE\",
  \"COMPACTION_QUEUE\".\"CQ_TABLE\",
  \"COMPACTION_QUEUE\".\"CQ_PARTITION\",
  \"COMPACTION_QUEUE\".\"CQ_STATE\",
  \"COMPACTION_QUEUE\".\"CQ_TYPE\",
  \"COMPACTION_QUEUE\".\"CQ_TBLPROPERTIES\",
  \"COMPACTION_QUEUE\".\"CQ_WORKER_ID\",
  \"COMPACTION_QUEUE\".\"CQ_ENQUEUE_TIME\",
  \"COMPACTION_QUEUE\".\"CQ_START\",
  \"COMPACTION_QUEUE\".\"CQ_RUN_AS\",
  \"COMPACTION_QUEUE\".\"CQ_HIGHEST_WRITE_ID\",
  \"COMPACTION_QUEUE\".\"CQ_HADOOP_JOB_ID\",
  \"COMPACTION_QUEUE\".\"CQ_ERROR_MESSAGE\",
  \"COMPACTION_QUEUE\".\"CQ_NEXT_TXN_ID\",
  \"COMPACTION_QUEUE\".\"CQ_TXN_ID\",
  \"COMPACTION_QUEUE\".\"CQ_COMMIT_TIME\",
  \"COMPACTION_QUEUE\".\"CQ_INITIATOR_ID\",
  \"COMPACTION_QUEUE\".\"CQ_INITIATOR_VERSION\",
  \"COMPACTION_QUEUE\".\"CQ_WORKER_VERSION\",
  \"COMPACTION_QUEUE\".\"CQ_CLEANER_START\",
  \"COMPACTION_QUEUE\".\"CQ_POOL_NAME\",
  \"COMPACTION_QUEUE\".\"CQ_NUMBER_OF_BUCKETS\"
FROM \"COMPACTION_QUEUE\"
"
);

CREATE EXTERNAL TABLE IF NOT EXISTS `COMPLETED_COMPACTIONS` (
  `CC_ID` bigint,
  `CC_DATABASE` string,
  `CC_TABLE` string,
  `CC_PARTITION` string,
  `CC_STATE` string,
  `CC_TYPE` string,
  `CC_TBLPROPERTIES` string,
  `CC_WORKER_ID` string,
  `CC_ENQUEUE_TIME` bigint,
  `CC_START` bigint,
  `CC_END` bigint,
  `CC_RUN_AS` string,
  `CC_HIGHEST_WRITE_ID` bigint,
  `CC_HADOOP_JOB_ID` string,
  `CC_ERROR_MESSAGE` string,
  `CC_NEXT_TXN_ID` bigint,
  `CC_TXN_ID` bigint,
  `CC_COMMIT_TIME` bigint,
  `CC_INITIATOR_ID` string,
  `CC_INITIATOR_VERSION` string,
  `CC_WORKER_VERSION` string,
  `CC_POOL_NAME` string,
  `CC_NUMBER_OF_BUCKETS` string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"COMPLETED_COMPACTIONS\".\"CC_ID\",
  \"COMPLETED_COMPACTIONS\".\"CC_DATABASE\",
  \"COMPLETED_COMPACTIONS\".\"CC_TABLE\",
  \"COMPLETED_COMPACTIONS\".\"CC_PARTITION\",
  \"COMPLETED_COMPACTIONS\".\"CC_STATE\",
  \"COMPLETED_COMPACTIONS\".\"CC_TYPE\",
  \"COMPLETED_COMPACTIONS\".\"CC_TBLPROPERTIES\",
  \"COMPLETED_COMPACTIONS\".\"CC_WORKER_ID\",
  \"COMPLETED_COMPACTIONS\".\"CC_ENQUEUE_TIME\",
  \"COMPLETED_COMPACTIONS\".\"CC_START\",
  \"COMPLETED_COMPACTIONS\".\"CC_END\",
  \"COMPLETED_COMPACTIONS\".\"CC_RUN_AS\",
  \"COMPLETED_COMPACTIONS\".\"CC_HIGHEST_WRITE_ID\",
  \"COMPLETED_COMPACTIONS\".\"CC_HADOOP_JOB_ID\",
  \"COMPLETED_COMPACTIONS\".\"CC_ERROR_MESSAGE\",
  \"COMPLETED_COMPACTIONS\".\"CC_NEXT_TXN_ID\",
  \"COMPLETED_COMPACTIONS\".\"CC_TXN_ID\",
  \"COMPLETED_COMPACTIONS\".\"CC_COMMIT_TIME\",
  \"COMPLETED_COMPACTIONS\".\"CC_INITIATOR_ID\",
  \"COMPLETED_COMPACTIONS\".\"CC_INITIATOR_VERSION\",
  \"COMPLETED_COMPACTIONS\".\"CC_WORKER_VERSION\",
  \"COMPLETED_COMPACTIONS\".\"CC_POOL_NAME\",
  \"COMPLETED_COMPACTIONS\".\"CC_NUMBER_OF_BUCKETS\"
FROM \"COMPLETED_COMPACTIONS\"
"
);

CREATE OR REPLACE VIEW `COMPACTIONS`
(
  `C_ID`,
  `C_CATALOG`,
  `C_DATABASE`,
  `C_TABLE`,
  `C_PARTITION`,
  `C_TYPE`,
  `C_STATE`,
  `C_WORKER_HOST`,
  `C_WORKER_ID`,
  `C_WORKER_VERSION`,
  `C_ENQUEUE_TIME`,
  `C_START`,
  `C_DURATION`,
  `C_HADOOP_JOB_ID`,
  `C_RUN_AS`,
  `C_ERROR_MESSAGE`,
  `C_NEXT_TXN_ID`,
  `C_TXN_ID`,
  `C_COMMIT_TIME`,
  `C_HIGHEST_WRITE_ID`,
  `C_INITIATOR_HOST`,
  `C_INITIATOR_ID`,
  `C_INITIATOR_VERSION`,
  `C_CLEANER_START`,
  `C_POOL_NAME`,
  `C_NUMBER_OF_BUCKETS`,
  `C_TBLPROPERTIES`
) AS
SELECT
  CC_ID,
  'default',
  CC_DATABASE,
  CC_TABLE,
  CC_PARTITION,
  CASE WHEN CC_TYPE = 'i' THEN 'minor' WHEN CC_TYPE = 'a' THEN 'major' ELSE 'UNKNOWN' END,
  CASE WHEN CC_STATE = 'f' THEN 'failed' WHEN CC_STATE = 's' THEN 'succeeded'
    WHEN CC_STATE = 'a' THEN 'did not initiate' WHEN CC_STATE = 'c' THEN 'refused' ELSE 'UNKNOWN' END,
  CASE WHEN CC_WORKER_ID IS NULL THEN cast (null as string) ELSE split(CC_WORKER_ID,"-")[0] END,
  CASE WHEN CC_WORKER_ID IS NULL THEN cast (null as string) ELSE split(CC_WORKER_ID,"-")[size(split(CC_WORKER_ID,"-"))-1] END,
  CC_WORKER_VERSION,
  FROM_UNIXTIME(CC_ENQUEUE_TIME DIV 1000),
  FROM_UNIXTIME(CC_START DIV 1000),
  CASE WHEN CC_END IS NULL THEN cast (null as string) ELSE CC_END-CC_START END,
  CC_HADOOP_JOB_ID,
  CC_RUN_AS,
  CC_ERROR_MESSAGE,
  CC_NEXT_TXN_ID,
  CC_TXN_ID,
  FROM_UNIXTIME(CC_COMMIT_TIME DIV 1000),
  CC_HIGHEST_WRITE_ID,
  CASE WHEN CC_INITIATOR_ID IS NULL THEN cast (null as string) ELSE split(CC_INITIATOR_ID,"-")[0] END,
  CASE WHEN CC_INITIATOR_ID IS NULL THEN cast (null as string) ELSE split(CC_INITIATOR_ID,"-")[size(split(CC_INITIATOR_ID,"-"))-1] END,
  CC_INITIATOR_VERSION,
  NULL,
  NVL(CC_POOL_NAME, 'default'),
  CC_NUMBER_OF_BUCKETS,
  CC_TBLPROPERTIES
FROM COMPLETED_COMPACTIONS
UNION ALL
SELECT
  CQ_ID,
  'default',
  CQ_DATABASE,
  CQ_TABLE,
  CQ_PARTITION,
  CASE WHEN CQ_TYPE = 'i' THEN 'minor' WHEN CQ_TYPE = 'a' THEN 'major' ELSE 'UNKNOWN' END,
  CASE WHEN CQ_STATE = 'i' THEN 'initiated' WHEN CQ_STATE = 'w' THEN 'working' WHEN CQ_STATE = 'r' THEN 'ready for cleaning' ELSE 'UNKNOWN' END,
  CASE WHEN CQ_WORKER_ID IS NULL THEN NULL ELSE split(CQ_WORKER_ID,"-")[0] END,
  CASE WHEN CQ_WORKER_ID IS NULL THEN NULL ELSE split(CQ_WORKER_ID,"-")[size(split(CQ_WORKER_ID,"-"))-1] END,
  CQ_WORKER_VERSION,
  FROM_UNIXTIME(CQ_ENQUEUE_TIME DIV 1000),
  FROM_UNIXTIME(CQ_START DIV 1000),
  cast (null as string),
  CQ_HADOOP_JOB_ID,
  CQ_RUN_AS,
  CQ_ERROR_MESSAGE,
  CQ_NEXT_TXN_ID,
  CQ_TXN_ID,
  FROM_UNIXTIME(CQ_COMMIT_TIME DIV 1000),
  CQ_HIGHEST_WRITE_ID,
  CASE WHEN CQ_INITIATOR_ID IS NULL THEN NULL ELSE split(CQ_INITIATOR_ID,"-")[0] END,
  CASE WHEN CQ_INITIATOR_ID IS NULL THEN NULL ELSE split(CQ_INITIATOR_ID,"-")[size(split(CQ_INITIATOR_ID,"-"))-1] END,
  CQ_INITIATOR_VERSION,
  FROM_UNIXTIME(CQ_CLEANER_START DIV 1000),
  NVL(CQ_POOL_NAME, 'default'),
  CQ_NUMBER_OF_BUCKETS,
  CQ_TBLPROPERTIES
FROM COMPACTION_QUEUE;

CREATE EXTERNAL TABLE IF NOT EXISTS `SCHEDULED_QUERIES` (
  `SCHEDULED_QUERY_ID` bigint,
  `SCHEDULE_NAME` string,
  `ENABLED` boolean,
  `CLUSTER_NAMESPACE` string,
  `SCHEDULE` string,
  `USER` string,
  `QUERY` string,
  `NEXT_EXECUTION` bigint,
  `ACTIVE_EXECUTION_ID` bigint,
  CONSTRAINT `SYS_PK_SCHEDULED_QUERIES` PRIMARY KEY (`SCHEDULED_QUERY_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SCHEDULED_QUERY_ID\",
  \"SCHEDULE_NAME\",
  \"ENABLED\",
  \"CLUSTER_NAMESPACE\",
  \"SCHEDULE\",
  \"USER\",
  \"QUERY\",
  \"NEXT_EXECUTION\",
  \"ACTIVE_EXECUTION_ID\"
FROM
  \"SCHEDULED_QUERIES\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `SCHEDULED_EXECUTIONS` (
  `SCHEDULED_EXECUTION_ID` bigint,
  `SCHEDULED_QUERY_ID` bigint,
  `EXECUTOR_QUERY_ID` string,
  `STATE` string,
  `START_TIME` int,
  `END_TIME` int,
  `ERROR_MESSAGE` string,
  `LAST_UPDATE_TIME` int,
  CONSTRAINT `SYS_PK_SCHEDULED_EXECUTIONS` PRIMARY KEY (`SCHEDULED_EXECUTION_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SCHEDULED_EXECUTION_ID\",
  \"SCHEDULED_QUERY_ID\",
  \"EXECUTOR_QUERY_ID\",
  \"STATE\",
  \"START_TIME\",
  \"END_TIME\",
  \"ERROR_MESSAGE\",
  \"LAST_UPDATE_TIME\"
FROM
  \"SCHEDULED_EXECUTIONS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `TXNS` (
    `TXN_ID` bigint,
    `TXN_STATE` string,
    `TXN_STARTED` bigint,
    `TXN_LAST_HEARTBEAT` bigint,
    `TXN_USER` string,
    `TXN_HOST` string,
    `TXN_AGENT_INFO` string,
    `TXN_META_INFO` string,
    `TXN_HEARTBEAT_COUNT` int,
    `TXN_TYPE` int
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
    \"TXN_ID\",
    \"TXN_STATE\",
    \"TXN_STARTED\",
    \"TXN_LAST_HEARTBEAT\",
    \"TXN_USER\",
    \"TXN_HOST\",
    \"TXN_AGENT_INFO\",
    \"TXN_META_INFO\",
    \"TXN_HEARTBEAT_COUNT\",
    \"TXN_TYPE\"
FROM \"TXNS\""
);


CREATE EXTERNAL TABLE IF NOT EXISTS `TXN_COMPONENTS` (
    `TC_TXNID` bigint,
    `TC_DATABASE` string,
    `TC_TABLE` string,
    `TC_PARTITION` string,
    `TC_OPERATION_TYPE` string,
    `TC_WRITEID` bigint
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
    \"TC_TXNID\",
    \"TC_DATABASE\",
    \"TC_TABLE\",
    \"TC_PARTITION\",
    \"TC_OPERATION_TYPE\",
    \"TC_WRITEID\"
FROM \"TXN_COMPONENTS\""
);


CREATE OR REPLACE VIEW `TRANSACTIONS` (
    `TXN_ID`,
    `STATE`,
    `STARTED`,
    `LAST_HEARTBEAT`,
    `USER`,
    `HOST`,
    `AGENT_INFO`,
    `META_INFO`,
    `HEARTBEAT_COUNT`,
    `TYPE`,
    `TC_DATABASE`,
    `TC_TABLE`,
    `TC_PARTITION`,
    `TC_OPERATION_TYPE`,
    `TC_WRITEID`
) AS
SELECT DISTINCT
    T.`TXN_ID`,
    CASE WHEN T.`TXN_STATE` = 'o' THEN 'open' WHEN T.`TXN_STATE` = 'a' THEN 'aborted' WHEN T.`TXN_STATE` = 'c' THEN 'commited' ELSE 'UNKNOWN' END  AS TXN_STATE,
    FROM_UNIXTIME(T.`TXN_STARTED` DIV 1000) AS TXN_STARTED,
    FROM_UNIXTIME(T.`TXN_LAST_HEARTBEAT` DIV 1000) AS TXN_LAST_HEARTBEAT,
    T.`TXN_USER`,
    T.`TXN_HOST`,
    T.`TXN_AGENT_INFO`,
    T.`TXN_META_INFO`,
    T.`TXN_HEARTBEAT_COUNT`,
    CASE WHEN T.`TXN_TYPE` = 0 THEN 'DEFAULT' WHEN T.`TXN_TYPE` = 1 THEN 'REPL_CREATED' WHEN T.`TXN_TYPE` = 2 THEN 'READ_ONLY' WHEN T.`TXN_TYPE` = 3 THEN 'COMPACTION' END AS TXN_TYPE,
    TC.`TC_DATABASE`,
    TC.`TC_TABLE`,
    TC.`TC_PARTITION`,
    CASE WHEN TC.`TC_OPERATION_TYPE` = 's' THEN 'SELECT' WHEN TC.`TC_OPERATION_TYPE` = 'i' THEN 'INSERT' WHEN TC.`TC_OPERATION_TYPE` = 'u' THEN 'UPDATE' WHEN TC.`TC_OPERATION_TYPE` = 'c' THEN 'COMPACT' END AS OPERATION_TYPE,
    TC.`TC_WRITEID`
FROM `SYS`.`TXNS` AS T
LEFT JOIN `SYS`.`TXN_COMPONENTS` AS TC ON T.`TXN_ID` = TC.`TC_TXNID`;

CREATE EXTERNAL TABLE `HIVE_LOCKS` (
    `HL_LOCK_EXT_ID` bigint,
    `HL_LOCK_INT_ID` bigint,
    `HL_TXNID` bigint,
    `HL_DB` string,
    `HL_TABLE` string,
    `HL_PARTITION` string,
    `HL_LOCK_STATE` string,
    `HL_LOCK_TYPE` string,
    `HL_LAST_HEARTBEAT` bigint,
    `HL_ACQUIRED_AT` bigint,
    `HL_USER` string,
    `HL_HOST` string,
    `HL_HEARTBEAT_COUNT` int,
    `HL_AGENT_INFO` string,
    `HL_BLOCKEDBY_EXT_ID` bigint,
    `HL_BLOCKEDBY_INT_ID` bigint
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
    \"HL_LOCK_EXT_ID\",
    \"HL_LOCK_INT_ID\",
    \"HL_TXNID\",
    \"HL_DB\",
    \"HL_TABLE\",
    \"HL_PARTITION\",
    \"HL_LOCK_STATE\",
    \"HL_LOCK_TYPE\",
    \"HL_LAST_HEARTBEAT\",
    \"HL_ACQUIRED_AT\",
    \"HL_USER\",
    \"HL_HOST\",
    \"HL_HEARTBEAT_COUNT\",
    \"HL_AGENT_INFO\",
    \"HL_BLOCKEDBY_EXT_ID\",
    \"HL_BLOCKEDBY_INT_ID\"
FROM \"HIVE_LOCKS\""
);

CREATE OR REPLACE VIEW `LOCKS` (
    `LOCK_EXT_ID`,
    `LOCK_INT_ID`,
    `TXNID`,
    `DB`,
    `TABLE`,
    `PARTITION`,
    `LOCK_STATE`,
    `LOCK_TYPE`,
    `LAST_HEARTBEAT`,
    `ACQUIRED_AT`,
    `USER`,
    `HOST`,
    `HEARTBEAT_COUNT`,
    `AGENT_INFO`,
    `BLOCKEDBY_EXT_ID`,
    `BLOCKEDBY_INT_ID`
) AS
SELECT DISTINCT
    HL.`HL_LOCK_EXT_ID`,
    HL.`HL_LOCK_INT_ID`,
    HL.`HL_TXNID`,
    HL.`HL_DB`,
    HL.`HL_TABLE`,
    HL.`HL_PARTITION`,
    CASE WHEN HL.`HL_LOCK_STATE` = 'a' THEN 'acquired' WHEN HL.`HL_LOCK_STATE` = 'w' THEN 'waiting' END AS LOCK_STATE,
    CASE WHEN HL.`HL_LOCK_TYPE` = 'e' THEN 'exclusive' WHEN HL.`HL_LOCK_TYPE` = 'x' THEN 'excl_write' WHEN HL.`HL_LOCK_TYPE` = 'r' THEN 'shared_read' WHEN HL.`HL_LOCK_TYPE` = 'w' THEN 'shared_write' END AS LOCK_TYPE,
    FROM_UNIXTIME(HL.`HL_LAST_HEARTBEAT`),
    FROM_UNIXTIME(HL.`HL_ACQUIRED_AT`),
    HL.`HL_USER`,
    HL.`HL_HOST`,
    HL.`HL_HEARTBEAT_COUNT`,
    HL.`HL_AGENT_INFO`,
    HL.`HL_BLOCKEDBY_EXT_ID`,
    HL.`HL_BLOCKEDBY_INT_ID`
FROM SYS.`HIVE_LOCKS` AS HL;

CREATE EXTERNAL TABLE IF NOT EXISTS `MIN_HISTORY_LEVEL` (
    `MHL_TXNID` bigint,
    `MHL_MIN_OPEN_TXNID` bigint
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
    \"MHL_TXNID\",
    \"MHL_MIN_OPEN_TXNID\",
FROM \"MIN_HISTORY_LEVEL\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `REPLICATION_METRICS_ORIG` (
    `SCHEDULED_EXECUTION_ID` bigint,
    `POLICY_NAME` string,
    `DUMP_EXECUTION_ID` bigint,
    `METADATA` string,
    `PROGRESS` string,
    `MESSAGE_FORMAT` varchar(16)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
    \"RM_SCHEDULED_EXECUTION_ID\",
    \"RM_POLICY\",
    \"RM_DUMP_EXECUTION_ID\",
    \"RM_METADATA\",
    \"RM_PROGRESS\",
    \"MESSAGE_FORMAT\"
FROM \"REPLICATION_METRICS\""
);

CREATE OR REPLACE VIEW `REPLICATION_METRICS` (
    `SCHEDULED_EXECUTION_ID`,
    `POLICY_NAME`,
    `DUMP_EXECUTION_ID`,
    `METADATA`,
    `PROGRESS`
) AS
SELECT DISTINCT
    RM.`SCHEDULED_EXECUTION_ID`,
    RM.`POLICY_NAME`,
    RM.`DUMP_EXECUTION_ID`,
    RM.`METADATA`,
    deserialize(RM.`PROGRESS`, RM.`MESSAGE_FORMAT`)
FROM SYS.`REPLICATION_METRICS_ORIG` AS RM;

CREATE EXTERNAL TABLE IF NOT EXISTS `NOTIFICATION_LOG` (
  `NL_ID` bigint,
  `EVENT_ID` bigint,
  `EVENT_TIME` int,
  `EVENT_TYPE` varchar(32),
  `CAT_NAME` varchar(256),
  `DB_NAME` varchar(128),
  `TBL_NAME` varchar(256),
  `MESSAGE` string,
  `MESSAGE_FORMAT` varchar(16)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
    \"NL_ID\",
    \"EVENT_ID\",
    \"EVENT_TIME\",
    \"EVENT_TYPE\",
    \"CAT_NAME\",
    \"DB_NAME\",
    \"TBL_NAME\",
    \"MESSAGE\",
    \"MESSAGE_FORMAT\"
FROM \"NOTIFICATION_LOG\""
);

CREATE OR REPLACE VIEW `REPLICATION_FAILOVER_FAILBACK_METRICS` (
    `DB_NAME`,
    `DB_REPL_METRICS_NAME`,
    `DB_REPL_METRICS_VALUE`
) AS
SELECT DISTINCT
    A.`NAME`,
    B.`PARAM_KEY`,
    B.`PARAM_VALUE`
FROM SYS.DBS A
JOIN SYS.DATABASE_PARAMS B
WHERE
    A.DB_ID=B.DB_ID
    AND
    B.PARAM_KEY LIKE 'repl_metrics%';


CREATE DATABASE IF NOT EXISTS INFORMATION_SCHEMA;

USE INFORMATION_SCHEMA;


CREATE OR REPLACE VIEW `SCHEMATA`
(
  `CATALOG_NAME`,
  `SCHEMA_NAME`,
  `SCHEMA_OWNER`,
  `DEFAULT_CHARACTER_SET_CATALOG`,
  `DEFAULT_CHARACTER_SET_SCHEMA`,
  `DEFAULT_CHARACTER_SET_NAME`,
  `SQL_PATH`
) AS
SELECT DISTINCT
  'default',
  D.`NAME`,
  D.`OWNER_NAME`,
  cast(null as string),
  cast(null as string),
  cast(null as string),
  `DB_LOCATION_URI`
FROM
  `sys`.`DBS` D LEFT JOIN `sys`.`TBLS` T ON (D.`DB_ID` = T.`DB_ID`)
                LEFT JOIN `sys`.`TBL_PRIVS` P ON (T.`TBL_ID` = P.`TBL_ID`)
WHERE
  NOT restrict_information_schema() OR P.`TBL_ID` IS NOT NULL
  AND (P.`PRINCIPAL_NAME`=current_user() AND P.`PRINCIPAL_TYPE`='USER'
    OR ((array_contains(current_groups(), P.`PRINCIPAL_NAME`) OR P.`PRINCIPAL_NAME` = 'public') AND P.`PRINCIPAL_TYPE`='GROUP'))
  AND current_authorizer() = P.`AUTHORIZER`;

CREATE OR REPLACE VIEW `TABLES`
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
SELECT DISTINCT
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
  `sys`.`TBLS` T JOIN `sys`.`DBS` D ON (D.`DB_ID` = T.`DB_ID`)
                 LEFT JOIN `sys`.`TBL_PRIVS` P ON (T.`TBL_ID` = P.`TBL_ID`)
WHERE
  NOT restrict_information_schema() OR P.`TBL_ID` IS NOT NULL
  AND (P.`PRINCIPAL_NAME`=current_user() AND P.`PRINCIPAL_TYPE`='USER'
    OR ((array_contains(current_groups(), P.`PRINCIPAL_NAME`) OR P.`PRINCIPAL_NAME` = 'public') AND P.`PRINCIPAL_TYPE`='GROUP'))
  AND P.`TBL_PRIV`='SELECT' AND P.`AUTHORIZER`=current_authorizer();

CREATE OR REPLACE VIEW `TABLE_PRIVILEGES`
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
SELECT DISTINCT
  P.`GRANTOR`,
  P.`PRINCIPAL_NAME`,
  'default',
  D.`NAME`,
  T.`TBL_NAME`,
  P.`TBL_PRIV`,
  IF (P.`GRANT_OPTION` == 0, 'NO', 'YES'),
  'NO'
FROM
  `sys`.`TBL_PRIVS` P JOIN `sys`.`TBLS` T ON (P.`TBL_ID` = T.`TBL_ID`)
                      JOIN `sys`.`DBS` D ON (T.`DB_ID` = D.`DB_ID`)
                      LEFT JOIN `sys`.`TBL_PRIVS` P2 ON (P.`TBL_ID` = P2.`TBL_ID`)
WHERE
  NOT restrict_information_schema() OR
  (P2.`TBL_ID` IS NOT NULL AND P.`PRINCIPAL_NAME` = P2.`PRINCIPAL_NAME` AND P.`PRINCIPAL_TYPE` = P2.`PRINCIPAL_TYPE`
  AND (P2.`PRINCIPAL_NAME`=current_user() AND P2.`PRINCIPAL_TYPE`='USER'
    OR ((array_contains(current_groups(), P2.`PRINCIPAL_NAME`) OR P2.`PRINCIPAL_NAME` = 'public') AND P2.`PRINCIPAL_TYPE`='GROUP'))
  AND P2.`TBL_PRIV`='SELECT' AND P.`AUTHORIZER` = current_authorizer() AND P2.`AUTHORIZER` = current_authorizer());

CREATE OR REPLACE VIEW `COLUMNS`
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
SELECT DISTINCT
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
  `sys`.`COLUMNS_V2` C JOIN `sys`.`SDS` S ON (C.`CD_ID` = S.`CD_ID`)
                       JOIN `sys`.`TBLS` T ON (S.`SD_ID` = T.`SD_ID`)
                       JOIN `sys`.`DBS` D ON (T.`DB_ID` = D.`DB_ID`)
                       LEFT JOIN `sys`.`TBL_COL_PRIVS` P ON (T.`TBL_ID` = P.`TBL_ID`)
WHERE
  NOT restrict_information_schema() OR P.`TBL_ID` IS NOT NULL
  AND C.`COLUMN_NAME` = P.`COLUMN_NAME`
  AND (P.`PRINCIPAL_NAME`=current_user() AND P.`PRINCIPAL_TYPE`='USER'
    OR ((array_contains(current_groups(), P.`PRINCIPAL_NAME`) OR P.`PRINCIPAL_NAME` = 'public') AND P.`PRINCIPAL_TYPE`='GROUP'))
  AND array_contains(split_map_privs(P.`TBL_COL_PRIV`),"SELECT") AND P.`AUTHORIZER`=current_authorizer();


CREATE OR REPLACE VIEW `COLUMN_PRIVILEGES`
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
SELECT DISTINCT
  P.`GRANTOR`,
  P.`PRINCIPAL_NAME`,
  'default',
  D.`NAME`,
  T.`TBL_NAME`,
  P.`COLUMN_NAME`,
  P.`TBL_COL_PRIV`,
  IF (P.`GRANT_OPTION` == 0, 'NO', 'YES')
FROM
  (SELECT
        Q.`GRANTOR`,
        Q.`GRANT_OPTION`,
        Q.`PRINCIPAL_NAME`,
        Q.`PRINCIPAL_TYPE`,
        Q.`AUTHORIZER`,
        Q.`COLUMN_NAME`,
        `TBL_COL_PRIV_TMP`.`TBL_COL_PRIV`,
        Q.`TBL_ID`
       FROM `sys`.`TBL_COL_PRIVS` AS Q
       LATERAL VIEW explode(split_map_privs(Q.`TBL_COL_PRIV`)) `TBL_COL_PRIV_TMP` AS `TBL_COL_PRIV`) P
                          JOIN `sys`.`TBLS` T ON (P.`TBL_ID` = T.`TBL_ID`)
                          JOIN `sys`.`DBS` D ON (T.`DB_ID` = D.`DB_ID`)
                          JOIN `sys`.`SDS` S ON (S.`SD_ID` = T.`SD_ID`)
                          LEFT JOIN `sys`.`TBL_PRIVS` P2 ON (P.`TBL_ID` = P2.`TBL_ID`)
WHERE
  NOT restrict_information_schema() OR P2.`TBL_ID` IS NOT NULL
  AND P.`PRINCIPAL_NAME` = P2.`PRINCIPAL_NAME` AND P.`PRINCIPAL_TYPE` = P2.`PRINCIPAL_TYPE`
  AND (P2.`PRINCIPAL_NAME`=current_user() AND P2.`PRINCIPAL_TYPE`='USER'
    OR ((array_contains(current_groups(), P2.`PRINCIPAL_NAME`) OR P2.`PRINCIPAL_NAME` = 'public') AND P2.`PRINCIPAL_TYPE`='GROUP'))
  AND P2.`TBL_PRIV`='SELECT' AND P.`AUTHORIZER`=current_authorizer() AND P2.`AUTHORIZER`=current_authorizer();

CREATE OR REPLACE VIEW `VIEWS`
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
SELECT DISTINCT
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
  `sys`.`DBS` D JOIN `sys`.`TBLS` T ON (D.`DB_ID` = T.`DB_ID`)
                LEFT JOIN `sys`.`TBL_PRIVS` P ON (T.`TBL_ID` = P.`TBL_ID`)
WHERE
  length(T.VIEW_ORIGINAL_TEXT) > 0
  AND (NOT restrict_information_schema() OR P.`TBL_ID` IS NOT NULL
  AND (P.`PRINCIPAL_NAME`=current_user() AND P.`PRINCIPAL_TYPE`='USER'
    OR ((array_contains(current_groups(), P.`PRINCIPAL_NAME`) OR P.`PRINCIPAL_NAME` = 'public') AND P.`PRINCIPAL_TYPE`='GROUP'))
  AND P.`TBL_PRIV`='SELECT' AND P.`AUTHORIZER`=current_authorizer());

CREATE OR REPLACE VIEW `COMPACTIONS`
(
  `C_ID`,
  `C_CATALOG`,
  `C_DATABASE`,
  `C_TABLE`,
  `C_PARTITION`,
  `C_TYPE`,
  `C_STATE`,
  `C_WORKER_HOST`,
  `C_WORKER_ID`,
  `C_WORKER_VERSION`,
  `C_ENQUEUE_TIME`,
  `C_START`,
  `C_DURATION`,
  `C_HADOOP_JOB_ID`,
  `C_RUN_AS`,
  `C_ERROR_MESSAGE`,
  `C_NEXT_TXN_ID`,
  `C_TXN_ID`,
  `C_COMMIT_TIME`,
  `C_HIGHEST_WRITE_ID`,
  `C_INITIATOR_HOST`,
  `C_INITIATOR_ID`,
  `C_INITIATOR_VERSION`,
  `C_CLEANER_START`,
  `C_POOL_NAME`,
  `C_NUMBER_OF_BUCKETS`,
  `C_TBLPROPERTIES`
) AS
SELECT DISTINCT
  C_ID,
  C_CATALOG,
  C_DATABASE,
  C_TABLE,
  C_PARTITION,
  C_TYPE,
  C_STATE,
  C_WORKER_HOST,
  C_WORKER_ID,
  C_WORKER_VERSION,
  C_ENQUEUE_TIME,
  C_START,
  C_DURATION,
  C_HADOOP_JOB_ID,
  C_RUN_AS,
  C_ERROR_MESSAGE,
  C_NEXT_TXN_ID,
  C_TXN_ID,
  C_COMMIT_TIME,
  C_HIGHEST_WRITE_ID,
  C_INITIATOR_HOST,
  C_INITIATOR_ID,
  C_INITIATOR_VERSION,
  C_CLEANER_START,
  C_POOL_NAME,
  C_NUMBER_OF_BUCKETS,
  C_TBLPROPERTIES
FROM
  `sys`.`COMPACTIONS` C JOIN `sys`.`TBLS` T ON (C.`C_TABLE` = T.`TBL_NAME`)
                        JOIN `sys`.`DBS` D ON (C.`C_DATABASE` = D.`NAME`)
                        LEFT JOIN `sys`.`TBL_PRIVS` P ON (T.`TBL_ID` = P.`TBL_ID`)
WHERE
  (NOT restrict_information_schema() OR P.`TBL_ID` IS NOT NULL
  AND (P.`PRINCIPAL_NAME`=current_user() AND P.`PRINCIPAL_TYPE`='USER'
    OR ((array_contains(current_groups(), P.`PRINCIPAL_NAME`) OR P.`PRINCIPAL_NAME` = 'public') AND P.`PRINCIPAL_TYPE`='GROUP'))
  AND P.`TBL_PRIV`='SELECT' AND P.`AUTHORIZER`=current_authorizer());


create or replace view SCHEDULED_QUERIES  as
select
  `SCHEDULED_QUERY_ID` ,
  `SCHEDULE_NAME` ,
  `ENABLED`,
  `CLUSTER_NAMESPACE`,
  `SCHEDULE`,
  `USER`,
  `QUERY`,
  FROM_UNIXTIME(NEXT_EXECUTION) as NEXT_EXECUTION,
  `ACTIVE_EXECUTION_ID`
FROM
  SYS.SCHEDULED_QUERIES
;

create or replace view SCHEDULED_EXECUTIONS as
SELECT
  SCHEDULED_EXECUTION_ID,
  SCHEDULE_NAME,
  EXECUTOR_QUERY_ID,
  `STATE`,
  FROM_UNIXTIME(START_TIME) as START_TIME,
  FROM_UNIXTIME(END_TIME) as END_TIME,
  END_TIME-START_TIME as ELAPSED,
  ERROR_MESSAGE,
  FROM_UNIXTIME(LAST_UPDATE_TIME) AS LAST_UPDATE_TIME,
  START_TIME as START_TIME_SINCE_EPOCH
FROM
  SYS.SCHEDULED_EXECUTIONS SE
JOIN
  SYS.SCHEDULED_QUERIES SQ
WHERE
  SE.SCHEDULED_QUERY_ID=SQ.SCHEDULED_QUERY_ID;

CREATE OR REPLACE VIEW `TRANSACTIONS` (
  `TXN_ID`,
  `STATE`,
  `STARTED`,
  `LAST_HEARTBEAT`,
  `USER`,
  `HOST`,
  `AGENT_INFO`,
  `META_INFO`,
  `HEARTBEAT_COUNT`,
  `TYPE`,
  `TC_DATABASE`,
  `TC_TABLE`,
  `TC_PARTITION`,
  `TC_OPERATION_TYPE`,
  `TC_WRITEID`
) AS
SELECT DISTINCT
  `TXN_ID`,
  `STATE`,
  `STARTED`,
  `LAST_HEARTBEAT`,
  `USER`,
  `HOST`,
  `AGENT_INFO`,
  `META_INFO`,
  `HEARTBEAT_COUNT`,
  `TYPE`,
  `TC_DATABASE`,
  `TC_TABLE`,
  `TC_PARTITION`,
  `TC_OPERATION_TYPE`,
  `TC_WRITEID`
FROM `SYS`.`TRANSACTIONS` AS TXN JOIN `sys`.`TBLS` T ON (TXN.`TC_TABLE` = T.`TBL_NAME`)
                                 JOIN `sys`.`DBS` D ON (TXN.`TC_DATABASE` = D.`NAME`)
                                 LEFT JOIN `sys`.`TBL_PRIVS` P ON (T.`TBL_ID` = P.`TBL_ID`)
WHERE
    (NOT restrict_information_schema() OR P.`TBL_ID` IS NOT NULL
        AND (P.`PRINCIPAL_NAME`=current_user() AND P.`PRINCIPAL_TYPE`='USER'
            OR ((array_contains(current_groups(), P.`PRINCIPAL_NAME`) OR P.`PRINCIPAL_NAME` = 'public') AND P.`PRINCIPAL_TYPE`='GROUP'))
        AND P.`TBL_PRIV`='SELECT' AND P.`AUTHORIZER`=current_authorizer());

CREATE OR REPLACE VIEW `LOCKS` (
  `LOCK_EXT_ID`,
  `LOCK_INT_ID`,
  `TXNID`,
  `DB`,
  `TABLE`,
  `PARTITION`,
  `LOCK_STATE`,
  `LOCK_TYPE`,
  `LAST_HEARTBEAT`,
  `ACQUIRED_AT`,
  `USER`,
  `HOST`,
  `HEARTBEAT_COUNT`,
  `AGENT_INFO`,
  `BLOCKEDBY_EXT_ID`,
  `BLOCKEDBY_INT_ID`
) AS
SELECT DISTINCT
  `LOCK_EXT_ID`,
  `LOCK_INT_ID`,
  `TXNID`,
  `DB`,
  `TABLE`,
  `PARTITION`,
  `LOCK_STATE`,
  `LOCK_TYPE`,
  `LAST_HEARTBEAT`,
  `ACQUIRED_AT`,
  `USER`,
  `HOST`,
  `HEARTBEAT_COUNT`,
  `AGENT_INFO`,
  `BLOCKEDBY_EXT_ID`,
  `BLOCKEDBY_INT_ID`
FROM SYS.`LOCKS` AS L JOIN `sys`.`TBLS` T ON (L.`TABLE` = T.`TBL_NAME`)
                               JOIN `sys`.`DBS` D ON (L.`DB` = D.`NAME`)
                               LEFT JOIN `sys`.`TBL_PRIVS` P ON (T.`TBL_ID` = P.`TBL_ID`)
WHERE
    (NOT restrict_information_schema() OR P.`TBL_ID` IS NOT NULL
        AND (P.`PRINCIPAL_NAME`=current_user() AND P.`PRINCIPAL_TYPE`='USER'
            OR ((array_contains(current_groups(), P.`PRINCIPAL_NAME`) OR P.`PRINCIPAL_NAME` = 'public') AND P.`PRINCIPAL_TYPE`='GROUP'))
        AND P.`TBL_PRIV`='SELECT' AND P.`AUTHORIZER`=current_authorizer());
