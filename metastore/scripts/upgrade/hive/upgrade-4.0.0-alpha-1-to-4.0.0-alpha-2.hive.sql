SELECT 'Upgrading MetaStore schema from 4.0.0-alpha-1 to 4.0.0-alpha-2';

USE SYS;

DROP TABLE IF EXISTS `WM_RESOURCEPLANS`;
CREATE EXTERNAL TABLE IF NOT EXISTS `DBS` (
  `DB_ID` bigint,
  `DB_LOCATION_URI` string,
  `NAME` string,
  `OWNER_NAME` string,
  `OWNER_TYPE` string,
  `DB_MANAGED_LOCATION_URI` string,
  `TYPE` string,
  `DATACONNECTOR_NAME` string,
  `REMOTE_DBNAME` string,
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
  \"OWNER_TYPE\",
  \"DB_MANAGED_LOCATION_URI\",
  \"TYPE\",
  \"DATACONNECTOR_NAME\",
  \"REMOTE_DBNAME\"
FROM
  \"DBS\""
);

DROP TABLE IF EXISTS `PARTITIONS`;
CREATE EXTERNAL TABLE IF NOT EXISTS `PARTITIONS` (
  `PART_ID` bigint,
  `CREATE_TIME` int,
  `LAST_ACCESS_TIME` int,
  `PART_NAME` string,
  `SD_ID` bigint,
  `TBL_ID` bigint,
  `WRITE_ID` bigint,
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
  \"TBL_ID\",
  \"WRITE_ID\"
FROM
  \"PARTITIONS\""
);

DROP TABLE IF EXISTS `SERDES`;
CREATE EXTERNAL TABLE IF NOT EXISTS `SERDES` (
  `SERDE_ID` bigint,
  `NAME` string,
  `SLIB` string,
  `DESCRIPTION` string,
  `SERIALIZER_CLASS` string,
  `DESERIALIZER_CLASS` string,
  `SERDE_TYPE` int,
  CONSTRAINT `SYS_PK_SERDES` PRIMARY KEY (`SERDE_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"SERDE_ID\",
  \"NAME\",
  \"SLIB\",
  \"DESCRIPTION\",
  \"SERIALIZER_CLASS\",
  \"DESERIALIZER_CLASS\",
  \"SERDE_TYPE\"
FROM
  \"SERDES\""
);

DROP TABLE IF EXISTS `MV_CREATION_METADATA`;
CREATE EXTERNAL TABLE IF NOT EXISTS `MV_CREATION_METADATA` (
  `MV_CREATION_METADATA_ID` bigint,
  `DB_NAME` string,
  `CAT_NAME` string,
  `TBL_NAME` string,
  `TXN_LIST` string,
  `MATERIALIZATION_TIME` bigint,
  CONSTRAINT `SYS_PK_MV_CREATION_METADATA` PRIMARY KEY (`MV_CREATION_METADATA_ID`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"MV_CREATION_METADATA_ID\",
  \"DB_NAME\",
  \"CAT_NAME\",
  \"TBL_NAME\",
  \"TXN_LIST\",
  \"MATERIALIZATION_TIME\"
FROM \"MV_CREATION_METADATA\""
);

DROP TABLE IF EXISTS `MV_TABLES_USED`;
CREATE EXTERNAL TABLE IF NOT EXISTS `MV_TABLES_USED` (
  `MV_CREATION_METADATA_ID` bigint,
  `TBL_ID` bigint,
  `INSERTED_COUNT` bigint,
  `UPDATED_COUNT` bigint,
  `DELETED_COUNT` bigint
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"MV_CREATION_METADATA_ID\",
  \"TBL_ID\",
  \"INSERTED_COUNT\",
  \"UPDATED_COUNT\",
  \"DELETED_COUNT\"
FROM \"MV_TABLES_USED\""
);

DROP TABLE IF EXISTS `TAB_COL_STATS`;
CREATE EXTERNAL TABLE IF NOT EXISTS `TAB_COL_STATS` (
 `CS_ID` bigint,
 `CAT_NAME` string,
 `DB_NAME` string,
 `TABLE_NAME` string,
 `COLUMN_NAME` string,
 `COLUMN_TYPE` string,
 `TBL_ID` bigint,
 `LONG_LOW_VALUE` bigint,
 `LONG_HIGH_VALUE` bigint,
 `DOUBLE_LOW_VALUE` double,
 `DOUBLE_HIGH_VALUE` double,
 `BIG_DECIMAL_LOW_VALUE` string,
 `BIG_DECIMAL_HIGH_VALUE` string,
 `NUM_NULLS` bigint,
 `NUM_DISTINCTS` bigint,
 `BIT_VECTOR` binary,
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
 \"CAT_NAME\",
 \"DB_NAME\",
 \"TABLE_NAME\",
 \"COLUMN_NAME\",
 \"COLUMN_TYPE\",
 \"TBL_ID\",
 \"LONG_LOW_VALUE\",
 \"LONG_HIGH_VALUE\",
 \"DOUBLE_LOW_VALUE\",
 \"DOUBLE_HIGH_VALUE\",
 \"BIG_DECIMAL_LOW_VALUE\",
 \"BIG_DECIMAL_HIGH_VALUE\",
 \"NUM_NULLS\",
 \"NUM_DISTINCTS\",
 \"BIT_VECTOR\",
 \"AVG_COL_LEN\",
 \"MAX_COL_LEN\",
 \"NUM_TRUES\",
 \"NUM_FALSES\",
 \"LAST_ANALYZED\",
 \"ENGINE\"
FROM
  \"TAB_COL_STATS\""
);

DROP TABLE IF EXISTS `PART_COL_STATS`;
CREATE EXTERNAL TABLE IF NOT EXISTS `PART_COL_STATS` (
 `CS_ID` bigint,
 `CAT_NAME` string,
 `DB_NAME` string,
 `TABLE_NAME` string,
 `PARTITION_NAME` string,
 `COLUMN_NAME` string,
 `COLUMN_TYPE` string,
 `PART_ID` bigint,
 `LONG_LOW_VALUE` bigint,
 `LONG_HIGH_VALUE` bigint,
 `DOUBLE_LOW_VALUE` double,
 `DOUBLE_HIGH_VALUE` double,
 `BIG_DECIMAL_LOW_VALUE` string,
 `BIG_DECIMAL_HIGH_VALUE` string,
 `NUM_NULLS` bigint,
 `NUM_DISTINCTS` bigint,
 `BIT_VECTOR` binary,
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
 \"CAT_NAME\",
 \"DB_NAME\",
 \"TABLE_NAME\",
 \"PARTITION_NAME\",
 \"COLUMN_NAME\",
 \"COLUMN_TYPE\",
 \"PART_ID\",
 \"LONG_LOW_VALUE\",
 \"LONG_HIGH_VALUE\",
 \"DOUBLE_LOW_VALUE\",
 \"DOUBLE_HIGH_VALUE\",
 \"BIG_DECIMAL_LOW_VALUE\",
 \"BIG_DECIMAL_HIGH_VALUE\",
 \"NUM_NULLS\",
 \"NUM_DISTINCTS\",
 \"BIT_VECTOR\",
 \"AVG_COL_LEN\",
 \"MAX_COL_LEN\",
 \"NUM_TRUES\",
 \"NUM_FALSES\",
 \"LAST_ANALYZED\",
 \"ENGINE\"
FROM
  \"PART_COL_STATS\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `FUNC_RU` (
  `FUNC_ID` bigint,
  `RESOURCE_TYPE` int,
  `RESOURCE_URI` string,
  `INTEGER_IDX` int,
  CONSTRAINT `SYS_PK_FUNCS_RU` PRIMARY KEY (`FUNC_ID`, `INTEGER_IDX`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"FUNC_ID\",
  \"RESOURCE_TYPE\",
  \"RESOURCE_URI\",
  \"INTEGER_IDX\"
FROM
  \"FUNC_RU\""
);

DROP TABLE IF EXISTS `COMPACTION_QUEUE`;
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
  `CQ_META_INFO` binary,
  `CQ_HADOOP_JOB_ID` string,
  `CQ_ERROR_MESSAGE` string,
  `CQ_INITIATOR_ID` string,
  `CQ_INITIATOR_VERSION` string,
  `CQ_WORKER_VERSION` string,
  `CQ_CLEANER_START` bigint
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"CQ_ID\",
  \"CQ_DATABASE\",
  \"CQ_TABLE\",
  \"CQ_PARTITION\",
  \"CQ_STATE\",
  \"CQ_TYPE\",
  \"CQ_TBLPROPERTIES\",
  \"CQ_WORKER_ID\",
  \"CQ_ENQUEUE_TIME\",
  \"CQ_START\",
  \"CQ_RUN_AS\",
  \"CQ_HIGHEST_WRITE_ID\",
  \"CQ_META_INFO\",
  \"CQ_HADOOP_JOB_ID\",
  \"CQ_ERROR_MESSAGE\",
  \"CQ_INITIATOR_ID\",
  \"CQ_INITIATOR_VERSION\",
  \"CQ_WORKER_VERSION\",
  \"CQ_CLEANER_START\"
FROM \"COMPACTION_QUEUE\"
"
);

DROP TABLE IF EXISTS `REPLICATION_METRICS_ORIG`;
CREATE EXTERNAL TABLE IF NOT EXISTS `REPLICATION_METRICS_ORIG` (
    `SCHEDULED_EXECUTION_ID` bigint,
    `POLICY_NAME` string,
    `DUMP_EXECUTION_ID` bigint,
    `METADATA` string,
    `PROGRESS` string,
    `RM_START_TIME` int,
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
    \"RM_START_TIME\",
    \"MESSAGE_FORMAT\"
FROM \"REPLICATION_METRICS\""
);

SELECT 'Finished upgrading MetaStore schema from 4.0.0-alpha-1 to 4.0.0-alpha-2';
