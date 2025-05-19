SELECT 'Upgrading MetaStore schema from 4.0.0 to 4.1.0';

USE SYS;

-- HIVE-28442
DROP TABLE IF EXISTS `KEY_CONSTRAINTS`;
CREATE EXTERNAL TABLE `KEY_CONSTRAINTS`
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

DROP TABLE IF EXISTS `PART_COL_STATS`;
CREATE EXTERNAL TABLE `PART_COL_STATS` (
 `CS_ID` bigint,
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

DROP TABLE IF EXISTS `SCHEDULED_QUERIES`;
CREATE EXTERNAL TABLE `SCHEDULED_QUERIES` (
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

DROP TABLE IF EXISTS `TAB_COL_STATS`;
CREATE EXTERNAL TABLE `TAB_COL_STATS` (
 `CS_ID` bigint,
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

-- HIVE-27855
CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_HIVE_QUERY_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_QUERY_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents$HiveHookEventProto',
  'proto.maptypes'='org.apache.hadoop.hive.ql.hooks.proto.MapFieldEntry'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_APP_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_APP_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$HistoryEventProto',
  'proto.maptypes'='KVPair'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_DAG_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_DAG_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$HistoryEventProto',
  'proto.maptypes'='KVPair'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_DAG_META`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_DAG_META_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$ManifestEntryProto'
);

CREATE OR REPLACE VIEW `VERSION` AS SELECT 1 AS `VER_ID`, '4.1.0' AS `SCHEMA_VERSION`,
  'Hive release version 4.1.0' AS `VERSION_COMMENT`;
  
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
  CASE WHEN CC_TYPE = 'i' THEN 'minor' WHEN CC_TYPE = 'a' THEN 'major' WHEN CC_TYPE = '*' THEN 'smart-optimize' ELSE 'UNKNOWN' END,
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

USE INFORMATION_SCHEMA;

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

SELECT 'Finished upgrading MetaStore schema from 4.0.0 to 4.1.0';
