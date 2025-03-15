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

SELECT 'Finished upgrading MetaStore schema from 4.0.0 to 4.1.0';
