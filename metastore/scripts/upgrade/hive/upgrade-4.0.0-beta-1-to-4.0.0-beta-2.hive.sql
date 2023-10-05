SELECT 'Upgrading MetaStore schema from 4.0.0-beta-1 to 4.0.0-beta-2';
USE SYS;
--HIVE-27303
DROP TABLE IF EXISTS `HIVE_LOCKS`;
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
  `HL_BLOCKEDBY_INT_ID` bigint,
  `HL_ERROR_MESSAGE` string
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
    \"HL_BLOCKEDBY_INT_ID\",
    \"HL_ERROR_MESSAGE\"
FROM \"HIVE_LOCKS\""
);

SELECT 'Finished upgrading MetaStore schema from 4.0.0-beta-1 to 4.0.0-beta-2';
