SELECT 'Upgrading MetaStore schema from 3.1.0 to 4.0.0';

USE SYS;

-- HIVE-20793
DROP TABLE IF EXISTS `WM_RESOURCEPLANS`;
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

DROP TABLE IF EXISTS `WM_TRIGGERS`;
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

DROP TABLE IF EXISTS `WM_POOLS`;
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

DROP TABLE IF EXISTS `WM_POOLS_TO_TRIGGERS`;
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

DROP TABLE IF EXISTS `WM_MAPPINGS`;
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

-- HIVE-22553
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
    FROM_UNIXTIME(T.`TXN_STARTED`) AS TXN_STARTED,
    FROM_UNIXTIME(T.`TXN_LAST_HEARTBEAT`) AS TXN_LAST_HEARTBEAT,
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
    CASE WHEN HL.`HL_LOCK_TYPE` = 'e' THEN 'exclusive' WHEN HL.`HL_LOCK_TYPE` = 'r' THEN 'shared' WHEN HL.`HL_LOCK_TYPE` = 'w' THEN 'semi-shared' END AS LOCK_TYPE,
    FROM_UNIXTIME(HL.`HL_LAST_HEARTBEAT`),
    FROM_UNIXTIME(HL.`HL_ACQUIRED_AT`),
    HL.`HL_USER`,
    HL.`HL_HOST`,
    HL.`HL_HEARTBEAT_COUNT`,
    HL.`HL_AGENT_INFO`,
    HL.`HL_BLOCKEDBY_EXT_ID`,
    HL.`HL_BLOCKEDBY_INT_ID`
FROM SYS.`HIVE_LOCKS` AS HL;

DROP TABLE IF EXISTS `VERSION`;

CREATE OR REPLACE VIEW `VERSION` AS SELECT 1 AS `VER_ID`, '4.0.0' AS `SCHEMA_VERSION`,
  'Hive release version 4.0.0' AS `VERSION_COMMENT`;

SELECT 'Finished upgrading MetaStore schema from 3.1.0 to 4.0.0';
