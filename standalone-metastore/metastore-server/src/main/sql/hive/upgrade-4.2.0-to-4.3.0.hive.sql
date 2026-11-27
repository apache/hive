SELECT 'Upgrading MetaStore schema from 4.2.0 to 4.3.0';

USE SYS;

-- HIVE-29244, HIVE-30286
DROP TABLE IF EXISTS `HIVE_LOCKS`;
CREATE EXTERNAL TABLE `HIVE_LOCKS` (
    `HL_LOCK_EXT_ID` bigint,
    `HL_LOCK_INT_ID` bigint,
    `HL_TXNID` bigint,
    `HL_CATALOG` string,
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
    \"HL_CATALOG\",
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
    `CATALOG`,
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
    HL.`HL_CATALOG`,
    HL.`HL_DB`,
    HL.`HL_TABLE`,
    HL.`HL_PARTITION`,
    CASE
        WHEN HL.`HL_LOCK_STATE` = 'a' THEN 'acquired'
        WHEN HL.`HL_LOCK_STATE` = 'w' THEN 'waiting'
    END AS LOCK_STATE,
    CASE
        WHEN HL.`HL_LOCK_TYPE` = 'e' THEN 'exclusive'
        WHEN HL.`HL_LOCK_TYPE` = 'x' THEN 'excl_write'
        WHEN HL.`HL_LOCK_TYPE` = 'r' THEN 'shared_read'
        WHEN HL.`HL_LOCK_TYPE` = 'w' THEN 'shared_write'
    END AS LOCK_TYPE,
    FROM_UNIXTIME(HL.`HL_LAST_HEARTBEAT`),
    FROM_UNIXTIME(HL.`HL_ACQUIRED_AT`),
    HL.`HL_USER`,
    HL.`HL_HOST`,
    HL.`HL_HEARTBEAT_COUNT`,
    HL.`HL_AGENT_INFO`,
    HL.`HL_BLOCKEDBY_EXT_ID`,
    HL.`HL_BLOCKEDBY_INT_ID`
FROM SYS.`HIVE_LOCKS` AS HL;

CREATE OR REPLACE VIEW `VERSION` AS SELECT 1 AS `VER_ID`, '4.3.0' AS `SCHEMA_VERSION`,
  'Hive release version 4.3.0' AS `VERSION_COMMENT`;

USE INFORMATION_SCHEMA;

CREATE OR REPLACE VIEW `LOCKS` (
  `LOCK_EXT_ID`,
  `LOCK_INT_ID`,
  `TXNID`,
  `CATALOG`,
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
  `CATALOG`,
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

SELECT 'Finished upgrading MetaStore schema from 4.2.0 to 4.3.0';
