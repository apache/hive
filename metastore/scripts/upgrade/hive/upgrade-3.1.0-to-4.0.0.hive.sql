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



DROP TABLE IF EXISTS `VERSION`;

CREATE OR REPLACE VIEW `VERSION` AS SELECT 1 AS `VER_ID`, '4.0.0' AS `SCHEMA_VERSION`,
  'Hive release version 4.0.0' AS `VERSION_COMMENT`;

SELECT 'Finished upgrading MetaStore schema from 3.1.0 to 4.0.0';
