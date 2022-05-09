SELECT 'Upgrading MetaStore schema from 4.0.0-alpha-1 to 4.0.0-alpha-2';

USE SYS;

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

SELECT 'Finished upgrading MetaStore schema from 4.0.0-alpha-1 to 4.0.0-alpha-2';
