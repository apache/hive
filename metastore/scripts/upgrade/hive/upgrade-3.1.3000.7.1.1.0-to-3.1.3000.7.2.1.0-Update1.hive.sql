SELECT 'Upgrading MetaStore schema from 3.1.3000.7.1.1.0 to 3.1.3000.7.2.1.0-Update1';

USE SYS;


DROP TABLE IF EXISTS `REPLICATION_METRICS`;

CREATE EXTERNAL TABLE IF NOT EXISTS `REPLICATION_METRICS` (
    `SCHEDULED_EXECUTION_ID` bigint,
    `POLICY_NAME` string,
    `DUMP_EXECUTION_ID` bigint,
    `METADATA` string,
    `PROGRESS` string
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
    \"RM_PROGRESS\"
FROM \"REPLICATION_METRICS\""
);


CREATE OR REPLACE VIEW SYS.CDH_VERSION AS SELECT 1 AS VER_ID, '3.1.3000.7.2.1.0-Update1' AS SCHEMA_VERSION,
  'Hive release version 3.1.3000 for CDH 7.2.1.0-Update1' AS VERSION_COMMENT;

SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.1.1.0 to 3.1.3000.7.2.1.0-Update1';

