SELECT 'Upgrading MetaStore schema from 4.2.0 to 4.3.0';

USE SYS;

DROP TABLE IF EXISTS `DBS`;
CREATE EXTERNAL TABLE IF NOT EXISTS `DBS` (
  `DB_ID` bigint,
  `DB_LOCATION_URI` string,
  `NAME` string,
  `CTLG_NAME` string,
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
  \"CTLG_NAME\",
  \"OWNER_NAME\",
  \"OWNER_TYPE\"
FROM
  \"DBS\""
);

DROP TABLE IF EXISTS `TXN_COMPONENTS`;
CREATE EXTERNAL TABLE IF NOT EXISTS `TXN_COMPONENTS` (
    `TC_TXNID` bigint,
    `TC_CATALOG` string,
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
    \"TC_CATALOG\",
    \"TC_DATABASE\",
    \"TC_TABLE\",
    \"TC_PARTITION\",
    \"TC_OPERATION_TYPE\",
    \"TC_WRITEID\"
FROM \"TXN_COMPONENTS\""
);

DROP TABLE IF EXISTS `COMPLETED_COMPACTIONS`;
CREATE EXTERNAL TABLE IF NOT EXISTS `COMPLETED_COMPACTIONS` (
  `CC_ID` bigint,
  `CC_CATALOG` string,
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
  \"COMPLETED_COMPACTIONS\".\"CC_CATALOG\",
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
FROM \"COMPLETED_COMPACTIONS\""
);

DROP TABLE IF EXISTS `COMPACTION_QUEUE`;
CREATE EXTERNAL TABLE IF NOT EXISTS `COMPACTION_QUEUE` (
  `CQ_ID` bigint,
  `CQ_CATALOG` string,
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
  \"COMPACTION_QUEUE\".\"CQ_CATALOG\",
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
FROM \"COMPACTION_QUEUE\""
);

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
    `TC_CATALOG`,
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
    TC.`TC_CATALOG`,
    TC.`TC_DATABASE`,
    TC.`TC_TABLE`,
    TC.`TC_PARTITION`,
    CASE WHEN TC.`TC_OPERATION_TYPE` = 's' THEN 'SELECT' WHEN TC.`TC_OPERATION_TYPE` = 'i' THEN 'INSERT' WHEN TC.`TC_OPERATION_TYPE` = 'u' THEN 'UPDATE' WHEN TC.`TC_OPERATION_TYPE` = 'c' THEN 'COMPACT' END AS OPERATION_TYPE,
    TC.`TC_WRITEID`
FROM `SYS`.`TXNS` AS T
LEFT JOIN `SYS`.`TXN_COMPONENTS` AS TC ON T.`TXN_ID` = TC.`TC_TXNID`;

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
  CC_CATALOG,
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
  CQ_CATALOG,
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
  D.`CTLG_NAME`,
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
  D.CTLG_NAME,
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
  D.`CTLG_NAME`,
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
  D.CTLG_NAME,
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
  D.`CTLG_NAME`,
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
  D.CTLG_NAME,
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
                        JOIN `sys`.`DBS` D ON (C.`C_DATABASE` = D.`NAME`) AND (C.`C_CATALOG` = D.`CTLG_NAME`)
                        LEFT JOIN `sys`.`TBL_PRIVS` P ON (T.`TBL_ID` = P.`TBL_ID`)
WHERE
  (NOT restrict_information_schema() OR P.`TBL_ID` IS NOT NULL
  AND (P.`PRINCIPAL_NAME`=current_user() AND P.`PRINCIPAL_TYPE`='USER'
    OR ((array_contains(current_groups(), P.`PRINCIPAL_NAME`) OR P.`PRINCIPAL_NAME` = 'public') AND P.`PRINCIPAL_TYPE`='GROUP'))
  AND P.`TBL_PRIV`='SELECT' AND P.`AUTHORIZER`=current_authorizer());

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
  `TC_CATALOG`,
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
  `TC_CATALOG`,
  `TC_DATABASE`,
  `TC_TABLE`,
  `TC_PARTITION`,
  `TC_OPERATION_TYPE`,
  `TC_WRITEID`
FROM `SYS`.`TRANSACTIONS` AS TXN JOIN `sys`.`TBLS` T ON (TXN.`TC_TABLE` = T.`TBL_NAME`)
                                 JOIN `sys`.`DBS` D ON (TXN.`TC_DATABASE` = D.`NAME`) AND (TXN.`TC_CATALOG` = D.`CTLG_NAME`)
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
                               JOIN `sys`.`DBS` D ON (L.`DB` = D.`NAME`) AND (L.`CATALOG` = D.`CTLG_NAME`)
                               LEFT JOIN `sys`.`TBL_PRIVS` P ON (T.`TBL_ID` = P.`TBL_ID`)
WHERE
    (NOT restrict_information_schema() OR P.`TBL_ID` IS NOT NULL
        AND (P.`PRINCIPAL_NAME`=current_user() AND P.`PRINCIPAL_TYPE`='USER'
            OR ((array_contains(current_groups(), P.`PRINCIPAL_NAME`) OR P.`PRINCIPAL_NAME` = 'public') AND P.`PRINCIPAL_TYPE`='GROUP'))
        AND P.`TBL_PRIV`='SELECT' AND P.`AUTHORIZER`=current_authorizer());

SELECT 'Finished upgrading MetaStore schema from 4.2.0 to 4.3.0';
