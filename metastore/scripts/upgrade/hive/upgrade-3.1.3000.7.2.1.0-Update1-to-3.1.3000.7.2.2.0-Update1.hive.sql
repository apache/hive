SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.1.0-Update1 to 3.1.3000.7.2.2.0-Update1';

USE INFORMATION_SCHEMA;

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
  'default',
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
  'default',
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



CREATE OR REPLACE VIEW SYS.CDH_VERSION AS SELECT 1 AS VER_ID, '3.1.3000.7.2.2.0-Update1' AS SCHEMA_VERSION,
  'Hive release version 3.1.3000 for CDH 7.2.2.0-Update1' AS VERSION_COMMENT;

SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.1.0-Update1 to 3.1.3000.7.2.2.0-Update1';

