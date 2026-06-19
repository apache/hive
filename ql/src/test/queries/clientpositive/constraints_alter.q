CREATE EXTERNAL TABLE IF NOT EXISTS `COLUMNS_V2` (
  `CD_ID` bigint,
  `COMMENT` string,
  `COLUMN_NAME` string,
  `TYPE_NAME` string,
  `INTEGER_IDX` int,
  CONSTRAINT `SYS_PK_COLUMN_V2` PRIMARY KEY (`CD_ID`,`COLUMN_NAME`) DISABLE
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
  \"CD_ID\",
  \"COMMENT\",
  \"COLUMN_NAME\",
  \"TYPE_NAME\",
  \"INTEGER_IDX\"
FROM
  \"COLUMNS_V2\""
);

CREATE EXTERNAL TABLE IF NOT EXISTS `KEY_CONSTRAINTS`
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
  CONSTRAINT `SYS_PK_KEY_CONSTRAINTS` PRIMARY KEY (`CONSTRAINT_NAME`, `POSITION`) DISABLE
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


create table t1_n2939 (a_n2939 integer, b_n2939 string);

insert into table t1_n2939 values (1, '1'), (2, '2');

select COLUMN_NAME from columns_v2 where column_name = 'a_n2939' or column_name = 'b_n2939' or column_name = 'c_n2939';
select CONSTRAINT_NAME from key_constraints where constraint_name = 't1_n2939_pk' or constraint_name = 't1_n2939_nn' or constraint_name = 't1_n2939_nn_2';

alter table t1_n2939 add constraint t1_n2939_pk primary key (a_n2939) disable novalidate rely;

select COLUMN_NAME from columns_v2 where column_name = 'a_n2939' or column_name = 'b_n2939' or column_name = 'c_n2939';
select CONSTRAINT_NAME from key_constraints where constraint_name = 't1_n2939_pk' or constraint_name = 't1_n2939_nn' or constraint_name = 't1_n2939_nn_2';

alter table t1_n2939 change column b_n2939 b_n2939 string constraint t1_n2939_nn not null disable novalidate rely;

select COLUMN_NAME from columns_v2 where column_name = 'a_n2939' or column_name = 'b_n2939' or column_name = 'c_n2939';
select CONSTRAINT_NAME from key_constraints where constraint_name = 't1_n2939_pk' or constraint_name = 't1_n2939_nn' or constraint_name = 't1_n2939_nn_2';

alter table t1_n2939 change column b_n2939 c_n2939 string constraint t1_n2939_nn_2 not null disable novalidate rely;

select COLUMN_NAME from columns_v2 where column_name = 'a_n2939' or column_name = 'b_n2939' or column_name = 'c_n2939';
select CONSTRAINT_NAME from key_constraints where constraint_name = 't1_n2939_pk' or constraint_name = 't1_n2939_nn' or constraint_name = 't1_n2939_nn_2';

drop table t1_n2939;

select COLUMN_NAME from columns_v2 where column_name = 'a_n2939' or column_name = 'b_n2939' or column_name = 'c_n2939';
select CONSTRAINT_NAME from key_constraints where constraint_name = 't1_n2939_pk' or constraint_name = 't1_n2939_nn' or constraint_name = 't1_n2939_nn_2';
