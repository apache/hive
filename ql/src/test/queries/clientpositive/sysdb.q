set hive.strict.checks.cartesian.product=false;

set hive.compute.query.using.stats=false;

set hive.support.concurrency=true;

set hive.cbo.enable=false;

create table src_buck (key int, value string) clustered by(value) into 2 buckets;

create table src_skew (key int) skewed by (key) on (1,2,3);

CREATE TABLE scr_txn (key int, value string)
    CLUSTERED BY (key) INTO 2 BUCKETS STORED AS ORC
    TBLPROPERTIES (
      "transactional"="true",
      "compactor.mapreduce.map.memory.mb"="2048",
      "compactorthreshold.hive.compactor.delta.num.threshold"="4",
      "compactorthreshold.hive.compactor.delta.pct.threshold"="0.5");

CREATE TEMPORARY TABLE src_tmp (key int, value string);

CREATE TABLE moretypes (a decimal(10,2), b tinyint, c smallint, d int, e bigint, f varchar(10), g char(3));

show grant user hive_test_user;

source ../../metastore/scripts/upgrade/hive/hive-schema-3.0.0.hive.sql;

use sys;

select bucket_col_name, integer_idx from bucketing_cols order by bucket_col_name, integer_idx limit 5;

select count(*) from cds;

select column_name, type_name, integer_idx from columns_v2 order by column_name, integer_idx limit 5;

select param_key, param_value from database_params order by param_key, param_value limit 5;

select db_location_uri, name, owner_name, owner_type from dbs order by name;

select grantor, principal_name from db_privs order by grantor, principal_name limit 5;

select grantor, principal_name from global_privs order by grantor, principal_name limit 5;

select index_name, index_handler_class from idxs order by index_name limit 5;

select param_key, param_value from index_params order by param_key, param_value limit 5;

select part_name from partitions order by part_name limit 5;

select pkey_name, pkey_type from partition_keys order by pkey_name limit 5;

select part_key_val, integer_idx from partition_key_vals order by part_key_val, integer_idx limit 5;

select param_key, param_value from partition_params order by param_key, param_value limit 5;

select grantor, principal_name from part_col_privs order by grantor, principal_name limit 5;

select grantor, principal_name from part_privs order by grantor, principal_name limit 5;

select role_name from roles order by role_name limit 5;

select principal_name, grantor from role_map order by principal_name, grantor limit 5;

select count(*) from sds;

select param_key, param_value from sd_params order by param_key, param_value limit 5;

select sequence_name from sequence_table order by sequence_name limit 5;

select name, slib from serdes order by name, slib limit 5;

select param_key, param_value from serde_params order by param_key, param_value limit 5;

select skewed_col_name from skewed_col_names order by skewed_col_name limit 5;

select count(*) from skewed_col_value_loc_map;

select count(*) from skewed_string_list;

select count(*) from skewed_string_list_values;

select count(*) from skewed_values;

select column_name, `order` from sort_cols order by column_name limit 5;

select param_key, param_value from table_params order by param_key, param_value limit 5;

select tbl_name from tbls order by tbl_name limit 5;

select column_name, grantor, principal_name from tbl_col_privs order by column_name, principal_name limit 5;

select grantor, principal_name from tbl_privs order by grantor, principal_name limit 5;

select table_name, column_name, num_nulls, num_distincts from tab_col_stats order by table_name, column_name limit 10;

select table_name, partition_name, column_name, num_nulls, num_distincts from part_col_stats order by table_name, partition_name, column_name limit 10;

select schema_version from version order by schema_version limit 5;

select func_name, func_type from funcs order by func_name, func_type limit 5;

select constraint_name from key_constraints order by constraint_name limit 5;

select COLUMN_STATS_ACCURATE, NUM_FILES, NUM_ROWS, RAW_DATA_SIZE, TOTAL_SIZE FROM TABLE_STATS_VIEW where COLUMN_STATS_ACCURATE is not null order by NUM_FILES, NUM_ROWS, RAW_DATA_SIZE limit 5;

select COLUMN_STATS_ACCURATE, NUM_FILES, NUM_ROWS, RAW_DATA_SIZE, TOTAL_SIZE FROM PARTITION_STATS_VIEW where COLUMN_STATS_ACCURATE is not null order by NUM_FILES, NUM_ROWS, RAW_DATA_SIZE limit 5;

describe sys.tab_col_stats;

explain select max(num_distincts) from sys.tab_col_stats;

select max(num_distincts) from sys.tab_col_stats;

use INFORMATION_SCHEMA;

select count(*) from SCHEMATA;

select * from TABLES order by TABLE_SCHEMA, TABLE_NAME;

select * from TABLE_PRIVILEGES order by GRANTOR, GRANTEE, TABLE_SCHEMA, TABLE_NAME, PRIVILEGE_TYPE limit 10;

select * from COLUMNS where TABLE_NAME = 'alltypesorc' or TABLE_NAME = 'moretypes' order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION ;

select * from COLUMN_PRIVILEGES order by GRANTOR, GRANTEE, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME limit 10;

select TABLE_SCHEMA, TABLE_NAME from views order by TABLE_SCHEMA, TABLE_NAME;
