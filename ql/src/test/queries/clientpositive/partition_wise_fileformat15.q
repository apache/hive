--! qt:dataset:src
-- SORT_QUERY_RESULTS
set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-- This tests that the schema can be changed for binary serde data
create table partition_test_partitioned_n6(key string, value string)
partitioned by (dt string) stored as rcfile;
insert overwrite table partition_test_partitioned_n6 partition(dt='1')
select * from src where key = 238;

select * from partition_test_partitioned_n6 where dt is not null;
select key+key, value from partition_test_partitioned_n6 where dt is not null;
set hive.metastore.disallow.incompatible.col.type.changes=false;
alter table partition_test_partitioned_n6 change key key int;
reset hive.metastore.disallow.incompatible.col.type.changes;
select key+key, value from partition_test_partitioned_n6 where dt is not null;
select * from partition_test_partitioned_n6 where dt is not null;

alter table partition_test_partitioned_n6 add columns (value2 string);

select key+key, value from partition_test_partitioned_n6 where dt is not null;
select * from partition_test_partitioned_n6 where dt is not null;

insert overwrite table partition_test_partitioned_n6 partition(dt='2')
select key, value, value from src where key = 86;

select key+key, value, value2, dt from partition_test_partitioned_n6 where dt is not null;
select * from partition_test_partitioned_n6 where dt is not null;
