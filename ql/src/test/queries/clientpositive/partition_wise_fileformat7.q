--! qt:dataset:src1
set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

create table partition_test_partitioned_n11(key string, value string) partitioned by (dt string);

alter table partition_test_partitioned_n11 set fileformat rcfile;
insert overwrite table partition_test_partitioned_n11 partition(dt=101) select * from src1;

select count(1) from partition_test_partitioned_n11  a join partition_test_partitioned_n11  b on a.key = b.key
where a.dt = '101' and b.dt = '101';

select count(1) from partition_test_partitioned_n11  a join partition_test_partitioned_n11  b on a.key = b.key
where a.dt = '101' and b.dt = '101' and a.key < 100;