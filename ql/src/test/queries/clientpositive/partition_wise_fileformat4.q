--! qt:dataset:src1
create table partition_test_partitioned_n5(key string, value string) partitioned by (dt string);
alter table partition_test_partitioned_n5 set fileformat sequencefile;
insert overwrite table partition_test_partitioned_n5 partition(dt='1') select * from src1;
alter table partition_test_partitioned_n5 partition (dt='1') set fileformat sequencefile;

alter table partition_test_partitioned_n5 add partition (dt='2');
alter table partition_test_partitioned_n5 drop partition (dt='2');

