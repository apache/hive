--! qt:dataset:src1


create table partition_test_partitioned_n8(key string, value string) partitioned by (dt string);

alter table partition_test_partitioned_n8 set fileformat rcfile;
insert overwrite table partition_test_partitioned_n8 partition(dt=101) select * from src1;
show table extended like partition_test_partitioned_n8 partition(dt=101);

alter table partition_test_partitioned_n8 set fileformat Sequencefile;
insert overwrite table partition_test_partitioned_n8 partition(dt=102) select * from src1;
show table extended like partition_test_partitioned_n8 partition(dt=102);
select key from partition_test_partitioned_n8 where dt=102;

insert overwrite table partition_test_partitioned_n8 partition(dt=101) select * from src1;
show table extended like partition_test_partitioned_n8 partition(dt=101);
select key from partition_test_partitioned_n8 where dt=101;


