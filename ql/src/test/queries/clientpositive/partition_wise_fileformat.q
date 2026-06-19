--! qt:dataset:src1
set hive.mapred.mode=nonstrict;


create table partition_test_partitioned_n1(key string, value string) partitioned by (dt string);

insert overwrite table partition_test_partitioned_n1 partition(dt=100) select * from src1;
show table extended like partition_test_partitioned_n1;
show table extended like partition_test_partitioned_n1 partition(dt=100);
select key from partition_test_partitioned_n1 where dt=100;
select key from partition_test_partitioned_n1;

alter table partition_test_partitioned_n1 set fileformat rcfile;
insert overwrite table partition_test_partitioned_n1 partition(dt=101) select * from src1;
show table extended like partition_test_partitioned_n1;
show table extended like partition_test_partitioned_n1 partition(dt=100);
show table extended like partition_test_partitioned_n1 partition(dt=101);
select key from partition_test_partitioned_n1 where dt=100;
select key from partition_test_partitioned_n1 where dt=101;
select key from partition_test_partitioned_n1;

alter table partition_test_partitioned_n1 set fileformat Sequencefile;
insert overwrite table partition_test_partitioned_n1 partition(dt=102) select * from src1;
show table extended like partition_test_partitioned_n1;
show table extended like partition_test_partitioned_n1 partition(dt=100);
show table extended like partition_test_partitioned_n1 partition(dt=101);
show table extended like partition_test_partitioned_n1 partition(dt=102);
select key from partition_test_partitioned_n1 where dt=100;
select key from partition_test_partitioned_n1 where dt=101;
select key from partition_test_partitioned_n1 where dt=102;
select key from partition_test_partitioned_n1;

select key from partition_test_partitioned_n1 where dt >=100 and dt <= 102;

