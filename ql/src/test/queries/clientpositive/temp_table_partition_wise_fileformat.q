--! qt:dataset:src1
set hive.mapred.mode=nonstrict;


create temporary table partition_test_partitioned_n1_temp(key string, value string) partitioned by (dt string);

insert overwrite table partition_test_partitioned_n1_temp partition(dt=100) select * from src1;
show table extended like partition_test_partitioned_n1_temp;
show table extended like partition_test_partitioned_n1_temp partition(dt=100);
select key from partition_test_partitioned_n1_temp where dt=100;
select key from partition_test_partitioned_n1_temp;

alter table partition_test_partitioned_n1_temp set fileformat rcfile;
insert overwrite table partition_test_partitioned_n1_temp partition(dt=101) select * from src1;
show table extended like partition_test_partitioned_n1_temp;
show table extended like partition_test_partitioned_n1_temp partition(dt=100);
show table extended like partition_test_partitioned_n1_temp partition(dt=101);
select key from partition_test_partitioned_n1_temp where dt=100;
select key from partition_test_partitioned_n1_temp where dt=101;
select key from partition_test_partitioned_n1_temp;

alter table partition_test_partitioned_n1_temp set fileformat Sequencefile;
insert overwrite table partition_test_partitioned_n1_temp partition(dt=102) select * from src1;
show table extended like partition_test_partitioned_n1_temp;
show table extended like partition_test_partitioned_n1_temp partition(dt=100);
show table extended like partition_test_partitioned_n1_temp partition(dt=101);
show table extended like partition_test_partitioned_n1_temp partition(dt=102);
select key from partition_test_partitioned_n1_temp where dt=100;
select key from partition_test_partitioned_n1_temp where dt=101;
select key from partition_test_partitioned_n1_temp where dt=102;
select key from partition_test_partitioned_n1_temp;

select key from partition_test_partitioned_n1_temp where dt >=100 and dt <= 102;

