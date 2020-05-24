--! qt:dataset:src1

create temporary table partition_schema1_temp(key string, value string) partitioned by (dt string);

insert overwrite table partition_schema1_temp partition(dt='100') select * from src1;
desc partition_schema1_temp partition(dt='100');

alter table partition_schema1_temp add columns (x string);

desc partition_schema1_temp;
desc partition_schema1_temp partition (dt='100');


