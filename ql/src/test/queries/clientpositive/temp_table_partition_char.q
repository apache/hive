--! qt:dataset:src
set hive.mapred.mode=nonstrict;
drop table partition_char_1_temp;

create temporary table partition_char_1_temp (key string, value char(20)) partitioned by (dt char(10), region int);

insert overwrite table partition_char_1_temp partition(dt='2000-01-01', region=1)
  select * from src tablesample (10 rows);

select * from partition_char_1_temp limit 1;

drop table partition_char_1_temp;
