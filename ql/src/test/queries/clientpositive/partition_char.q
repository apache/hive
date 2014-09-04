drop table partition_char_1;

create table partition_char_1 (key string, value char(20)) partitioned by (dt char(10), region int);

insert overwrite table partition_char_1 partition(dt='2000-01-01', region=1)
  select * from src tablesample (10 rows);

select * from partition_char_1 limit 1;

drop table partition_char_1;
