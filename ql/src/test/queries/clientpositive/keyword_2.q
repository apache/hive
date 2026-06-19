--! qt:dataset:src
drop table varchar_udf_1_n1;

create table varchar_udf_1_n1 (c1 string, c2 string, c3 varchar(10), c4 varchar(20));
insert overwrite table varchar_udf_1_n1
  select key, value, key, value from src where key = '238' limit 1;

select
  c2 regexp 'val',
  c4 regexp 'val',
  (c2 regexp 'val') = (c4 regexp 'val')
from varchar_udf_1_n1 limit 1;

drop table varchar_udf_1_n1;
