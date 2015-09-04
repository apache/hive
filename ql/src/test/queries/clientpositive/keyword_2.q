set hive.support.sql11.reserved.keywords=false;
drop table varchar_udf_1;

create table varchar_udf_1 (c1 string, c2 string, c3 varchar(10), c4 varchar(20));
insert overwrite table varchar_udf_1
  select key, value, key, value from src where key = '238' limit 1;

select
  regexp(c2, 'val'),
  regexp(c4, 'val'),
  regexp(c2, 'val') = regexp(c4, 'val')
from varchar_udf_1 limit 1;

drop table varchar_udf_1;
