SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table varchar_udf_2;

create table varchar_udf_2 (c1 string, c2 string, c3 varchar(10), c4 varchar(20)) STORED AS ORC;
insert overwrite table varchar_udf_2
  select key, value, key, value from src where key = '238' limit 1;

explain
select 
  c1 LIKE '%38%',
  c2 LIKE 'val_%',
  c3 LIKE '%38',
  c1 LIKE '%3x8%',
  c2 LIKE 'xval_%',
  c3 LIKE '%x38'
from varchar_udf_2 limit 1;

select 
  c1 LIKE '%38%',
  c2 LIKE 'val_%',
  c3 LIKE '%38',
  c1 LIKE '%3x8%',
  c2 LIKE 'xval_%',
  c3 LIKE '%x38'
from varchar_udf_2 limit 1;

drop table varchar_udf_2;
