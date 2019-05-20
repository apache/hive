--! qt:dataset:src
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table varchar_udf_2;

create table varchar_udf_2 (c1 string, c2 string, c3 varchar(10), c4 varchar(20)) STORED AS ORC;
insert overwrite table varchar_udf_2
  select key, value, key, value from src where key = '238' limit 1;

explain vectorization expression
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


create temporary table HIVE_14349 (a string) stored as orc;

insert into HIVE_14349 values('XYZa'), ('badXYZa');

explain vectorization expression
select * from HIVE_14349 where a LIKE 'XYZ%a%';

select * from HIVE_14349 where a LIKE 'XYZ%a%';

insert into HIVE_14349 values ('XYZab'), ('XYZabBAD'), ('badXYZab'), ('badXYZabc');

explain vectorization expression
select * from HIVE_14349 where a LIKE 'XYZ%a_';

select * from HIVE_14349 where a LIKE 'XYZ%a_';

drop table HIVE_14349;
