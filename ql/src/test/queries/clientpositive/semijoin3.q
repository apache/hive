create table t1 as select cast(key as int) key, value from src;

create table t2 as select cast(key as int) key, value from src;

set hive.cbo.enable=false;

explain
select count(1)
from
  (select key
  from t1
  where key = 0) t1
left semi join
  (select key
  from t2
  where key = 0) t2
on 1 = 1;

select count(1)
from
  (select key
  from t1
  where key = 0) t1
left semi join
  (select key
  from t2
  where key = 0) t2
on 1 = 1;
