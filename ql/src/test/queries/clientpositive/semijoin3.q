--! qt:dataset:src
create table t1_n59 as select cast(key as int) key, value from src;

create table t2_n37 as select cast(key as int) key, value from src;

set hive.cbo.enable=false;

explain
select count(1)
from
  (select key
  from t1_n59
  where key = 0) t1_n59
left semi join
  (select key
  from t2_n37
  where key = 0) t2_n37
on 1 = 1;

select count(1)
from
  (select key
  from t1_n59
  where key = 0) t1_n59
left semi join
  (select key
  from t2_n37
  where key = 0) t2_n37
on 1 = 1;
