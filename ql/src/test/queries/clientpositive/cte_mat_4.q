--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.cte.materialize.threshold=2;
set hive.optimize.cte.materialize.full.aggregate.only=false;
set hive.explain.user=true;

create temporary table q1 (a int, b string);
insert into q1 values (1, 'A');

show tables like "q1";

explain
with q1 as (select * from src where key= '5')
select a.key
from q1 a join q1 b
on a.key=b.key;

with q1 as (select * from src where key= '5')
select a.key
from q1 a join q1 b
on a.key=b.key;

show tables like "q1";

select * from q1;

drop table q1;

show tables like "q1";

explain
with q1 as (select * from src where key= '5')
select a.key
from q1 a join q1 b
on a.key=b.key;

with q1 as (select * from src where key= '5')
select a.key
from q1 a join q1 b
on a.key=b.key;

show tables like "q1";
