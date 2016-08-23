set hive.mapred.mode=nonstrict;
set hive.optimize.cte.materialize.threshold=2;
set hive.explain.user=true;

create temporary table q1 (a int, b string);
insert into q1 values (1, 'A');

show tables;

explain
with q1 as (select * from src where key= '5')
select a.key
from q1 a join q1 b
on a.key=b.key;

with q1 as (select * from src where key= '5')
select a.key
from q1 a join q1 b
on a.key=b.key;

show tables;

select * from q1;

drop table q1;

show tables;

explain
with q1 as (select * from src where key= '5')
select a.key
from q1 a join q1 b
on a.key=b.key;

with q1 as (select * from src where key= '5')
select a.key
from q1 a join q1 b
on a.key=b.key;

show tables;
