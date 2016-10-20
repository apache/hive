
-- union test
with q1 as (select * from src where key= '5'),
q2 as (select * from src s2 where key = '4')
select * from q1 union all select * from q2
;

-- insert test
create table s1 like src;
with q1 as ( select key, value from src where key = '5')
from q1
insert overwrite table s1
select *
;
select * from s1;
drop table s1;

-- from style
with q1 as (select * from src where key= '5')
from q1
select *
;

-- ctas
create table s2 as
with q1 as ( select key from src where key = '4')
select * from q1
;

select * from s2;
drop table s2;

-- view test
create view v1 as
with q1 as ( select key from src where key = '5')
select * from q1
;

select * from v1;

drop view v1;


-- view test, name collision
create view v1 as
with q1 as ( select key from src where key = '5')
select * from q1
;

with q1 as ( select key from src where key = '4')
select * from v1
;

drop view v1;

drop view if exists view_3;
create view view_3 as select cstring2, AVG(cint) from alltypesorc group by cstring2 limit 10;

drop view if exists view_4;
create view view_4 as select s.cstring1, v.ctimestamp1 from alltypesorc s join alltypesorc v on (s.cstring2= v.cstring1);

