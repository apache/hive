-- HIVE-30037: Combine operator outputs with explicit insert layouts and nullable lateral-view rows.
set hive.cbo.returnpath.hiveop=true;
create table paci_src (a int, b int);
insert into paci_src values (2,10),(3,20),(1,30);
create table paci_a (a int, b int);
create table paci_b (a int, b int);

from paci_src
lateral view outer explode(if(a=1,array(cast(a as string),cast(-a as string)),if(a=2,array(),null))) t as c
lateral view outer explode(array(b,-b)) u as n
insert into paci_a (b,a) select t.c,u.n order by 1 desc nulls last,2 desc limit 1
insert into paci_b (a,b) select t.c,u.n order by 1 asc nulls last,2 desc limit 1;
select 'multi_a',a,b from paci_a;
select 'multi_b',a,b from paci_b;

create table paci_default (a int, b int default 9);
insert into paci_default (a)
select t.c from paci_src
lateral view outer explode(if(a=1,array(cast(a as string),cast(-a as string)),if(a=2,array(),null))) t as c
order by 1 desc nulls last limit 1;
select 'default',a,b from paci_default;

create table paci_part (a int, b int) partitioned by (p int);
insert into table paci_part partition(p) (p,a)
select t.c,b from paci_src lateral view outer explode(array(a,-a)) t as c
order by 1 desc,2 desc limit 1;
select 'partition',a,b,p from paci_part;

-- Verify empty and NULL arrays retain their source rows under OUTER.
select a,t.c from paci_src
lateral view outer explode(if(a=1,array(cast(a as string),cast(-a as string)),if(a=2,array(),null))) t as c
order by 1,2;

create table paci_udtf_a (a int);
create table paci_udtf_b (a int);
from paci_src tablesample (5 rows)
insert into paci_udtf_a (a) select explode(array(a)) order by 1 desc limit 1
insert into paci_udtf_b (a) select explode(array(b)) order by 1 desc limit 1;
select 'udtf_a',a from paci_udtf_a;
select 'udtf_b',a from paci_udtf_b;
