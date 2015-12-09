set hive.mapred.mode=nonstrict;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger;
set hive.metastore.disallow.incompatible.col.type.changes=false;
drop table if exists d1;
create table d1(a int);

from (select a.ctinyint x, b.cstring1 y
from alltypesorc a join alltypesorc b on a.cint = b.cbigint) t
insert into table d1 select x + length(y);

drop table if exists d2;
create table d2(b varchar(128));

from (select a.ctinyint x, b.cstring1 y
from alltypesorc a join alltypesorc b on a.cint = b.cbigint) t
insert into table d1 select x where y is null
insert into table d2 select y where x > 0;

drop table if exists t;
create table t as
select * from
  (select * from
     (select key from src1 limit 1) v1) v2;

drop table if exists dest_l1;
create table dest_l1(a int, b varchar(128))
  partitioned by (ds string) clustered by (a) into 2 buckets;

insert into table dest_l1 partition (ds='today')
select cint, cast(cstring1 as varchar(128)) as cs
from alltypesorc
where cint is not null and cint < 0 order by cint, cs limit 5;

insert into table dest_l1 partition (ds='tomorrow')
select min(cint), cast(min(cstring1) as varchar(128)) as cs
from alltypesorc
where cint is not null and cboolean1 = true
group by csmallint
having min(cbigint) > 10;

select cint, rank() over(order by cint) from alltypesorc
where cint > 10 and cint < 10000 limit 10;

select a.ctinyint, a.cint, count(a.cdouble)
  over(partition by a.ctinyint order by a.cint desc
    rows between 1 preceding and 1 following)
from alltypesorc a inner join alltypesorc b on a.cint = b.cbigint
order by a.ctinyint, a.cint;

with v2 as
  (select cdouble, count(cint) over() a,
    sum(cint + cbigint) over(partition by cboolean1) b
    from (select * from alltypesorc) v1)
select cdouble, a, b, a + b, cdouble + a from v2
where cdouble is not null
order by cdouble, a, b limit 5;

select a.cbigint, a.ctinyint, b.cint, b.ctinyint
from
  (select ctinyint, cbigint from alltypesorc
   union all
   select ctinyint, cbigint from alltypesorc) a
  inner join
  alltypesorc b
  on (a.ctinyint = b.ctinyint)
where b.ctinyint < 100 and a.cbigint is not null and b.cint is not null
order by a.cbigint, a.ctinyint, b.cint, b.ctinyint limit 5;

select x.ctinyint, x.cint, c.cbigint-100, c.cstring1
from alltypesorc c
join (
   select a.ctinyint ctinyint, b.cint cint
   from (select * from alltypesorc a where cboolean1=false) a
   join alltypesorc b on (a.cint = b.cbigint - 224870380)
 ) x on (x.cint = c.cint)
where x.ctinyint > 10
and x.cint < 4.5
and x.ctinyint + length(c.cstring2) < 1000;

select c1, x2, x3
from (
  select c1, min(c2) x2, sum(c3) x3
  from (
    select c1, c2, c3
    from (
      select cint c1, ctinyint c2, min(cbigint) c3
      from alltypesorc
      where cint is not null
      group by cint, ctinyint
      order by cint, ctinyint
      limit 5
    ) x
  ) x2
  group by c1
) y
where x2 > 0
order by x2, c1 desc;

select key, value from src1
where key in (select key+18 from src1) order by key;

select * from src1 a
where exists
  (select cint from alltypesorc b
   where a.key = b.ctinyint + 300)
and key > 300;

select key, value from src1
where key not in (select key+18 from src1) order by key;

select * from src1 a
where not exists
  (select cint from alltypesorc b
   where a.key = b.ctinyint + 300)
and key > 300;

with t as (select key x, value y from src1 where key > '2')
select x, y from t where y > 'v' order by x, y limit 5;

from (select key x, value y from src1 where key > '2') t
select x, y where y > 'v' order by x, y limit 5;

drop view if exists dest_v1;
create view dest_v1 as
  select ctinyint, cint from alltypesorc where ctinyint is not null;

select * from dest_v1 order by ctinyint, cint limit 2;

alter view dest_v1 as select ctinyint from alltypesorc;

select t.ctinyint from (select * from dest_v1 where ctinyint is not null) t
where ctinyint > 10 order by ctinyint limit 2;

drop view if exists dest_v2;
create view dest_v2 (a, b) as select c1, x2
from (
  select c1, min(c2) x2
  from (
    select c1, c2, c3
    from (
      select cint c1, ctinyint c2, min(cfloat) c3
      from alltypesorc
      group by cint, ctinyint
      order by cint, ctinyint
      limit 1
    ) x
  ) x2
  group by c1
) y
order by x2,c1 desc;

drop view if exists dest_v3;
create view dest_v3 (a1, a2, a3, a4, a5, a6, a7) as
  select x.csmallint, x.cbigint bint1, x.ctinyint, c.cbigint bint2, x.cint, x.cfloat, c.cstring1
  from alltypesorc c
  join (
     select a.csmallint csmallint, a.ctinyint ctinyint, a.cstring2 cstring2,
           a.cint cint, a.cstring1 ctring1, b.cfloat cfloat, b.cbigint cbigint
     from ( select * from alltypesorc a where cboolean1=true ) a
     join alltypesorc b on (a.csmallint = b.cint)
   ) x on (x.ctinyint = c.cbigint)
  where x.csmallint=11
  and x.cint > 899
  and x.cfloat > 4.5
  and c.cstring1 < '7'
  and x.cint + x.cfloat + length(c.cstring1) < 1000;

alter view dest_v3 as
  select * from (
    select sum(a.ctinyint) over (partition by a.csmallint order by a.csmallint) a,
      count(b.cstring1) x, b.cboolean1
    from alltypesorc a join alltypesorc b on (a.cint = b.cint)
    where a.cboolean2 = true and b.cfloat > 0
    group by a.ctinyint, a.csmallint, b.cboolean1
    having count(a.cint) > 10
    order by a, x, b.cboolean1 limit 10) t;

select * from dest_v3 limit 2;

drop table if exists src_dp;
create table src_dp (first string, word string, year int, month int, day int);
drop table if exists dest_dp1;
create table dest_dp1 (first string, word string) partitioned by (year int);
drop table if exists dest_dp2;
create table dest_dp2 (first string, word string) partitioned by (y int, m int);
drop table if exists dest_dp3;
create table dest_dp3 (first string, word string) partitioned by (y int, m int, d int);

set hive.exec.dynamic.partition.mode=nonstrict;

insert into dest_dp1 partition (year) select first, word, year from src_dp;
insert into dest_dp2 partition (y, m) select first, word, year, month from src_dp;
insert into dest_dp2 partition (y=0, m) select first, word, month from src_dp where year=0;
insert into dest_dp3 partition (y=0, m, d) select first, word, month m, day d from src_dp where year=0;

drop table if exists src_dp1;
create table src_dp1 (f string, w string, m int);

from src_dp, src_dp1
insert into dest_dp1 partition (year) select first, word, year
insert into dest_dp2 partition (y, m) select first, word, year, month
insert into dest_dp3 partition (y=2, m, d) select first, word, month m, day d where year=2
insert into dest_dp2 partition (y=1, m) select f, w, m
insert into dest_dp1 partition (year=0) select f, w;

reset hive.metastore.disallow.incompatible.col.type.changes;
