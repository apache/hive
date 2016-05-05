set hive.mapred.mode=nonstrict;
set hive.optimize.constant.propagation=false;

explain
select * from (
  select * from (select * from srcpart a where a.ds = '2008-04-08' and a.hr = '11' order by a.key limit 5)pa
    union all
  select * from (select * from srcpart b where b.ds = '2008-04-08' and b.hr = '14' limit 5)pb
)subq;

select * from (
  select * from (select * from srcpart a where a.ds = '2008-04-08' and a.hr = '11' order by a.key limit 5)pa
    union all
  select * from (select * from srcpart b where b.ds = '2008-04-08' and b.hr = '14' limit 5)pb
)subq;

explain
select * from (
  select * from (select a.ds, a.key, a.hr from srcpart a where a.ds = '2008-04-08' and a.hr = '11' order by a.key limit 5)pa
    union all
  select * from (select b.ds, b.key, b.hr from srcpart b where b.ds = '2008-04-08' and b.hr = '14' limit 5)pb
)subq;

select * from (
  select * from (select a.ds, a.key, a.hr from srcpart a where a.ds = '2008-04-08' and a.hr = '11' order by a.key limit 5)pa
    union all
  select * from (select b.ds, b.key, b.hr from srcpart b where b.ds = '2008-04-08' and b.hr = '14' limit 5)pb
)subq;

explain
select * from (
  select * from (select a.ds, a.key, a.hr from srcpart a where a.ds = '2008-04-08' and a.hr = '11' order by a.hr,a.key limit 5)pa
    union all
  select * from (select b.ds, b.key, b.hr from srcpart b where b.ds = '2008-04-08' and b.hr = '14' limit 5)pb
)subq;

select * from (
  select * from (select a.ds, a.key, a.hr from srcpart a where a.ds = '2008-04-08' and a.hr = '11' order by a.hr,a.key limit 5)pa
    union all
  select * from (select b.ds, b.key, b.hr from srcpart b where b.ds = '2008-04-08' and b.hr = '14' limit 5)pb
)subq;

explain
select * from (
  select * from (select a.key, a.ds, a.value from srcpart a where a.ds = '2008-04-08' and a.hr = '11' order by a.ds limit 5)pa
    union all
  select * from (select b.key, b.ds, b.value from srcpart b where b.ds = '2008-04-08' and b.hr = '14' limit 5)pb
)subq;

select * from (
  select * from (select a.key, a.ds, a.value from srcpart a where a.ds = '2008-04-08' and a.hr = '11' order by a.ds limit 5)pa
    union all
  select * from (select b.key, b.ds, b.value from srcpart b where b.ds = '2008-04-08' and b.hr = '14' limit 5)pb
)subq;
