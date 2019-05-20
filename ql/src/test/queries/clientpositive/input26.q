--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;
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
