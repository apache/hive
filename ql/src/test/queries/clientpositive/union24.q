-- SORT_QUERY_RESULTS

create table src2 as select key, count(1) as count from src group by key;
create table src3 as select * from src2;
create table src4 as select * from src2;
create table src5 as select * from src2;


set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;


explain extended
select s.key, s.count from (
  select key, count from src2  where key < 10
  union all
  select key, count from src3  where key < 10
  union all
  select key, count from src4  where key < 10
  union all
  select key, count(1) as count from src5 where key < 10 group by key
)s
;

select s.key, s.count from (
  select key, count from src2  where key < 10
  union all
  select key, count from src3  where key < 10
  union all
  select key, count from src4  where key < 10
  union all
  select key, count(1) as count from src5 where key < 10 group by key
)s
;

explain extended
select s.key, s.count from (
  select key, count from src2  where key < 10
  union all
  select key, count from src3  where key < 10
  union all
  select a.key as key, b.count as count from src4 a join src5 b on a.key=b.key where a.key < 10
)s
;

select s.key, s.count from (
  select key, count from src2  where key < 10
  union all
  select key, count from src3  where key < 10
  union all
  select a.key as key, b.count as count from src4 a join src5 b on a.key=b.key where a.key < 10
)s
;

explain extended
select s.key, s.count from (
  select key, count from src2  where key < 10
  union all
  select key, count from src3  where key < 10
  union all
  select a.key as key, count(1) as count from src4 a join src5 b on a.key=b.key where a.key < 10 group by a.key
)s
;

select s.key, s.count from (
  select key, count from src2  where key < 10
  union all
  select key, count from src3  where key < 10
  union all
  select a.key as key, count(1) as count from src4 a join src5 b on a.key=b.key where a.key < 10 group by a.key
)s
;
