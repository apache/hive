--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table src2_n6 as select key, count(1) as count from src group by key;
create table src3_n2 as select * from src2_n6;
create table src4_n0 as select * from src2_n6;
create table src5_n3 as select * from src2_n6;


set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;


explain extended
select s.key, s.count from (
  select key, count from src2_n6  where key < 10
  union all
  select key, count from src3_n2  where key < 10
  union all
  select key, count from src4_n0  where key < 10
  union all
  select key, count(1) as count from src5_n3 where key < 10 group by key
)s
;

select s.key, s.count from (
  select key, count from src2_n6  where key < 10
  union all
  select key, count from src3_n2  where key < 10
  union all
  select key, count from src4_n0  where key < 10
  union all
  select key, count(1) as count from src5_n3 where key < 10 group by key
)s
;

explain extended
select s.key, s.count from (
  select key, count from src2_n6  where key < 10
  union all
  select key, count from src3_n2  where key < 10
  union all
  select a.key as key, b.count as count from src4_n0 a join src5_n3 b on a.key=b.key where a.key < 10
)s
;

select s.key, s.count from (
  select key, count from src2_n6  where key < 10
  union all
  select key, count from src3_n2  where key < 10
  union all
  select a.key as key, b.count as count from src4_n0 a join src5_n3 b on a.key=b.key where a.key < 10
)s
;

explain extended
select s.key, s.count from (
  select key, count from src2_n6  where key < 10
  union all
  select key, count from src3_n2  where key < 10
  union all
  select a.key as key, count(1) as count from src4_n0 a join src5_n3 b on a.key=b.key where a.key < 10 group by a.key
)s
;

select s.key, s.count from (
  select key, count from src2_n6  where key < 10
  union all
  select key, count from src3_n2  where key < 10
  union all
  select a.key as key, count(1) as count from src4_n0 a join src5_n3 b on a.key=b.key where a.key < 10 group by a.key
)s
;
