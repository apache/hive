--! qt:dataset:src
set hive.mapred.mode=nonstrict;
create table t1_n79 (id1 string, id2 string);
create table t2_n48 (id string, d int);

from src tablesample (1 rows)
  insert into table t1_n79 select 'a','a'
  insert into table t2_n48 select 'a',2;

explain
select a.*,b.d d1,c.d d2 from
  t1_n79 a join t2_n48 b on (a.id1 = b.id)
       join t2_n48 c on (a.id2 = b.id) where b.d <= 1 and c.d <= 1;

explain
select * from (
select a.*,b.d d1,c.d d2 from
  t1_n79 a join t2_n48 b on (a.id1 = b.id)
       join t2_n48 c on (a.id2 = b.id) where b.d <= 1 and c.d <= 1
) z where d1 > 1 or d2 > 1;

select * from (
select a.*,b.d d1,c.d d2 from
  t1_n79 a join t2_n48 b on (a.id1 = b.id)
       join t2_n48 c on (a.id2 = b.id) where b.d <= 1 and c.d <= 1
) z where d1 > 1 or d2 > 1;
