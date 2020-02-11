--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

create table A_n8 as
select * from src;

create table B_n6 as
select * from src
limit 10;

set hive.auto.convert.join.noconditionaltask.size=100;

explain select * from A_n8 join B_n6;

explain select * from B_n6 d1 join B_n6 d2 on d1.key = d2.key join A_n8;

explain select * from A_n8 join 
         (select d1.key 
          from B_n6 d1 join B_n6 d2 on d1.key = d2.key
          where 1 = 1 group by d1.key) od1;

explain select * from A_n8 join (select d1.key from B_n6 d1 join B_n6 d2 where 1 = 1  group by d1.key) od1;

explain select * from 
(select A_n8.key from A_n8  group by key) ss join
(select d1.key from B_n6 d1 join B_n6 d2 on d1.key = d2.key where 1 = 1 group by d1.key) od1;


