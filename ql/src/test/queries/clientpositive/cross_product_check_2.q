--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

create table A_n2 as
select * from src;

create table B_n2 as
select * from src order by key
limit 10;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000000;

explain select * from A_n2 join B_n2;

explain select * from B_n2 d1 join B_n2 d2 on d1.key = d2.key join A_n2;

explain select * from A_n2 join 
         (select d1.key 
          from B_n2 d1 join B_n2 d2 on d1.key = d2.key 
          where 1 = 1 group by d1.key) od1;

explain select * from A_n2 join (select d1.key from B_n2 d1 join B_n2 d2 where 1 = 1 group by d1.key) od1;

explain select * from 
(select A_n2.key from A_n2 group by key) ss join 
(select d1.key from B_n2 d1 join B_n2 d2 on d1.key = d2.key where 1 = 1 group by d1.key) od1;


