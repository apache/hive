--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.returnpath.hiveop=true;
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

create table A_n18 as
select * from src;

create table B_n14 as
select * from src order by key
limit 10;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000000;

explain select * from A_n18 join B_n14;

explain select * from B_n14 d1 join B_n14 d2 on d1.key = d2.key join A_n18;

explain select * from A_n18 join 
         (select d1.key 
          from B_n14 d1 join B_n14 d2 on d1.key = d2.key 
          where 1 = 1 group by d1.key) od1;

explain select * from A_n18 join (select d1.key from B_n14 d1 join B_n14 d2 where 1 = 1 group by d1.key) od1;

explain select * from 
(select A_n18.key from A_n18 group by key) ss join 
(select d1.key from B_n14 d1 join B_n14 d2 on d1.key = d2.key where 1 = 1 group by d1.key) od1;


