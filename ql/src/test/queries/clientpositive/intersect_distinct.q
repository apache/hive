--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

create table a_n17(key int, value int);

insert into table a_n17 values (1,2),(1,2),(1,3),(2,3);

create table b_n13(key int, value int);

insert into table b_n13 values (1,2),(2,3);

select key, count(1) as c from a_n17 group by key intersect all select value, max(key) as c from b_n13 group by value;

select * from a_n17 intersect distinct select * from b_n13;

select * from b_n13 intersect distinct select * from a_n17 intersect distinct select * from b_n13;

select * from a_n17 intersect distinct select * from b_n13 union all select * from a_n17 intersect distinct select * from b_n13;

select * from a_n17 intersect distinct select * from b_n13 union select * from a_n17 intersect distinct select * from b_n13;

select * from a_n17 intersect distinct select * from b_n13 intersect distinct select * from a_n17 intersect distinct select * from b_n13;

select * from (select a_n17.key, b_n13.value from a_n17 join b_n13 on a_n17.key=b_n13.key)sub1 
intersect distinct 
select * from (select a_n17.key, b_n13.value from a_n17 join b_n13 on a_n17.key=b_n13.key)sub2; 

select * from (select a_n17.key, b_n13.value from a_n17 join b_n13 on a_n17.key=b_n13.key)sub1
intersect distinct
select * from (select b_n13.value as key, a_n17.key as value from a_n17 join b_n13 on a_n17.key=b_n13.key)sub2;

explain select * from src intersect distinct select * from src;

select * from src intersect distinct select * from src;

explain select * from src intersect distinct select * from src intersect distinct select * from src intersect distinct select * from src;

select * from src intersect distinct select * from src intersect distinct select * from src intersect distinct select * from src;

explain select value from a_n17 group by value intersect distinct select key from b_n13 group by key;

select value from a_n17 group by value intersect distinct select key from b_n13 group by key;
