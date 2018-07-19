--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

create table a_n10(key int, value int);

insert into table a_n10 values (1,2),(1,2),(1,3),(2,3);

create table b_n8(key int, value int);

insert into table b_n8 values (1,2),(2,3);

select key, value, count(1) as c from a_n10 group by key, value;

select * from a_n10 intersect all select * from b_n8;

select * from b_n8 intersect all select * from a_n10 intersect all select * from b_n8;

select * from a_n10 intersect all select * from b_n8 union all select * from a_n10 intersect all select * from b_n8;

select * from a_n10 intersect all select * from b_n8 union select * from a_n10 intersect all select * from b_n8;

select * from a_n10 intersect all select * from b_n8 intersect all select * from a_n10 intersect all select * from b_n8;

select * from (select a_n10.key, b_n8.value from a_n10 join b_n8 on a_n10.key=b_n8.key)sub1 
intersect all 
select * from (select a_n10.key, b_n8.value from a_n10 join b_n8 on a_n10.key=b_n8.key)sub2; 

select * from (select a_n10.key, b_n8.value from a_n10 join b_n8 on a_n10.key=b_n8.key)sub1
intersect all
select * from (select b_n8.value as key, a_n10.key as value from a_n10 join b_n8 on a_n10.key=b_n8.key)sub2;

explain select * from src intersect all select * from src;

select * from src intersect all select * from src;

explain select * from src intersect all select * from src intersect all select * from src intersect all select * from src;

select * from src intersect all select * from src intersect all select * from src intersect all select * from src;

explain select value from a_n10 group by value intersect all select key from b_n8 group by key;

select value from a_n10 group by value intersect all select key from b_n8 group by key;
