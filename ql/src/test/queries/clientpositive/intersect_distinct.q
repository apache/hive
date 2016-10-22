set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

create table a(key int, value int);

insert into table a values (1,2),(1,2),(1,3),(2,3);

create table b(key int, value int);

insert into table b values (1,2),(2,3);

select key, count(1) as c from a group by key intersect all select value, max(key) as c from b group by value;

select * from a intersect distinct select * from b;

select * from b intersect distinct select * from a intersect distinct select * from b;

select * from a intersect distinct select * from b union all select * from a intersect distinct select * from b;

select * from a intersect distinct select * from b union select * from a intersect distinct select * from b;

select * from a intersect distinct select * from b intersect distinct select * from a intersect distinct select * from b;

select * from (select a.key, b.value from a join b on a.key=b.key)sub1 
intersect distinct 
select * from (select a.key, b.value from a join b on a.key=b.key)sub2; 

select * from (select a.key, b.value from a join b on a.key=b.key)sub1
intersect distinct
select * from (select b.value as key, a.key as value from a join b on a.key=b.key)sub2;

explain select * from src intersect distinct select * from src;

select * from src intersect distinct select * from src;

explain select * from src intersect distinct select * from src intersect distinct select * from src intersect distinct select * from src;

select * from src intersect distinct select * from src intersect distinct select * from src intersect distinct select * from src;

explain select value from a group by value intersect distinct select key from b group by key;

select value from a group by value intersect distinct select key from b group by key;
