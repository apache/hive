set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

create table a(key int);

insert into table a values (0),(1),(2),(2),(2),(2),(3),(NULL),(NULL);

create table b(key bigint);

insert into table b values (1),(2),(2),(3),(5),(5),(NULL),(NULL),(NULL);

select * from a except all select * from b;

drop table a;

drop table b;

create table a(key int, value int);

insert into table a values (1,2),(1,2),(1,3),(2,3),(2,2);

create table b(key int, value int);

insert into table b values (1,2),(2,3),(2,2),(2,2),(2,20);

select * from a except all select * from b;

select * from b except all select * from a;

select * from b except all select * from a intersect distinct select * from b;

select * from b except all select * from a except distinct select * from b;

select * from a except all select * from b union all select * from a except distinct select * from b;

select * from a except all select * from b union select * from a except distinct select * from b;

select * from a except all select * from b except distinct select * from a except distinct select * from b;

select * from (select a.key, b.value from a join b on a.key=b.key)sub1 
except all 
select * from (select a.key, b.value from a join b on a.key=b.key)sub2; 

select * from (select a.key, b.value from a join b on a.key=b.key)sub1
except all
select * from (select b.value as key, a.key as value from a join b on a.key=b.key)sub2;

explain select * from src except all select * from src;

select * from src except all select * from src;

explain select * from src except all select * from src except distinct select * from src except distinct select * from src;

select * from src except all select * from src except distinct select * from src except distinct select * from src;

explain select value from a group by value except distinct select key from b group by key;

select value from a group by value except distinct select key from b group by key;
