set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

create table a(key int, value int);

insert into table a values (1,2),(1,2),(1,2),(1,3),(2,3);

create table b(key int, value int);

insert into table b values (1,2),(1,2),(2,3);

select * from a intersect select * from b;

(select * from b intersect (select * from a)) intersect select * from b;

select * from b intersect all select * from a intersect select * from b;

(select * from b) intersect all ((select * from a) intersect select * from b);

select * from (select a.key, b.value from a join b on a.key=b.key)sub1 
intersect 
select * from (select a.key, b.value from a join b on a.key=b.key)sub2; 

drop table a;

drop table b;

create table a(key int);

insert into table a values (0),(1),(2),(2),(2),(2),(3),(NULL),(NULL),(NULL),(NULL),(NULL);

create table b(key bigint);

insert into table b values (1),(2),(2),(3),(5),(5),(NULL),(NULL),(NULL);

select * from a except select * from b;

(select * from a) minus select * from b union (select * from a) minus select * from b;

(select * from a) minus select * from b union all ((select * from a) minus select * from b);

(select * from a) minus select * from b union all (select * from a) minus all select * from b;

select * from a minus select * from b minus (select * from a minus select * from b);

(select * from a) minus (select * from b minus (select * from a minus select * from b));

drop table a;

drop table b;

