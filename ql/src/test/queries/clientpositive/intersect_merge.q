set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

create table a(key int, value int);

insert into table a values (1,2),(1,2),(1,3),(2,3);

create table b(key int, value int);

insert into table b values (1,2),(2,3);

explain select * from b intersect distinct select * from a intersect distinct select * from b intersect distinct select * from a intersect distinct select * from b;

explain (select * from b intersect distinct select * from a) intersect distinct (select * from b intersect distinct select * from a);

explain select * from b intersect distinct (select * from a intersect distinct (select * from b intersect distinct (select * from a intersect distinct select * from b)));

explain (((select * from b intersect distinct select * from a) intersect distinct select * from b) intersect distinct select * from a) intersect distinct select * from b;

explain select * from b intersect distinct (select * from a intersect distinct select * from b) intersect distinct select * from a intersect distinct select * from b;

explain select * from b intersect distinct (select * from a intersect all select * from b);

explain select * from b intersect all (select * from a intersect all select * from b);

explain select * from b intersect all (select * from a intersect distinct select * from b);

