create table table_map_types (id int, c1 map<int,int>, c2 map<int,int>);
insert into table_map_types VALUES (1, map(1,1), map(2,1));
insert into table_map_types VALUES (2, map(1,2), map(2,2));
insert into table_map_types VALUES (3, map(1,3), map(2,3));
insert into table_map_types VALUES (4, map(1,4), map(1,4));
insert into table_map_types VALUES (1, map(1,1,2,2,3,3,4,4), map(2,1,1,4));
select * from table_map_types;

create table table_map_types1 (id int, c1 map<int,int>, c2 map<int,int>);
insert into table_map_types1 VALUES (1, map(1,1), map(2,1));
insert into table_map_types1 VALUES (2, map(1,2), map(2,2));
insert into table_map_types1 VALUES (3, map(1,4), map(1,3));
insert into table_map_types1 VALUES (1, map(1,1,2,2,3,3,4,4), map(2,1,1,5));
insert into table_map_types1 VALUES (1, map(1,1,2,2,3,3,4,5), map(2,1,1,4));
select * from table_map_types1;

set hive.cbo.enable=false;
set hive.auto.convert.join=false;
set hive.optimize.ppd=false;

explain select * from table_map_types t1 inner join table_map_types1 t2 on t1.c1 = t2.c1;
select * from table_map_types t1 inner join table_map_types1 t2 on t1.c1 = t2.c1;

explain select * from table_map_types t1 inner join table_map_types1 t2 on t1.c2 = t2.c2;
select * from table_map_types t1 inner join table_map_types1 t2 on t1.c2 = t2.c2;
