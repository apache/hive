create table table_list_types (id int, c1 array<int>, c2 array<int>);
insert into table_list_types VALUES (1, array(1,1), array(2,1));
insert into table_list_types VALUES (2, array(1,2), array(2,2));
insert into table_list_types VALUES (3, array(1,3), array(2,3));
insert into table_list_types VALUES (4, array(1,4), array(1,4));
insert into table_list_types VALUES (5, array(1,4), array(null,4));

create table table_list_types1 (id int, c1 array<int>, c2 array<int>);
insert into table_list_types1 VALUES (1, array(1,1), array(2,1));
insert into table_list_types1 VALUES (2, array(1,2), array(2,2));
insert into table_list_types1 VALUES (3, array(1,4), array(1,3));

set hive.cbo.enable=false;
set hive.auto.convert.join=false;
set hive.optimize.ppd=false;

explain select * from table_list_types t1 inner join table_list_types1 t2 on t1.c1 = t2.c1;
select * from table_list_types t1 inner join table_list_types1 t2 on t1.c1 = t2.c1;

explain select * from table_list_types t1 inner join table_list_types1 t2 on t1.c2 = t2.c2;
select * from table_list_types t1 inner join table_list_types1 t2 on t1.c2 = t2.c2;

set hive.cbo.enable=true;
set hive.auto.convert.join=true;
set hive.optimize.ppd=false;

explain select * from table_list_types t1 inner join table_list_types1 t2 on t1.c1 = t2.c1;
select * from table_list_types t1 inner join table_list_types1 t2 on t1.c1 = t2.c1;

explain select * from table_list_types t1 inner join table_list_types1 t2 on t1.c2 = t2.c2;
select * from table_list_types t1 inner join table_list_types1 t2 on t1.c2 = t2.c2;

set hive.cbo.enable=true;
set hive.auto.convert.join=true;
set hive.optimize.ppd=true;
explain select * from table_list_types t1 inner join table_list_types1 t2 on t1.c1 = t2.c1;
select * from table_list_types t1 inner join table_list_types1 t2 on t1.c1 = t2.c1;

explain select * from table_list_types t1 inner join table_list_types1 t2 on t1.c2 = t2.c2;
select * from table_list_types t1 inner join table_list_types1 t2 on t1.c2 = t2.c2;

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
insert into table_map_types1 VALUES (1, map(2,2,1,1,3,3,4,4), map(2,1,1,5));
insert into table_map_types1 VALUES (1, map(1,1,2,2,3,3,4,5), map(2,1,1,4));
select * from table_map_types1;

set hive.cbo.enable=true;
set hive.auto.convert.join=true;
set hive.optimize.ppd=false;

explain select * from table_map_types t1 inner join table_map_types1 t2 on t1.c1 = t2.c1;
select * from table_map_types t1 inner join table_map_types1 t2 on t1.c1 = t2.c1;

explain select * from table_map_types t1 inner join table_map_types1 t2 on t1.c2 = t2.c2;
select * from table_map_types t1 inner join table_map_types1 t2 on t1.c2 = t2.c2;

set hive.cbo.enable=true;
set hive.auto.convert.join=true;
set hive.optimize.ppd=true;

explain select * from table_map_types t1 inner join table_map_types1 t2 on t1.c1 = t2.c1;
select * from table_map_types t1 inner join table_map_types1 t2 on t1.c1 = t2.c1;

explain select * from table_map_types t1 inner join table_map_types1 t2 on t1.c2 = t2.c2;
select * from table_map_types t1 inner join table_map_types1 t2 on t1.c2 = t2.c2;


create table table_struct_types (id int, c1 struct<f1: int,f2: string>, c2 struct<f1: int,f2: string>);
insert into table_struct_types VALUES (1, named_struct("f1",1,"f2",'val1'), named_struct("f1",2,"f2",'val1'));
insert into table_struct_types VALUES (2, named_struct("f1",2,"f2",'val2'), named_struct("f1",2,"f2",'val2'));
insert into table_struct_types VALUES (3, named_struct("f1",3,"f2",'val3'), named_struct("f1",2,"f2",'val3'));
insert into table_struct_types VALUES (4, named_struct("f1",4,"f2",'val4'), named_struct("f1",2,"f2",'val4'));

create table table_struct_types1 (id int, c1 struct<f1: int,f2: string>, c2 struct<f1: int,f2: string>);
insert into table_struct_types1 VALUES (1, named_struct("f1",1,"f2",'val1'), named_struct("f1",2,"f2",'val1'));
insert into table_struct_types1 VALUES (2, named_struct("f1",2,"f2",'val2'), named_struct("f1",2,"f2",'val2'));
insert into table_struct_types1 VALUES (4, named_struct("f1",4,"f2",'val4'), named_struct("f1",1,"f2",'val3'));

set hive.cbo.enable=false;
set hive.auto.convert.join=false;
set hive.optimize.ppd=false;

explain select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c1 = t2.c1;
select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c1 = t2.c1;

explain select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c2 = t2.c2;
select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c2 = t2.c2;

set hive.cbo.enable=true;
set hive.auto.convert.join=true;
set hive.optimize.ppd=false;

explain select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c1 = t2.c1;
select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c1 = t2.c1;

explain select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c2 = t2.c2;
select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c2 = t2.c2;

set hive.cbo.enable=true;
set hive.auto.convert.join=true;
set hive.optimize.ppd=true;

explain select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c1 = t2.c1;
select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c1 = t2.c1;

explain select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c2 = t2.c2;
select * from table_struct_types t1 inner join table_struct_types1 t2 on t1.c2 = t2.c2;

create table table_union_types (id int, c1 UNIONTYPE<int, string, struct<f1: int,f2: string>>, c2 UNIONTYPE<int, string>);
insert into table_union_types VALUES (1, create_union(2, 2, "val",named_struct("f1",1,"f2",'val1')), create_union(0, 2, "val_2"));
insert into table_union_types VALUES (2, create_union(2, 2, "val",named_struct("f1",2,"f2",'val2')), create_union(0, 2, "val_2"));
insert into table_union_types VALUES (3, create_union(2, 2, "val",named_struct("f1",3,"f2",'val3')), create_union(0, 2, "val_2"));
insert into table_union_types VALUES (4, create_union(2, 2, "val",named_struct("f1",4,"f2",'val4')), create_union(0, 2, "val_2"));

create table table_union_types1 (id int, c1 UNIONTYPE<int, string, struct<f1: int,f2: string>>, c2 UNIONTYPE<int, string>);
insert into table_union_types1 VALUES (1, create_union(2, 2, "val", named_struct("f1",1,"f2",'val1')), create_union(0, 2, "val_2"));
insert into table_union_types1 VALUES (2, create_union(2, 2, "val", named_struct("f1",2,"f2",'val2')), create_union(0, 2, "val_2"));
insert into table_union_types1 VALUES (2, create_union(0, 2, "val", named_struct("f1",1,"f2",'val1')), create_union(1, 2, "val_2"));
insert into table_union_types1 VALUES (2, create_union(1, 2, "val_2", named_struct("f1",1,"f2",'val1')), create_union(1, 2, "val_2"));
insert into table_union_types1 VALUES (4, create_union(2, 2, "val", named_struct("f1",4,"f2",'val4')), create_union(0, 1, "val_2"));

set hive.cbo.enable=false;
set hive.auto.convert.join=false;
set hive.optimize.ppd=false;

explain select * from table_union_types t1 inner join table_union_types1 t2 on t1.c1 = t2.c1;
select * from table_union_types t1 inner join table_union_types1 t2 on t1.c1 = t2.c1;

explain select * from table_union_types t1 inner join table_union_types1 t2 on t1.c2 = t2.c2;
select * from table_union_types t1 inner join table_union_types1 t2 on t1.c2 = t2.c2;

set hive.cbo.enable=true;
set hive.auto.convert.join=true;
set hive.optimize.ppd=false;

explain select * from table_union_types t1 inner join table_union_types1 t2 on t1.c1 = t2.c1;
select * from table_union_types t1 inner join table_union_types1 t2 on t1.c1 = t2.c1;

explain select * from table_union_types t1 inner join table_union_types1 t2 on t1.c2 = t2.c2;
select * from table_union_types t1 inner join table_union_types1 t2 on t1.c2 = t2.c2;

set hive.cbo.enable=true;
set hive.auto.convert.join=true;
set hive.optimize.ppd=true;

explain select * from table_union_types t1 inner join table_union_types1 t2 on t1.c1 = t2.c1;
select * from table_union_types t1 inner join table_union_types1 t2 on t1.c1 = t2.c1;

explain select * from table_union_types t1 inner join table_union_types1 t2 on t1.c2 = t2.c2;
select * from table_union_types t1 inner join table_union_types1 t2 on t1.c2 = t2.c2;
