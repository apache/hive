create table table_list_types (id int, c1 array<int>, c2 array<int>);
insert into table_list_types VALUES (1, array(1,1), array(2,1));
insert into table_list_types VALUES (2, array(1,2), array(2,2));
insert into table_list_types VALUES (3, array(1,3), array(2,3));
insert into table_list_types VALUES (4, array(1,4), array(1,4));
insert into table_list_types VALUES (5, array(1,4), array(null,4));
insert into table_list_types VALUES (6, array(1,1,1), array(1,2,3));
insert into table_list_types VALUES (7, array(1,2,3), array(3,2,1));
insert into table_list_types VALUES (8, array(1,1,1,1), array(4,3,2,1));

create table table_list_types1 (id int, c1 array<int>, c2 array<int>);
insert into table_list_types1 VALUES (1, array(1,1), array(2,1));
insert into table_list_types1 VALUES (2, array(1,2), array(2,2));
insert into table_list_types1 VALUES (3, array(1,4), array(1,3));
insert into table_list_types1 VALUES (4, array(1,1,1), array(1,2,3));
insert into table_list_types1 VALUES (5, array(1,2,3), array(2,2,2));
insert into table_list_types1 VALUES (6, array(1,1,1,1), array(2,2,2,2));

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
