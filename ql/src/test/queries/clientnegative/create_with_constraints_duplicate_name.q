create database db1;
use db1;
create table t1(x int, constraint constraint_name primary key (x) disable);

-- same constraint name in different db or different table is valid, thus only the foreign key creation should fail
create database db2;
use db2;
create table t1(x int, constraint constraint_name primary key (x) disable);
create table t2(x int, constraint constraint_name primary key (x) disable);

alter table t1 add constraint constraint_name foreign key (x) references t2(x) disable novalidate rely;
