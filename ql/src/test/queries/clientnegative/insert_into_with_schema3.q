-- set of tests HIVE-9481
drop database if exists x314n cascade;
create database x314n;
use x314n;
create table target1(x int, y int, z int);
create table source(s1 int, s2 int);

--invalid column name
insert into target1(a,z) select * from source;


drop database if exists x314n cascade;