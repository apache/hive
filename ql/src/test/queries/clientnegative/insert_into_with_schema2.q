set hive.mapred.mode=nonstrict;
-- set of tests HIVE-9481
drop database if exists x314n cascade;
create database x314n;
use x314n;
create table source(s1 int, s2 int);
create table target1(x int, y int, z int);

--number of columns mismatched
insert into target1(x,y,z) select * from source;

drop database if exists x314n cascade;