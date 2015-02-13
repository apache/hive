-- set of tests HIVE-9481
drop database if exists x314n cascade;
create database x314n;
use x314n;
create table source(s1 int, s2 int);

--number of columns mismatched
insert into source(s2,s1) values(1);

drop database if exists x314n cascade;