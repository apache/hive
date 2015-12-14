set hive.mapred.mode=nonstrict;
-- set of tests HIVE-9481
drop database if exists x314n cascade;
create database x314n;
use x314n;
create table source(s1 int, s2 int);
--column number mismatch
insert into source(s2) values(2,1);

drop database if exists x314n cascade;
