
create database tmpdb;
create temporary table tmpdb.tmp1 (c1 string, c2 string);
show create table tmpdb.tmp1;
drop table tmp1;
drop database tmpdb;
