
create database tmpdb;
create temporary table tmpdb.tmp1 (c1 string, c2 string);
show create table tmpdb.tmp1;
drop table tmp1;
create temporary table tmpdb.tmp_not_null_tbl (a int NOT NULL);
show create table tmpdb.tmp_not_null_tbl;
drop table tmpdb.tmp_not_null_tbl;

drop database tmpdb;
