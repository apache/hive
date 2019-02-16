create database bad_rename1;
use bad_rename1;
create table rename1(a int);
alter table bad_rename1.rename1 rename to bad_db_notexists.rename1;
