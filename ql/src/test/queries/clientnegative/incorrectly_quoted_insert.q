create database tdb;
use tdb;
create table t1(id int);
create table t2(id int);
explain insert into `tdb.t1` select * from t2;
