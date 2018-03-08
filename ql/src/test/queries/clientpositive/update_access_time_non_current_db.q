create database temp1;
use temp1;
create table test1(id int);
create database temp2;
use temp2;
create table test2(id int);
set hive.exec.pre.hooks=org.apache.hadoop.hive.ql.hooks.UpdateInputAccessTimeHook$PreExec;
use temp1;
desc temp2.test2;
