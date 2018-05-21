create database temp1;
use temp1;
create table test1_n3(id int);
create database temp2;
use temp2;
create table test2_n1(id int);
set hive.exec.pre.hooks=org.apache.hadoop.hive.ql.hooks.UpdateInputAccessTimeHook$PreExec;
use temp1;
desc temp2.test2_n1;
