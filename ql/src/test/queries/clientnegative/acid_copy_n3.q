create database acidtestdb;
use acidtestdb;

create table nocopyfiles(a int, b int) partitioned by (c int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES('transactional'='false');
insert into table nocopyfiles partition (c=1) values(1, 1);
alter table nocopyfiles SET TBLPROPERTIES ('transactional'='true');

create table withcopyfiles(a int, b int) partitioned by (c int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES('transactional'='false');
insert into table withcopyfiles partition (c=1) values (1, 1);
insert into table withcopyfiles partition (c=1) values (1, 1);
insert into table withcopyfiles partition (c=2) values (2, 2);
alter table withcopyfiles SET TBLPROPERTIES ('transactional'='true');
