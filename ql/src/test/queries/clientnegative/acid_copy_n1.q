create table nocopyfiles(a int, b int) clustered by (a)  into 2 buckets stored as orc TBLPROPERTIES('transactional'='false');
insert into nocopyfiles(a,b) values(1,2);
alter table nocopyfiles SET TBLPROPERTIES ('transactional'='true');

create table withcopyfiles(a int, b int) clustered by (a)  into 2 buckets stored as orc TBLPROPERTIES('transactional'='false');
insert into withcopyfiles(a,b) values(1,2);
insert into withcopyfiles(a,b) values(1,3);
alter table withcopyfiles SET TBLPROPERTIES ('transactional'='true');
