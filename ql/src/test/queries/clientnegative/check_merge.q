set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE t_target(
name string CHECK (length(name)<=20),
age int,
gpa double CHECK (gpa BETWEEN 0.0 AND 4.0))
stored as orc TBLPROPERTIES ('transactional'='true');

CREATE TABLE t_source(
name string,
age int,
gpa double);

insert into t_source(name, age, gpa) values ('student1', 16, null);

insert into t_target(name, age, gpa) values ('student1', 16, 2.0);

merge into t_target using t_source source on source.age=t_target.age when matched then update set gpa=6;
