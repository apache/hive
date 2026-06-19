set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.vectorized.execution.enabled=true;
set hive.explain.user=true;
set hive.metastore.fastpath=false;
set hive.fetch.task.conversion=none;

drop table if exists char_part_tbl1 ;
drop table if exists char_part_tbl2;

create table studenttab(name string, age int, gpa double) clustered by (age) into 2 buckets stored as orc tblproperties('transactional'='true');
insert into table studenttab values ('calvin garcia',56,2.50), ('oscar miller',66,3.00), ('(yuri xylophone',30,2.74),('alice underhill',46,3.50);

create table char_tbl1(name string, age int) partitioned  by(gpa char(50)) stored as orc;
create table char_tbl2(name string, age int) partitioned by(gpa char(5)) stored as orc;

insert into table char_tbl1 partition(gpa='3.5') select name, age from studenttab where gpa = 3.5;
insert into table char_tbl1 partition(gpa='2.5') select name, age from studenttab where gpa = 2.5;
insert into table char_tbl2 partition(gpa='3.5') select name, age from studenttab where gpa = 3.5;
insert into table char_tbl2 partition(gpa='3') select name, age from studenttab where gpa = 3;

show partitions char_tbl1;
show partitions char_tbl2;

explain vectorization select c1.name, c1.age, c1.gpa, c2.name, c2.age, c2.gpa from char_tbl1 c1 join char_tbl2 c2 on (c1.gpa = c2.gpa);
select c1.name, c1.age, c1.gpa, c2.name, c2.age, c2.gpa from char_tbl1 c1 join char_tbl2 c2 on (c1.gpa = c2.gpa);

set hive.vectorized.execution.enabled=false;
select c1.name, c1.age, c1.gpa, c2.name, c2.age, c2.gpa from char_tbl1 c1 join char_tbl2 c2 on (c1.gpa = c2.gpa);
