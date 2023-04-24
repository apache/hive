--! qt:dataset:alltypesorc
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.mapred.mode=nonstrict;
set hive.strict.checks.bucketing=true;
set hive.enforce.bucketing=true;

create table insert_only(col1 Int, col2 String) stored as orc  TBLPROPERTIES ('transactional'='true','transactional_properties'='insert_only');
insert into insert_only values(1,'hi'),(2,'hello');
describe formatted insert_only;
ALTER TABLE insert_only convert to acid TBLPROPERTIES ('transactional_properties'='default');
describe formatted insert_only;
insert into insert_only values(1,'hi'),(2,'hello');
select * from insert_only;

create table insert_only_1(col1 Int, col2 String) stored as orc  TBLPROPERTIES ('transactional'='true','transactional_properties'='insert_only');
insert into insert_only_1 values(1,'hi'),(2,'hello');
describe formatted insert_only_1;
ALTER TABLE insert_only SET TBLPROPERTIES ('transactional_properties'='default');
describe formatted insert_only_1;
insert into insert_only_1 values(1,'hi'),(2,'hello');
select * from insert_only_1;

create table insert_only_part(a INT, b STRING) partitioned by (ds string) CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
insert into table insert_only_part partition (ds = 'today') select cint, cstring1 from alltypesorc where cint is not null order by cint limit 10;
insert into table insert_only_part partition (ds = 'tomorrow') select cint, cstring1 from alltypesorc where cint is not null order by cint limit 10;
describe formatted insert_only_part;
ALTER TABLE insert_only_part SET TBLPROPERTIES ('transactional'='true','transactional_properties'='default');
describe formatted insert_only_part;
select * from insert_only_part order by a, b, ds;

