--! qt:dataset:src

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table rct1_1 (key string, value string) stored as orc tblproperties ('transactional'='true');

insert into rct1_1 select * from default.src;

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=false;

explain
select count(*) from rct1_1;
select count(*) from rct1_1;

set test.comment="Query on transactional table should use cache";
set test.comment;
explain
select count(*) from rct1_1;
select count(*) from rct1_1;

truncate table rct1_1;

set test.comment="Table truncated - query cache invalidated";
set test.comment;
explain
select count(*) from rct1_1;
select count(*) from rct1_1;

create table rct1_2 (key string, value string) partitioned by (p1 string) stored as orc tblproperties ('transactional'='true');

insert into rct1_2 partition (p1='part1') select * from default.src;
insert into rct1_2 partition (p1='part2') select * from default.src;

explain
select count(*) from rct1_2;
select count(*) from rct1_2;

set test.comment="Query on transactional table should use cache";
set test.comment;
explain
select count(*) from rct1_2;
select count(*) from rct1_2;

truncate table rct1_2 partition (p1='part1');

set test.comment="Partition truncated - query cache invalidated";
set test.comment;
explain
select count(*) from rct1_2;
select count(*) from rct1_2;

truncate table rct1_2;

set test.comment="Table truncated - query cache invalidated";
set test.comment;
explain
select count(*) from rct1_2;
select count(*) from rct1_2;

