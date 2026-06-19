--! qt:dataset:src

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table tab1_n6 (key string, value string) stored as orc tblproperties ('transactional'='true');
create table tab2_n5 (key string, value string) stored as orc tblproperties ('transactional'='true');

insert into tab1_n6 select * from default.src;
insert into tab2_n5 select * from default.src;

set hive.query.results.cache.enabled=true;

set test.comment="Run queries to load into cache";
set test.comment;

-- Q1
explain
select count(*) from tab1_n6 a where key >= 0;
select count(*) from tab1_n6 a where key >= 0;

-- Q2
explain
select max(key) from tab2_n5;
select max(key) from tab2_n5;

-- Q3
explain
select count(*) from tab1_n6 join tab2_n5 on (tab1_n6.key = tab2_n5.key);
select count(*) from tab1_n6 join tab2_n5 on (tab1_n6.key = tab2_n5.key);

set test.comment="Q1 should now be able to use cache";
set test.comment;
explain
select count(*) from tab1_n6 a where key >= 0;
select count(*) from tab1_n6 a where key >= 0;

set test.comment="Q2 should now be able to use cache";
set test.comment;
explain
select max(key) from tab2_n5;
select max(key) from tab2_n5;

set test.comment="Q3 should now be able to use cache";
set test.comment;
explain
select count(*) from tab1_n6 join tab2_n5 on (tab1_n6.key = tab2_n5.key);
select count(*) from tab1_n6 join tab2_n5 on (tab1_n6.key = tab2_n5.key);

-- Update tab1_n6 which should invalidate Q1 and Q3.
insert into tab1_n6 values ('88', 'val_88');

set test.comment="Q1 should not use cache";
set test.comment;
explain
select count(*) from tab1_n6 a where key >= 0;
select count(*) from tab1_n6 a where key >= 0;

set test.comment="Q2 should still use cache since tab2_n5 not updated";
set test.comment;
explain
select max(key) from tab2_n5;
select max(key) from tab2_n5;

set test.comment="Q3 should not use cache";
set test.comment;
explain
select count(*) from tab1_n6 join tab2_n5 on (tab1_n6.key = tab2_n5.key);
select count(*) from tab1_n6 join tab2_n5 on (tab1_n6.key = tab2_n5.key);

-- Update tab2_n5 which should invalidate Q2 and Q3.
insert into tab2_n5 values ('88', 'val_88');

set test.comment="Q1 should use cache";
set test.comment;
explain
select count(*) from tab1_n6 a where key >= 0;
select count(*) from tab1_n6 a where key >= 0;

set test.comment="Q2 should not use cache";
set test.comment;
explain
select max(key) from tab2_n5;
select max(key) from tab2_n5;

set test.comment="Q3 should not use cache";
set test.comment;
explain
select count(*) from tab1_n6 join tab2_n5 on (tab1_n6.key = tab2_n5.key);
select count(*) from tab1_n6 join tab2_n5 on (tab1_n6.key = tab2_n5.key);
