
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table tab1 (key string, value string) stored as orc tblproperties ('transactional'='true');
create table tab2 (key string, value string) stored as orc tblproperties ('transactional'='true');

insert into tab1 select * from default.src;
insert into tab2 select * from default.src;

set hive.query.results.cache.enabled=true;

set test.comment="Run queries to load into cache";
set test.comment;

-- Q1
explain
select count(*) from tab1 a where key >= 0;
select count(*) from tab1 a where key >= 0;

-- Q2
explain
select max(key) from tab2;
select max(key) from tab2;

-- Q3
explain
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);

set test.comment="Q1 should now be able to use cache";
set test.comment;
explain
select count(*) from tab1 a where key >= 0;
select count(*) from tab1 a where key >= 0;

set test.comment="Q2 should now be able to use cache";
set test.comment;
explain
select max(key) from tab2;
select max(key) from tab2;

set test.comment="Q3 should now be able to use cache";
set test.comment;
explain
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);

-- Update tab1 which should invalidate Q1 and Q3.
insert into tab1 values ('88', 'val_88');

set test.comment="Q1 should not use cache";
set test.comment;
explain
select count(*) from tab1 a where key >= 0;
select count(*) from tab1 a where key >= 0;

set test.comment="Q2 should still use cache since tab2 not updated";
set test.comment;
explain
select max(key) from tab2;
select max(key) from tab2;

set test.comment="Q3 should not use cache";
set test.comment;
explain
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);

-- Update tab2 which should invalidate Q2 and Q3.
insert into tab2 values ('88', 'val_88');

set test.comment="Q1 should use cache";
set test.comment;
explain
select count(*) from tab1 a where key >= 0;
select count(*) from tab1 a where key >= 0;

set test.comment="Q2 should not use cache";
set test.comment;
explain
select max(key) from tab2;
select max(key) from tab2;

set test.comment="Q3 should not use cache";
set test.comment;
explain
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);
