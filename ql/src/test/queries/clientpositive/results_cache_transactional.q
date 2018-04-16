
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table tab1 (key string, value string) stored as orc tblproperties ('transactional'='true');
create table tab2 (key string, value string) stored as orc tblproperties ('transactional'='true');

insert into tab1 select * from default.src;
insert into tab2 select * from default.src;

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=false;

explain
select max(key) from tab1;
select max(key) from tab1;

set test.comment="Query on transactional table should use cache";
set test.comment;
explain
select max(key) from tab1;
select max(key) from tab1;

explain
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);

set test.comment="Join on transactional tables, should use cache";
set test.comment;
explain
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);


-- Non-transactional tables

explain
select max(key) from src;
select max(key) from src;

set test.comment="Query on non-transactional table should not use cache";
set test.comment;
explain
select max(key) from src;
select max(key) from src;

explain
select count(*) from tab1 join src on (tab1.key = src.key);
select count(*) from tab1 join src on (tab1.key = src.key);

set test.comment="Join uses non-transactional table, should not use cache";
set test.comment;
explain
select count(*) from tab1 join src on (tab1.key = src.key);
select count(*) from tab1 join src on (tab1.key = src.key);

