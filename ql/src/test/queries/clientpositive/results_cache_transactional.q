--! qt:dataset:src
--! qt:replace:/(Data size: )\d+/$1#MASKED#/
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table tab1_n1 (key string, value string) stored as orc tblproperties ('transactional'='true');
create table tab2_n1 (key string, value string) stored as orc tblproperties ('transactional'='true');

insert into tab1_n1 select * from default.src;
insert into tab2_n1 select * from default.src;

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=false;

explain
select max(key) from tab1_n1;
select max(key) from tab1_n1;

set test.comment="Query on transactional table should use cache";
set test.comment;
explain
select max(key) from tab1_n1;
select max(key) from tab1_n1;

explain
select count(*) from tab1_n1 join tab2_n1 on (tab1_n1.key = tab2_n1.key);
select count(*) from tab1_n1 join tab2_n1 on (tab1_n1.key = tab2_n1.key);

set test.comment="Join on transactional tables, should use cache";
set test.comment;
explain
select count(*) from tab1_n1 join tab2_n1 on (tab1_n1.key = tab2_n1.key);
select count(*) from tab1_n1 join tab2_n1 on (tab1_n1.key = tab2_n1.key);


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
select count(*) from tab1_n1 join src on (tab1_n1.key = src.key);
select count(*) from tab1_n1 join src on (tab1_n1.key = src.key);

set test.comment="Join uses non-transactional table, should not use cache";
set test.comment;
explain
select count(*) from tab1_n1 join src on (tab1_n1.key = src.key);
select count(*) from tab1_n1 join src on (tab1_n1.key = src.key);

-- view on transactional tables
create view join_count_transactional_view as select count(*) from tab1_n1 join tab2_n1 on (tab1_n1.key = tab2_n1.key);
explain select * from join_count_transactional_view;
select * from join_count_transactional_view;
set test.comment="View on transactional tables, should use cache";
set test.comment;
explain select * from join_count_transactional_view;
select * from join_count_transactional_view;
insert into tab1_n1 select * from default.src limit 1;
set test.comment="Cache entry should be invalidated from prior insert, should not use cache";
set test.comment;
explain select * from join_count_transactional_view;

-- view with non-transactional tables
create view join_count_view as select count(*) from tab1_n1 join src on (tab1_n1.key = src.key);
explain select * from join_count_view;
select * from join_count_view;
set test.comment="View with non-transactional tables, should not use cache";
set test.comment;
explain select * from join_count_view;
select * from join_count_view;
