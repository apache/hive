--! qt:dataset:src
set hive.metastore.event.listeners=org.apache.hive.hcatalog.listener.DbNotificationListener;

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=true;

set hive.fetch.task.conversion=more;
-- Start polling the notification events
set hive.notification.event.poll.interval=2s;
select reflect('org.apache.hadoop.hive.ql.QTestUtil', 'initEventNotificationPoll');

create table tab1 stored as textfile as select * from src;
insert into tab1 select * from src;

create table tab2 (key string, value string) stored as textfile;
insert into tab2 select * from src;

-- Run queries which should be saved by the cache.
select count(*) from tab1 where key > 0;
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);
select count(*) from tab2 where key > 0;

set test.comment="Cached entry should be used";
set test.comment;
explain
select count(*) from tab1 where key > 0;
select count(*) from tab1 where key > 0;

set test.comment="Cached entry should be used";
set test.comment;
explain
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);

set test.comment="Cached entry should be used";
set test.comment;
explain
select count(*) from tab2 where key > 0;
select count(*) from tab2 where key > 0;

-- Update tab1
insert into tab1 select * from src;

-- Run a query long enough that the invalidation check can run.
select reflect("java.lang.Thread", 'sleep', cast(10000 as bigint));

set test.comment="Cached entry should be invalidated - query should not use cache";
set test.comment;
explain
select count(*) from tab1 where key > 0;
select count(*) from tab1 where key > 0;

set test.comment="Cached entry should be invalidated - query should not use cache";
set test.comment;
explain
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);
select count(*) from tab1 join tab2 on (tab1.key = tab2.key);

set test.comment="tab2 was not modified, this query should still use cache";
set test.comment;
explain
select count(*) from tab2 where key > 0;
select count(*) from tab2 where key > 0;
