set hive.stats.column.autogather=false;
set hive.stats.estimate=true;
set hive.stats.fetch.column.stats=true;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table t1 (cint INT, cbigint BIGINT, cts timestamp) stored as orc TBLPROPERTIES ('transactional'='true');

insert into table t1 values
    (1, 1, null),
    (2, 2, null),
    (3, 3, null),
    (4, 4, null),
    (5, 5, null),
    (-1070551680, 1704034800000, '2025-06-25 10:00:00.000'),
    (-1070551680, 1704034800000, '2025-06-25 20:00:00.000'),
    (-1070551680, 1704034800000, '2025-06-25 10:30:00.000'),
    (-1070551680, 1704034800000, '2025-06-25 16:30:00.000');

set hive.fetch.task.conversion=none;

explain analyze
select * from t1 where cint < -1070551679;
explain analyze
select * from t1 where cbigint >= 1704034800000;
explain analyze
select * from t1 where cts > '2025-06-25 10:00:00.000';

analyze table t1 compute statistics for columns;

explain
select * from t1 where cint < -1070551679;
explain
select * from t1 where cbigint >= 1704034800000;
explain
select * from t1 where cbigint >= 10;
explain
select * from t1 where cts > '2025-06-25 10:00:00.000';
