-- A table has a column named 'default' and try to assign it in an update
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
-- SORT_QUERY_RESULTS

-- with default constraint
CREATE TABLE t1 (a int, `default` int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1 values (1, 2), (10, 11);

explain
update t1 set a = `default`;
update t1 set a = `default`;

select * from t1;