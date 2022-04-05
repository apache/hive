set hive.vectorized.execution.enabled=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists tbl_ice;
create external table tbl_ice(a int, b string, c int) partitioned by spec (bucket(16, a), truncate(3, b)) stored by iceberg tblproperties ('format-version'='2');

-- delete using simple predicates
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
delete from tbl_ice where b in ('one', 'four') or a = 22;
