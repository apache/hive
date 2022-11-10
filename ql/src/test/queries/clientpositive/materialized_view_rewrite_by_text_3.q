set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting=false;

create table t1(col0 string) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

create materialized view mat1 as
SELECT * FROM t1 WHERE col0 = 'foo';

-- Constant literal is upper case -> no rewrite
explain cbo
SELECT * FROM t1 WHERE col0 = 'FOO';

explain cbo
SELECT * FROM t1 WHERE col0 = 'foo';

drop materialized view mat1;
