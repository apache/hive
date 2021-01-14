set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting=false;

create table t1(col0 string) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

insert into t1(col0) values('foo');

create materialized view mat1 as
SELECT * FROM t1 WHERE col0 = 'foo';

-- mv is used
explain cbo
SELECT * FROM t1 WHERE col0 = 'foo';

insert into t1(col0) values('bar');

-- mv is outdated -> not used
explain cbo
SELECT * FROM t1 WHERE col0 = 'foo';

ALTER MATERIALIZED VIEW mat1 REBUILD;

-- mv is up to date -> used
explain cbo
SELECT * FROM t1 WHERE col0 = 'foo';

drop materialized view mat1;
