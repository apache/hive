set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.support.concurrency=true;

create table t1(a int, b int) clustered by (a) into 2 buckets stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

insert into t1(a, b) values (1, 1), (10, 10);
insert into t1(a, b) values (2, 20);
insert into t1(a, b) values (10, 15),(2, 32),(42, 42);


set hive.vectorized.execution.enabled=false;

SELECT t1.ROW__ID, t1.ROW__ID.writeId, a, b FROM t1;
SELECT t1.ROW__ID, t1.ROW__ID.writeId, a, b FROM t1('insertonly.fetch.bucketid'='true');

SELECT a, sum(b) FROM t1
where t1.ROW__ID.writeId > 1
group by a;

SELECT a, sum(b) FROM t1('insertonly.fetch.bucketid'='true')
where t1.ROW__ID.writeId > 1
group by a;


set hive.vectorized.execution.enabled=true;

SELECT t1.ROW__ID, t1.ROW__ID.writeId, a, b FROM t1;
SELECT t1.ROW__ID, t1.ROW__ID.writeId, a, b FROM t1('insertonly.fetch.bucketid'='true');

SELECT a, sum(b) FROM t1
where t1.ROW__ID.writeId > 1
group by a;

SELECT a, sum(b) FROM t1('insertonly.fetch.bucketid'='true')
where t1.ROW__ID.writeId > 1
group by a;
