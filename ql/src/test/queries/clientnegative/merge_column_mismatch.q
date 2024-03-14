set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.explain.user=false;
set hive.merge.cardinality.check=true;

create table t (a int, b int default 1) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table upd_t (a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into t values (1,2), (2,4);
insert into upd_t values (1,3), (3,5);

merge into t as t using upd_t as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = default
WHEN NOT MATCHED THEN INSERT (a) VALUES(u.a, default);
