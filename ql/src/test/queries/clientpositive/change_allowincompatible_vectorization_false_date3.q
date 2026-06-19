--! qt:dataset:alltypesorc

set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.tez.bucket.pruning=true;
set hive.optimize.index.filter=true;
set hive.metastore.disallow.incompatible.col.type.changes=false;

create table change_allowincompatible_vectorization_false_date (ts timestamp) partitioned by (s string) clustered by (ts) into 32 buckets stored as orc tblproperties ('transactional'='true');

alter table change_allowincompatible_vectorization_false_date add partition(s='aaa');

alter table change_allowincompatible_vectorization_false_date add partition(s='bbb');

insert into table change_allowincompatible_vectorization_false_date partition (s='aaa') values ('0001-01-01 00:00:00.0');

select ts from change_allowincompatible_vectorization_false_date where ts='0001-01-01 00:00:00.0' and s='aaa';

set hive.vectorized.execution.enabled=true;
select ts from change_allowincompatible_vectorization_false_date where ts='0001-01-01 00:00:00.0' and s='aaa';
