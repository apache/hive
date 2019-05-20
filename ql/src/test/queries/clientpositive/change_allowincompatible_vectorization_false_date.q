--! qt:dataset:alltypesorc

set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.tez.bucket.pruning=true;
set hive.optimize.index.filter=true;
set hive.metastore.disallow.incompatible.col.type.changes=false;

create table change_allowincompatible_vectorization_false_date (ts date) partitioned by (s string) clustered by (ts) into 32 buckets stored as orc tblproperties ('transactional'='true');

alter table change_allowincompatible_vectorization_false_date add partition(s='aaa');

alter table change_allowincompatible_vectorization_false_date add partition(s='bbb');

insert into table change_allowincompatible_vectorization_false_date partition (s='aaa') select ctimestamp1 from alltypesorc where ctimestamp1 > '2000-01-01' limit 50;

insert into table change_allowincompatible_vectorization_false_date partition (s='bbb') select ctimestamp1 from alltypesorc where ctimestamp1 < '2000-01-01' limit 50;

select count(*) from change_allowincompatible_vectorization_false_date;

alter table change_allowincompatible_vectorization_false_date change column ts ts timestamp;

select count(*) from change_allowincompatible_vectorization_false_date;

insert into table change_allowincompatible_vectorization_false_date partition (s='aaa') values ('2038-03-22 07:26:48.0');

select ts from change_allowincompatible_vectorization_false_date where ts='2038-03-22 07:26:48.0' and s='aaa';

