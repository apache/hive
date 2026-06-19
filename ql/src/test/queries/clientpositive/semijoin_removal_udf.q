set hive.optimize.index.filter=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.vectorized.execution.enabled=true;

drop table if exists t1;
drop table if exists t2;

create table t1 (
	v1 string
);

create table t2 (
	v2 string
);

insert into t1 values ('e123456789'),('x123456789');
insert into t2 values
('123'),
 ('e123456789');


alter table t1 update statistics set ('numRows'='934884357','rawDataSize'='0');
alter table t2 update statistics set ('numRows'='9348','rawDataSize'='0');

alter table t1 update statistics for column v1 set ('numNulls'='0','numDVs'='15541355','avgColLen'='10.0','maxColLen'='10');
alter table t2 update statistics for column v2 set ('numNulls'='0','numDVs'='155','avgColLen'='5.0','maxColLen'='10');

explain
select v1,v2 from t1 join t2 on (substr(v1,1,3) = v2);
