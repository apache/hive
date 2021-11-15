--! qt:dataset:src
set hive.stats.column.autogather=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=30000;

set hive.optimize.bucketingsorting=false;

set hive.optimize.ppd=true;
set hive.optimize.index.filter=true;
set hive.tez.bucket.pruning=true;
set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create transactional table acid_ptn_bucket1 (a int, b int) partitioned by(ds string)
clustered by (a) into 2 buckets stored as ORC
TBLPROPERTIES('bucketing_version'='1', 'transactional'='true', 'transactional_properties'='default');

explain extended insert into acid_ptn_bucket1 partition (ds) values(1,2,'today'),(1,3,'today'),(1,4,'yesterday'),(2,2,'yesterday'),(2,3,'today'),(2,4,'today');
insert into acid_ptn_bucket1 partition (ds) values(1,2,'today'),(1,3,'today'),(1,4,'yesterday'),(2,2,'yesterday'),(2,3,'today'),(2,4,'today');

alter table acid_ptn_bucket1 add columns(c int);

insert into acid_ptn_bucket1 partition (ds) values(3,2,1000,'yesterday'),(3,3,1001,'today'),(3,4,1002,'yesterday'),(4,2,1003,'today'), (4,3,1004,'yesterday'),(4,4,1005,'today');
select ROW__ID, * from acid_ptn_bucket1 where ROW__ID.bucketid = 536870912 and ds='today';
select ROW__ID, * from acid_ptn_bucket1 where ROW__ID.bucketid = 536936448 and ds='today';

--create table s1 as select key, value from src where value > 2 group by key, value limit 10;
--create table s2 as select key, '45' from src s2 where key > 1 group by key limit 10;

create table s1 (key int, value int) stored as ORC;
create table s2 (key int, value int) stored as ORC;

insert into s1 values(111, 33), (10, 45), (103, 44), (129, 34), (128, 11);
insert into s2 values(10, 45), (100, 45), (103, 44), (110, 12), (128, 34), (117, 71);
insert into table acid_ptn_bucket1 partition(ds='today') select key, count(value), key from (select * from s1 union all select * from s2) sub group by key;
select ROW__ID, * from acid_ptn_bucket1 where ROW__ID.bucketid = 536870912 and ds='today';
select ROW__ID, * from acid_ptn_bucket1 where ROW__ID.bucketid = 536936448 and ds='today';
