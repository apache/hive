--! qt:dataset:src
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;

-- set the blob scheme as hdfs & file, as one of the test spins up the cluster with file scheme & one with hdfs, so
-- should cover both
set hive.blobstore.supported.schemes=hdfs,file;


-- try CTAS with non-empty external table
create external table t1 (key int, value string);

insert into t1 values (1,'ABCD'),(2, 'BCDE');

insert into t1 values (3,'FGHI'),(4, 'JKLM');

insert into t1 values (5,'NOPQ'),(6, 'RSTUV');

create external table t1_ctas as select * from t1;

select * from t1_ctas order by key;

create external table t1_ctas_part partitioned by (key) as select * from t1;

select * from t1_ctas_part order by key;

-- try CTAS with empty external table.

create external table t2 (key int, value string);

create external table t2_ctas as select * from t2;

select * from t2_ctas order by key;

--sanity check, the managed tables CTAS shouldn't get affected by the optimisation
create table t3 (key int, value string)  stored as orc tblproperties ('transactional'='true');

insert into t3 values (1,'ABC'),(2, 'BCD');

insert into t3 values (3,'ABC'),(4, 'BCD');

insert into t3 values (5,'ABC'),(6, 'BCD');

create table t3_ctas stored as orc tblproperties ('transactional'='true') as select * from t3;

select * from t3_ctas order by key;

-- try CTAS with target as external and source as managed.

create external table texternal as select * from t3;

select * from texternal order by key;

-- sanity check: try CTAS with target as managed and source as external.

create table tmanaged stored as orc tblproperties ('transactional'='true') as select * from t1;

select * from tmanaged order by key;

-- cleanup

drop table t1;
drop table t2;
drop table t3;
drop table t1_ctas;
drop table t2_ctas;
drop table t3_ctas;
drop table texternal;
drop table tmanaged;
