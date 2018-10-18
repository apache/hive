set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
-- enable ppd
set hive.optimize.index.filter=true;

set hive.explain.user=false;

-- needed for TestCliDriver but not TestMiniTezCliDriver
-- set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- attempts to get compaction to run - see below
-- set yarn.scheduler.maximum-allocation-mb=2024;
-- set hive.tez.container.size=500;
-- set mapreduce.map.memory.mb=500;
-- set mapreduce.reduce.memory.mb=500;
-- set mapred.job.map.memory.mb=500;
-- set mapred.job.reduce.memory.mb=500;



CREATE TEMPORARY FUNCTION runWorker AS 'org.apache.hadoop.hive.ql.udf.UDFRunWorker';
create table mydual_n0(a int);
insert into mydual_n0 values(1);

CREATE TABLE over10k_n9(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

--oddly this has 9999 rows not > 10K
LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE over10k_n9;

CREATE TABLE over10k_orc_bucketed_n0(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary) CLUSTERED BY(si) INTO 4 BUCKETS STORED AS ORC;

-- this produces about 250 distinct values across all 4 equivalence classes
select distinct si, si%4 from over10k_n9 order by si;

-- explain insert into over10k_orc_bucketed_n0 select * from over10k_n9;
insert into over10k_orc_bucketed_n0 select * from over10k_n9;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/over10k_orc_bucketed_n0;
-- create copy_N files
insert into over10k_orc_bucketed_n0 select * from over10k_n9;

-- this output of this is masked in .out - it is visible in .orig
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/over10k_orc_bucketed_n0;

--this actually shows the data files in the .out on Tez but not LLAP
select distinct 7 as seven, INPUT__FILE__NAME from over10k_orc_bucketed_n0;

-- convert table to acid
alter table over10k_orc_bucketed_n0 set TBLPROPERTIES ('transactional'='true');

-- this should vectorize (and push predicate to storage: filterExpr in TableScan )
--             Execution mode: vectorized (both Map and Reducer)
explain select t, si, i from over10k_orc_bucketed_n0 where b = 4294967363 and t < 100 order by t, si, i;
select t, si, i from over10k_orc_bucketed_n0 where b = 4294967363 and t < 100 order by  t, si, i;

-- this should vectorize (and push predicate to storage: filterExpr in TableScan )
--            Execution mode: vectorized
explain select ROW__ID, t, si, i from over10k_orc_bucketed_n0 where b = 4294967363 and t < 100 order by ROW__ID;
select ROW__ID, t, si, i from over10k_orc_bucketed_n0 where b = 4294967363 and t < 100 order by ROW__ID;

-- this should vectorize (and push predicate to storage: filterExpr in TableScan )
-- same as above
explain update over10k_orc_bucketed_n0 set i = 0 where b = 4294967363 and t < 100;
update over10k_orc_bucketed_n0 set i = 0 where b = 4294967363 and t < 100;

-- this should produce the same result (data) as previous time this exact query ran
-- ROW__ID will be different (same bucketProperty)
select ROW__ID, t, si, i from over10k_orc_bucketed_n0 where b = 4294967363 and t < 100 order by ROW__ID;

-- The idea below was to do check sum queries to ensure that ROW__IDs are unique
-- to run Compaction and to check that ROW__IDs are the same before and after compaction (for rows
-- w/o applicable delete events)

-- this doesn't vectorize
-- use explain VECTORIZATION DETAIL to see
-- notVectorizedReason: Key expression for GROUPBY operator: Vectorizing complex type STRUCT not supported
explain select ROW__ID, count(*) from over10k_orc_bucketed_n0 group by ROW__ID having count(*) > 1;

-- this test that there are no duplicate ROW__IDs so should produce no output
select ROW__ID, count(*) from over10k_orc_bucketed_n0 group by ROW__ID having count(*) > 1;

-- schedule compactor
alter table over10k_orc_bucketed_n0 compact 'major' WITH OVERWRITE TBLPROPERTIES ('compactor.mapreduce.map.memory.mb'='500', 'compactor.mapreduce.reduce.memory.mb'='500','compactor.mapreduce.map.memory.mb'='500', 'compactor.hive.tez.container.size'='500');


--  run compactor - this currently fails with
-- Invalid resource request, requested memory < 0, or requested memory > max configured, requestedMemory=1536, maxMemory=512
-- select runWorker() from mydual_n0;

-- show compactions;

-- this should produce the same (data + ROW__ID) as before compaction
select ROW__ID, t, si, i from over10k_orc_bucketed_n0 where b = 4294967363 and t < 100 order by ROW__ID;

-- this test that there are no duplicate ROW__IDs so should produce no output
select ROW__ID, count(*) from over10k_orc_bucketed_n0 group by ROW__ID having count(*) > 1;
