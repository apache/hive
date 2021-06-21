set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

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
create table mydual(a int);
insert into mydual values(1);

CREATE TABLE over10k_n2(t tinyint,
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
LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE over10k_n2;

CREATE TABLE over10k_orc_bucketed(t tinyint,
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
select distinct si, si%4 from over10k_n2 order by si;

-- explain insert into over10k_orc_bucketed select * from over10k_n2;
insert into over10k_orc_bucketed select * from over10k_n2;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/over10k_orc_bucketed;
-- create copy_N files
insert into over10k_orc_bucketed select * from over10k_n2;

-- this output of this is masked in .out - it is visible in .orig
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/over10k_orc_bucketed;

--this actually shows the data files in the .out on Tez but not LLAP
select distinct 7 as seven, INPUT__FILE__NAME from over10k_orc_bucketed;

-- convert table to acid
alter table over10k_orc_bucketed set TBLPROPERTIES ('transactional'='true');

-- this should vectorize (and push predicate to storage: filterExpr in TableScan )
--             Execution mode: vectorized, llap
--             LLAP IO: may be used (ACID table)
explain select t, si, i from over10k_orc_bucketed where b = 4294967363 and t < 100 order by t, si, i;
select t, si, i from over10k_orc_bucketed where b = 4294967363 and t < 100 order by  t, si, i;

-- this should vectorize (and push predicate to storage: filterExpr in TableScan )
--             Execution mode: vectorized, llap
--             LLAP IO: may be used (ACID table)
explain select ROW__ID, t, si, i from over10k_orc_bucketed where b = 4294967363 and t < 100 order by ROW__ID;
-- HIVE-17943
select ROW__ID, t, si, i from over10k_orc_bucketed where b = 4294967363 and t < 100 order by ROW__ID;

-- this should vectorize (and push predicate to storage: filterExpr in TableScan )
--            Execution mode: vectorized, llap
--            LLAP IO: may be used (ACID table)
explain update over10k_orc_bucketed set i = 0 where b = 4294967363 and t < 100;
update over10k_orc_bucketed set i = 0 where b = 4294967363 and t < 100;

-- this should produce the same result (data) as previous time this exact query ran
-- ROW__ID will be different (same bucketProperty)
select ROW__ID, t, si, i from over10k_orc_bucketed where b = 4294967363 and t < 100 order by ROW__ID;

-- The idea below was to do check sum queries to ensure that ROW__IDs are unique
-- to run Compaction and to check that ROW__IDs are the same before and after compaction (for rows
-- w/o applicable delete events)
-- Everything below is commented out because it doesn't work
-- group by ROW__ID produces wrong results
-- compactor doesn't run - see error below...

-- this doesn't vectorize but use llap which is perhaps the problem if it's using cache
-- use explain VECTORIZATION DETAIL to see
-- notVectorizedReason: Key expression for GROUPBY operator: Vectorizing complex type STRUCT not supported
explain select ROW__ID, count(*) from over10k_orc_bucketed group by ROW__ID having count(*) > 1;

-- this test that there are no duplicate ROW__IDs so should produce no output
-- on LLAP this produces "NULL, 6"; on tez it produces nothing: HIVE-17921
-- this makes sure that the same code is running on the Ptest and on localhost. The target is:
-- Original split count is 11 grouped split count is 1, for bucket: 1
set tez.grouping.split-count=1;

select ROW__ID, count(*) from over10k_orc_bucketed group by ROW__ID having count(*) > 1;
-- this produces nothing (as it should)
select ROW__ID, * from over10k_orc_bucketed where ROW__ID is null;

-- schedule compactor
-- alter table over10k_orc_bucketed compact 'major' WITH OVERWRITE TBLPROPERTIES ("compactor.mapreduce.map.memory.mb"="500","compactor.hive.tez.container.size"="500");;


-- run compactor - this currently fails with
-- Invalid resource request, requested memory < 0, or requested memory > max configured, requestedMemory=1536, maxMemory=512
-- HIVE-17922
-- select runWorker() from mydual;

-- show compactions;

-- this should produce the same result (data+ ROW__ID) as previous time this exact query ran
-- select ROW__ID, t, si, i from over10k_orc_bucketed where b = 4294967363 and t < 100 order by ROW__ID;

-- this test that there are no duplicate ROW__IDs so should produce no output
-- select ROW__ID, count(*) from over10k_orc_bucketed group by ROW__ID having count(*) > 1;

-- select ROW__ID, * from over10k_orc_bucketed where ROW__ID is null;

-- TODO: Remove this line after fixing HIVE-24351
set hive.vectorized.execution.enabled=false;

CREATE TABLE over10k_orc STORED AS ORC as select * from over10k_n2 where t between 3 and 4;

-- Make sure there are multiple original files
INSERT INTO over10k_orc select * from over10k_n2 where t between 3 and 4;

-- TODO: Remove this line after fixing HIVE-24351
set hive.vectorized.execution.enabled=true;

alter table over10k_orc set TBLPROPERTIES ('transactional'='true');

-- row id is projected but there are no delete deltas
set hive.exec.orc.split.strategy=ETL;
select o1.ROW__ID r1, o1.* from over10k_orc o1 join over10k_orc o2
on o1.ROW__ID.rowid == o2.ROW__ID.rowid and o1.ROW__ID.writeid == o2.ROW__ID.writeid and o1.ROW__ID.bucketid == o2.ROW__ID.bucketid;

set hive.exec.orc.split.strategy=BI;
select o1.ROW__ID r1, o1.* from over10k_orc o1 join over10k_orc o2
on o1.ROW__ID.rowid == o2.ROW__ID.rowid
and o1.ROW__ID.writeid == o2.ROW__ID.writeid
and o1.ROW__ID.bucketid == o2.ROW__ID.bucketid;

delete from over10k_orc where t = 3;

-- row id not projected but has delete deltas
set hive.exec.orc.split.strategy=ETL;
select t, count(*) from over10k_orc
group by t;

set hive.exec.orc.split.strategy=BI;
select t, count(*) from over10k_orc
group by t;

select oo.ROW__ID.writeId, oo.ROW__IS__DELETED, oo.* from over10k_orc('acid.fetch.deleted.rows'='true') oo order by si;