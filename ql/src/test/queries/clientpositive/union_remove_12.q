set hive.mapred.mode=nonstrict;
set hive.stats.autogather=false;
set hive.optimize.union.remove=true;
set hive.auto.convert.join=true;

set hive.merge.sparkfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=1;
set mapred.input.dir.recursive=true;

-- SORT_QUERY_RESULTS
-- This is to test the union->selectstar->filesink optimization
-- Union of 2 subqueries is performed (one of which is a map-only query, and the
-- other one is a map-join query), followed by select star and a file sink.
-- The union optimization is applied, and the union is removed.

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output table outputTbl1_n29, it might be easier
-- to run the test only on hadoop 23

-- The final file format is different from the input and intermediate file format.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- on

create table inputTbl1_n21(key string, val string) stored as textfile;
create table outputTbl1_n29(key string, `values` bigint) stored as rcfile;

load data local inpath '../../data/files/T1.txt' into table inputTbl1_n21;

explain
insert overwrite table outputTbl1_n29
SELECT * FROM
(
select key, 1 as `values` from inputTbl1_n21
union all
select a.key as key, cast(b.val as bigint) as `values`
FROM inputTbl1_n21 a join inputTbl1_n21 b on a.key=b.key
)c;

insert overwrite table outputTbl1_n29
SELECT * FROM
(
select key, 1 as `values` from inputTbl1_n21
union all
select a.key as key, cast(b.val as bigint) as `values`
FROM inputTbl1_n21 a join inputTbl1_n21 b on a.key=b.key
)c;

desc formatted outputTbl1_n29;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1_n29;
