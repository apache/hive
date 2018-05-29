set hive.mapred.mode=nonstrict;
set hive.stats.autogather=false;
set hive.optimize.union.remove=true;

set hive.merge.sparkfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=1;
set mapred.input.dir.recursive=true;

-- SORT_QUERY_RESULTS
-- This is to test the union->selectstar->filesink optimization
-- Union of 2 subqueries is performed (one of which is a map-only query, and the
-- other one contains a nested union where also contains map only sub-queries),
-- followed by select star and a file sink.
-- There is no need for the union optimization, since the whole query can be performed
-- in a single map-only job
-- The final file format is different from the input and intermediate file format.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- on

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output table outputTbl1_n21, it might be easier
-- to run the test only on hadoop 23

create table inputTbl1_n14(key string, val string) stored as textfile;
create table outputTbl1_n21(key string, `values` bigint) stored as rcfile;

load data local inpath '../../data/files/T1.txt' into table inputTbl1_n14;

explain
insert overwrite table outputTbl1_n21
SELECT * FROM
(
select key, 1 as `values` from inputTbl1_n14
union all
select * FROM (
  SELECT key, 2 `values` from inputTbl1_n14 
  UNION ALL
  SELECT key, 3 as `values` from inputTbl1_n14
) a
)b;

insert overwrite table outputTbl1_n21
SELECT * FROM
(
select key, 1 as `values` from inputTbl1_n14
union all
select * FROM (
  SELECT key, 2 as `values` from inputTbl1_n14 
  UNION ALL
  SELECT key, 3 as `values` from inputTbl1_n14
) a
)b;

desc formatted outputTbl1_n21;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1_n21;
