set hive.mapred.mode=nonstrict;
set hive.stats.autogather=false;
set hive.optimize.union.remove=true;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

-- SORT_QUERY_RESULTS
-- This is to test the union->selectstar->filesink optimization
-- Union of 2 map-reduce subqueries is performed followed by select star and a file sink
-- There is no need to write the temporary results of the sub-queries, and then read them 
-- again to process the union. The union can be removed completely.
-- The final file format is different from the input and intermediate file format.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- off

-- Since this test creates sub-directories for the output table outputTbl1_n24, it might be easier
-- to run the test only on hadoop 23

create table inputTbl1_n17(key string, val string) stored as textfile;
create table outputTbl1_n24(key string, `values` bigint) stored as rcfile;

load data local inpath '../../data/files/T1.txt' into table inputTbl1_n17;

explain
insert overwrite table outputTbl1_n24
SELECT *
FROM (
  SELECT key, count(1) as `values` from inputTbl1_n17 group by key
  UNION ALL
  SELECT key, count(1) as `values` from inputTbl1_n17 group by key
) a;

insert overwrite table outputTbl1_n24
SELECT *
FROM (
  SELECT key, count(1) as `values` from inputTbl1_n17 group by key
  UNION ALL
  SELECT key, count(1) as `values` from inputTbl1_n17 group by key
) a;

desc formatted outputTbl1_n24;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1_n24;
