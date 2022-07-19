set hive.mapred.mode=nonstrict;
set hive.stats.autogather=false;
set hive.optimize.union.remove=true;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

set hive.exec.dynamic.partition=true;

-- SORT_QUERY_RESULTS
-- This is to test the union->selectstar->filesink optimization
-- Union of 2 map-reduce subqueries is performed followed by select star and a file sink
-- and the results are written to a table using dynamic partitions.
-- There is no need for this optimization, since the query is a map-only query.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- off
-- Since this test creates sub-directories for the output table outputTbl1_n4, it might be easier
-- to run the test only on hadoop 23

create table inputTbl1_n3(key string, val string) stored as textfile;
create table outputTbl1_n4(key string, `values` bigint) partitioned by (ds string) stored as rcfile;

load data local inpath '../../data/files/T1.txt' into table inputTbl1_n3;

explain
insert overwrite table outputTbl1_n4 partition (ds)
SELECT *
FROM (
  SELECT key, 1 as `values`, '1' as ds from inputTbl1_n3
  UNION ALL
  SELECT key, 2 as `values`, '2' as ds from inputTbl1_n3
) a;

insert overwrite table outputTbl1_n4 partition (ds)
SELECT *
FROM (
  SELECT key, 1 as `values`, '1' as ds from inputTbl1_n3
  UNION ALL
  SELECT key, 2 as `values`, '2' as ds from inputTbl1_n3
) a;

desc formatted outputTbl1_n4;
show partitions outputTbl1_n4;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1_n4 where ds = '1';
select * from outputTbl1_n4 where ds = '2';
