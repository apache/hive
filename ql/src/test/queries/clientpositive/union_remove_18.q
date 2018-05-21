set hive.mapred.mode=nonstrict;
set hive.stats.autogather=false;
set hive.optimize.union.remove=true;

set hive.merge.sparkfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set mapred.input.dir.recursive=true;

-- SORT_QUERY_RESULTS
-- This is to test the union->selectstar->filesink optimization
-- Union of 2 map-reduce subqueries is performed followed by select star and a file sink
-- There is no need to write the temporary results of the sub-queries, and then read them 
-- again to process the union. The union can be removed completely.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- off
-- This test demonstrates that the optimization works with dynamic partitions irrespective of the
-- file format of the output file
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output table outputTbl1_n30, it might be easier
-- to run the test only on hadoop 23

create table inputTbl1_n22(key string, ds string) stored as textfile;
create table outputTbl1_n30(key string, `values` bigint) partitioned by (ds string) stored as textfile;

load data local inpath '../../data/files/T1.txt' into table inputTbl1_n22;

explain
insert overwrite table outputTbl1_n30 partition (ds)
SELECT *
FROM (
  SELECT key, count(1) as `values`, ds from inputTbl1_n22 group by key, ds
  UNION ALL
  SELECT key, count(1) as `values`, ds from inputTbl1_n22 group by key, ds
) a;

insert overwrite table outputTbl1_n30 partition (ds)
SELECT *
FROM (
  SELECT key, count(1) as `values`, ds from inputTbl1_n22 group by key, ds
  UNION ALL
  SELECT key, count(1) as `values`, ds from inputTbl1_n22 group by key, ds
) a;

desc formatted outputTbl1_n30;

show partitions outputTbl1_n30;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1_n30 where ds = '11';
select * from outputTbl1_n30 where ds = '18';
select * from outputTbl1_n30 where ds is not null;
