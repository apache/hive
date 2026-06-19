--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;
set hive.stats.autogather=false;
set hive.optimize.union.remove=true;
set hive.exec.dynamic.partition=true;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

-- SORT_QUERY_RESULTS
-- This is to test the union->selectstar->filesink optimization
-- Union of 2 map-reduce subqueries is performed followed by select star and a file sink
-- There is no need to write the temporary results of the sub-queries, and then read them 
-- again to process the union. The union can be removed completely.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- off
-- Since this test creates sub-directories for the output table outputTbl1_n19, it might be easier
-- to run the test only on hadoop 23

create table inputTbl1_n13(key string, val string) stored as textfile;
create table outputTbl1_n19(key string, `values` bigint) partitioned by (ds string) stored as textfile;
create table outputTbl2_n6(key string, `values` bigint) partitioned by (ds string) stored as textfile;
create table outputTbl3_n3(key string, `values` bigint) partitioned by (ds string,hr string) stored as textfile;

load data local inpath '../../data/files/T1.txt' into table inputTbl1_n13;

explain
insert overwrite table outputTbl1_n19 partition(ds='2004')
SELECT *
FROM (
  SELECT key, count(1) as `values` from inputTbl1_n13 group by key
  UNION ALL
  SELECT key, count(1) as `values` from inputTbl1_n13 group by key
) a;

insert overwrite table outputTbl1_n19 partition(ds='2004')
SELECT *
FROM (
  SELECT key, count(1) as `values` from inputTbl1_n13 group by key
  UNION ALL
  SELECT key, count(1) as `values` from inputTbl1_n13 group by key
) a;

desc formatted outputTbl1_n19 partition(ds='2004');

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1_n19;

explain 
insert overwrite table outputTbl2_n6 partition(ds)
SELECT *
FROM (
  select * from (SELECT key, value, ds from srcpart where ds='2008-04-08' limit 500)a
  UNION ALL
  select * from (SELECT key, value, ds from srcpart where ds='2008-04-08' limit 500)b
) a;

insert overwrite table outputTbl2_n6 partition(ds)
SELECT *
FROM (
  select * from (SELECT key, value, ds from srcpart where ds='2008-04-08' limit 500)a
  UNION ALL
  select * from (SELECT key, value, ds from srcpart where ds='2008-04-08' limit 500)b
) a;

show partitions outputTbl2_n6;
desc formatted outputTbl2_n6 partition(ds='2008-04-08');

explain insert overwrite table outputTbl3_n3 partition(ds, hr)
SELECT *
FROM (
  select * from (SELECT key, value, ds, hr from srcpart where ds='2008-04-08' limit 1000)a
  UNION ALL
  select * from (SELECT key, value, ds, hr from srcpart where ds='2008-04-08' limit 1000)b
) a;

insert overwrite table outputTbl3_n3 partition(ds, hr)
SELECT *
FROM (
  select * from (SELECT key, value, ds, hr from srcpart where ds='2008-04-08' limit 1000)a
  UNION ALL
  select * from (SELECT key, value, ds, hr from srcpart where ds='2008-04-08' limit 1000)b
) a;

show partitions outputTbl3_n3;
desc formatted outputTbl3_n3 partition(ds='2008-04-08', hr='11');
