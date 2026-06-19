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
-- One sub-query has a double and the other sub-query has a bigint.
-- Since this test creates sub-directories for the output table outputTbl1_n28, it might be easier
-- to run the test only on hadoop 23

create table inputTbl1_n20(key string, val string) stored as textfile;
create table outputTbl1_n28(key double, `values` bigint) stored as textfile;

load data local inpath '../../data/files/T1.txt' into table inputTbl1_n20;

EXPLAIN
INSERT OVERWRITE TABLE outputTbl1_n28
SELECT * FROM
(
  SELECT CAST(key AS DOUBLE) AS key, count(1) as `values` FROM inputTbl1_n20 group by key
  UNION ALL
  SELECT CAST(key AS BIGINT) AS key, count(1) as `values` FROM inputTbl1_n20 group by key
) a;

INSERT OVERWRITE TABLE outputTbl1_n28
SELECT * FROM
(
  SELECT CAST(key AS DOUBLE) AS key, count(1) as `values` FROM inputTbl1_n20 group by key
  UNION ALL
  SELECT CAST(key AS BIGINT) AS key, count(1) as `values` FROM inputTbl1_n20 group by key
) a;

desc formatted outputTbl1_n28;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1_n28;
