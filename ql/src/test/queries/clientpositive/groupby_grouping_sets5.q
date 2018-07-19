set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=false;
set hive.merge.mapfiles = false;
set hive.merge.mapredfiles = false;
-- Set merging to false above to make the explain more readable

CREATE TABLE T1_n24(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1_n24;

-- SORT_QUERY_RESULTS

-- This tests that cubes and rollups work fine where the source is a sub-query
EXPLAIN
SELECT a, b, count(*) FROM
(SELECT a, b, count(1) from T1_n24 group by a, b) subq1 group by a, b with cube;

EXPLAIN
SELECT a, b, count(*) FROM
(SELECT a, b, count(1) from T1_n24 group by a, b) subq1 group by cube(a, b);

SELECT a, b, count(*) FROM
(SELECT a, b, count(1) from T1_n24 group by a, b) subq1 group by a, b with cube;

set hive.new.job.grouping.set.cardinality=2;

-- Since 4 grouping sets would be generated for the cube, an additional MR job should be created
EXPLAIN
SELECT a, b, count(*) FROM
(SELECT a, b, count(1) from T1_n24 group by a, b) subq1 group by a, b with cube;

SELECT a, b, count(*) FROM
(SELECT a, b, count(1) from T1_n24 group by a, b) subq1 group by a, b with cube;
