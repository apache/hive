set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.merge.mapfiles = false;
set hive.merge.mapredfiles = false;

-- SORT_QUERY_RESULTS

-- Set merging to false above to make the explain more readable

CREATE TABLE T1_text(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1_text;

CREATE TABLE T1 STORED AS ORC AS SELECT * FROM T1_text;

-- This tests that cubes and rollups work fine inside sub-queries.
EXPLAIN VECTORIZATION DETAIL
SELECT * FROM
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq1
join
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq2
on subq1.a = subq2.a;

EXPLAIN VECTORIZATION DETAIL
SELECT * FROM
(SELECT a, b, count(*) from T1 where a < 3 group by cube(a, b) ) subq1
join
(SELECT a, b, count(*) from T1 where a < 3 group by cube(a, b) ) subq2
on subq1.a = subq2.a;

SELECT * FROM
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq1
join
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq2
on subq1.a = subq2.a;

set hive.new.job.grouping.set.cardinality=2;

-- Since 4 grouping sets would be generated for each sub-query, an additional MR job should be created
-- for each of them
EXPLAIN VECTORIZATION DETAIL
SELECT * FROM
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq1
join
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq2
on subq1.a = subq2.a;

SELECT * FROM
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq1
join
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq2
on subq1.a = subq2.a;

