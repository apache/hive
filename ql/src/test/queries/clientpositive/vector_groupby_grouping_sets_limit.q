set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;
-- SORT_QUERY_RESULTS

CREATE TABLE T1_text_n8(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1_text_n8;

CREATE TABLE T1_n110 STORED AS ORC AS SELECT * FROM T1_text_n8;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION DETAIL
SELECT a, b, count(*) from T1_n110 group by a, b with cube order by a, b LIMIT 10;

SELECT a, b, count(*) from T1_n110 group by a, b with cube order by a, b LIMIT 10;

EXPLAIN VECTORIZATION DETAIL
SELECT a, b, count(*) FROM T1_n110 GROUP BY a, b  GROUPING SETS (a, (a, b), b, ()) order by a, b LIMIT 10;

SELECT a, b, count(*) FROM T1_n110 GROUP BY a, b  GROUPING SETS (a, (a, b), b, ()) order by a, b LIMIT 10;

EXPLAIN VECTORIZATION DETAIL
SELECT a, b, count(*) FROM T1_n110 GROUP BY a, b GROUPING SETS (a, (a, b)) order by a, b LIMIT 10;

SELECT a, b, count(*) FROM T1_n110 GROUP BY a, b GROUPING SETS (a, (a, b)) order by a, b LIMIT 10;

EXPLAIN VECTORIZATION DETAIL
SELECT a FROM T1_n110 GROUP BY a, b, c GROUPING SETS (a, b, c) order by a LIMIT 10;

SELECT a FROM T1_n110 GROUP BY a, b, c GROUPING SETS (a, b, c) order by a LIMIT 10;

EXPLAIN VECTORIZATION DETAIL
SELECT a FROM T1_n110 GROUP BY a GROUPING SETS ((a), (a)) order by a LIMIT 10;

SELECT a FROM T1_n110 GROUP BY a GROUPING SETS ((a), (a)) order by a LIMIT 10;

EXPLAIN VECTORIZATION DETAIL
SELECT a + b ab, count(*) FROM T1_n110 GROUP BY a + b GROUPING SETS (a+b) order by ab LIMIT 10;

SELECT a + b ab, count(*) FROM T1_n110 GROUP BY a + b GROUPING SETS (a+b) order by ab LIMIT 10;
