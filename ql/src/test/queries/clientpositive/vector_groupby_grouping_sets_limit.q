set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;

CREATE TABLE T1_text(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1_text;

CREATE TABLE T1 STORED AS ORC AS SELECT * FROM T1_text;

-- SORT_QUERY_RESULTS

EXPLAIN
SELECT a, b, count(*) from T1 group by a, b with cube LIMIT 10;

SELECT a, b, count(*) from T1 group by a, b with cube LIMIT 10;

EXPLAIN
SELECT a, b, count(*) FROM T1 GROUP BY a, b  GROUPING SETS (a, (a, b), b, ()) LIMIT 10;

SELECT a, b, count(*) FROM T1 GROUP BY a, b  GROUPING SETS (a, (a, b), b, ()) LIMIT 10;

EXPLAIN
SELECT a, b, count(*) FROM T1 GROUP BY a, b GROUPING SETS (a, (a, b)) LIMIT 10;

SELECT a, b, count(*) FROM T1 GROUP BY a, b GROUPING SETS (a, (a, b)) LIMIT 10;

EXPLAIN
SELECT a FROM T1 GROUP BY a, b, c GROUPING SETS (a, b, c) LIMIT 10;

SELECT a FROM T1 GROUP BY a, b, c GROUPING SETS (a, b, c) LIMIT 10;

EXPLAIN
SELECT a FROM T1 GROUP BY a GROUPING SETS ((a), (a)) LIMIT 10;

SELECT a FROM T1 GROUP BY a GROUPING SETS ((a), (a)) LIMIT 10;

EXPLAIN
SELECT a + b, count(*) FROM T1 GROUP BY a + b GROUPING SETS (a+b) LIMIT 10;

SELECT a + b, count(*) FROM T1 GROUP BY a + b GROUPING SETS (a+b) LIMIT 10;
