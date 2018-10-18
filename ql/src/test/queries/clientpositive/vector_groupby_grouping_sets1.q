set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_text_n0(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1_text_n0;

CREATE TABLE T1_n30 STORED AS ORC AS SELECT * FROM T1_text_n0;

SELECT * FROM T1_n30;

EXPLAIN VECTORIZATION DETAIL
SELECT a, b, count(*) from T1_n30 group by a, b with cube;
SELECT a, b, count(*) from T1_n30 group by a, b with cube;
EXPLAIN VECTORIZATION DETAIL
SELECT a, b, count(*) from T1_n30 group by cube(a, b);
SELECT a, b, count(*) from T1_n30 group by cube(a, b);

EXPLAIN VECTORIZATION DETAIL
SELECT a, b, count(*) FROM T1_n30 GROUP BY a, b  GROUPING SETS (a, (a, b), b, ());
SELECT a, b, count(*) FROM T1_n30 GROUP BY a, b  GROUPING SETS (a, (a, b), b, ());

EXPLAIN VECTORIZATION DETAIL
SELECT a, b, count(*) FROM T1_n30 GROUP BY a, b GROUPING SETS (a, (a, b));
SELECT a, b, count(*) FROM T1_n30 GROUP BY a, b GROUPING SETS (a, (a, b));

EXPLAIN VECTORIZATION DETAIL
SELECT a FROM T1_n30 GROUP BY a, b, c GROUPING SETS (a, b, c);
SELECT a FROM T1_n30 GROUP BY a, b, c GROUPING SETS (a, b, c);

EXPLAIN VECTORIZATION DETAIL
SELECT a FROM T1_n30 GROUP BY a GROUPING SETS ((a), (a));
SELECT a FROM T1_n30 GROUP BY a GROUPING SETS ((a), (a));

EXPLAIN VECTORIZATION DETAIL
SELECT a + b, count(*) FROM T1_n30 GROUP BY a + b GROUPING SETS (a+b);
SELECT a + b, count(*) FROM T1_n30 GROUP BY a + b GROUPING SETS (a+b);

