set hive.explain.user=false;
SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n41(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1_n41;

SELECT * FROM T1_n41;

EXPLAIN
SELECT a, b, count(*) from T1_n41 group by a, b with cube;
SELECT a, b, count(*) from T1_n41 group by a, b with cube;
EXPLAIN
SELECT a, b, count(*) from T1_n41 group by cube(a, b);
SELECT a, b, count(*) from T1_n41 group by cube(a, b);

EXPLAIN
SELECT a, b, count(*) FROM T1_n41 GROUP BY a, b  GROUPING SETS (a, (a, b), b, ());
SELECT a, b, count(*) FROM T1_n41 GROUP BY a, b  GROUPING SETS (a, (a, b), b, ());

EXPLAIN
SELECT a, b, count(*) FROM T1_n41 GROUP BY a, b GROUPING SETS (a, (a, b));
SELECT a, b, count(*) FROM T1_n41 GROUP BY a, b GROUPING SETS (a, (a, b));

EXPLAIN
SELECT a FROM T1_n41 GROUP BY a, b, c GROUPING SETS (a, b, c);
SELECT a FROM T1_n41 GROUP BY a, b, c GROUPING SETS (a, b, c);

EXPLAIN
SELECT a FROM T1_n41 GROUP BY a GROUPING SETS ((a), (a));
SELECT a FROM T1_n41 GROUP BY a GROUPING SETS ((a), (a));

EXPLAIN
SELECT a + b, count(*) FROM T1_n41 GROUP BY a + b GROUPING SETS (a+b);
SELECT a + b, count(*) FROM T1_n41 GROUP BY a + b GROUPING SETS (a+b);

