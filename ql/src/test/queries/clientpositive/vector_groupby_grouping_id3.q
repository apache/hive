set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;

CREATE TABLE T1_text_n9(key INT, value INT) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/groupby_groupingid.txt' INTO TABLE T1_text_n9;

CREATE TABLE T1_n112 STORED AS ORC AS SELECT * FROM T1_text_n9;

set hive.cbo.enable = false;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION DETAIL
SELECT key, value, GROUPING__ID, count(*)
FROM T1_n112
GROUP BY key, value
GROUPING SETS ((), (key))
HAVING GROUPING__ID = 1;
SELECT key, value, GROUPING__ID, count(*)
FROM T1_n112
GROUP BY key, value
GROUPING SETS ((), (key))
HAVING GROUPING__ID = 1;

set hive.cbo.enable = true;

EXPLAIN VECTORIZATION DETAIL
SELECT key, value, GROUPING__ID, count(*)
FROM T1_n112
GROUP BY key, value
GROUPING SETS ((), (key))
HAVING GROUPING__ID = 1;
SELECT key, value, GROUPING__ID, count(*)
FROM T1_n112
GROUP BY key, value
GROUPING SETS ((), (key))
HAVING GROUPING__ID = 1;

