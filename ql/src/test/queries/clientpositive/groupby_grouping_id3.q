SET hive.vectorized.execution.enabled=false;

CREATE TABLE T1_n86(key INT, value INT) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/groupby_groupingid.txt' INTO TABLE T1_n86;

set hive.cbo.enable = false;

-- SORT_QUERY_RESULTS

EXPLAIN
SELECT key, value, GROUPING__ID, count(*)
FROM T1_n86
GROUP BY key, value
GROUPING SETS ((), (key))
HAVING GROUPING__ID = 1;
SELECT key, value, GROUPING__ID, count(*)
FROM T1_n86
GROUP BY key, value
GROUPING SETS ((), (key))
HAVING GROUPING__ID = 1;

set hive.cbo.enable = true;

EXPLAIN
SELECT key, value, GROUPING__ID, count(*)
FROM T1_n86
GROUP BY key, value
GROUPING SETS ((), (key))
HAVING GROUPING__ID = 1;
SELECT key, value, GROUPING__ID, count(*)
FROM T1_n86
GROUP BY key, value
GROUPING SETS ((), (key))
HAVING GROUPING__ID = 1;

