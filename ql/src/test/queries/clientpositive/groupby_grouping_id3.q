CREATE TABLE T1(key INT, value INT) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/groupby_groupingid.txt' INTO TABLE T1;

set hive.cbo.enable = false;

-- SORT_QUERY_RESULTS

SELECT key, value, GROUPING__ID, count(*)
FROM T1
GROUP BY key, value
GROUPING SETS ((), (key))
HAVING GROUPING__ID = 1;

set hive.cbo.enable = true;

SELECT key, value, GROUPING__ID, count(*)
FROM T1
GROUP BY key, value
GROUPING SETS ((), (key))
HAVING GROUPING__ID = 1;

