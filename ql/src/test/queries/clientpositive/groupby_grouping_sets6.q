set hive.mapred.mode=nonstrict;
CREATE TABLE T1(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1;

-- SORT_QUERY_RESULTS

set hive.optimize.ppd = false;

-- This filter is not pushed down
EXPLAIN
SELECT a, b FROM
(SELECT a, b from T1 group by a, b grouping sets ( (a,b),a )) res
WHERE res.a=5;

SELECT a, b FROM
(SELECT a, b from T1 group by a, b grouping sets ( (a,b),a )) res
WHERE res.a=5;

set hive.cbo.enable = true;

-- This filter is pushed down through aggregate with grouping sets by Calcite
EXPLAIN
SELECT a, b FROM
(SELECT a, b from T1 group by a, b grouping sets ( (a,b),a )) res
WHERE res.a=5;

SELECT a, b FROM
(SELECT a, b from T1 group by a, b grouping sets ( (a,b),a )) res
WHERE res.a=5;
