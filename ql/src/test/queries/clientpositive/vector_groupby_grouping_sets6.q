set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;

CREATE TABLE T1_text_n6(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1_text_n6;

CREATE TABLE T1_n84 STORED AS ORC AS SELECT * FROM T1_text_n6;

-- SORT_QUERY_RESULTS

set hive.optimize.ppd = false;

-- This filter is not pushed down
EXPLAIN VECTORIZATION DETAIL
SELECT a, b FROM
(SELECT a, b from T1_n84 group by a, b grouping sets ( (a,b),a )) res
WHERE res.a=5;

SELECT a, b FROM
(SELECT a, b from T1_n84 group by a, b grouping sets ( (a,b),a )) res
WHERE res.a=5;

set hive.cbo.enable = true;

-- This filter is pushed down through aggregate with grouping sets by Calcite
EXPLAIN VECTORIZATION DETAIL
SELECT a, b FROM
(SELECT a, b from T1_n84 group by a, b grouping sets ( (a,b),a )) res
WHERE res.a=5;

SELECT a, b FROM
(SELECT a, b from T1_n84 group by a, b grouping sets ( (a,b),a )) res
WHERE res.a=5;
