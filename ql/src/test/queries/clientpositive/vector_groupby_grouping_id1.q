set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;

CREATE TABLE T1_text(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_text;

CREATE TABLE T1 STORED AS ORC AS SELECT * FROM T1_text;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION DETAIL
SELECT key, val, GROUPING__ID from T1 group by key, val with cube;

SELECT key, val, GROUPING__ID from T1 group by key, val with cube;

EXPLAIN VECTORIZATION DETAIL
SELECT key, val, GROUPING__ID from T1 group by cube(key, val);

SELECT key, val, GROUPING__ID from T1 group by cube(key, val);

EXPLAIN VECTORIZATION DETAIL
SELECT GROUPING__ID, key, val from T1 group by key, val with rollup;

SELECT GROUPING__ID, key, val from T1 group by key, val with rollup;

EXPLAIN VECTORIZATION DETAIL
SELECT GROUPING__ID, key, val from T1 group by rollup (key, val);

SELECT GROUPING__ID, key, val from T1 group by rollup (key, val);

EXPLAIN VECTORIZATION DETAIL
SELECT key, val, GROUPING__ID, CASE WHEN GROUPING__ID == 0 THEN "0" WHEN GROUPING__ID == 1 THEN "1" WHEN GROUPING__ID == 2 THEN "2" WHEN GROUPING__ID == 3 THEN "3" ELSE "nothing" END from T1 group by key, val with cube;

SELECT key, val, GROUPING__ID, CASE WHEN GROUPING__ID == 0 THEN "0" WHEN GROUPING__ID == 1 THEN "1" WHEN GROUPING__ID == 2 THEN "2" WHEN GROUPING__ID == 3 THEN "3" ELSE "nothing" END from T1 group by key, val with cube;

EXPLAIN VECTORIZATION DETAIL
SELECT key, val, GROUPING__ID, CASE WHEN GROUPING__ID == 0 THEN "0" WHEN GROUPING__ID == 1 THEN "1" WHEN GROUPING__ID == 2 THEN "2" WHEN GROUPING__ID == 3 THEN "3" ELSE "nothing" END from T1 group by cube(key, val);

SELECT key, val, GROUPING__ID, CASE WHEN GROUPING__ID == 0 THEN "0" WHEN GROUPING__ID == 1 THEN "1" WHEN GROUPING__ID == 2 THEN "2" WHEN GROUPING__ID == 3 THEN "3" ELSE "nothing" END from T1 group by cube(key, val);

