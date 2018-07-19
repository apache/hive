set hive.stats.column.autogather=false;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=false;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_text_n5(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_text_n5;

CREATE TABLE T1_n83 STORED AS ORC AS SELECT * FROM T1_text_n5;

EXPLAIN VECTORIZATION DETAIL
SELECT key, val, count(1) FROM T1_n83 GROUP BY key, val with rollup;

SELECT key, val, count(1) FROM T1_n83 GROUP BY key, val with rollup;

EXPLAIN VECTORIZATION DETAIL
SELECT key, count(distinct val) FROM T1_n83 GROUP BY key with rollup;

SELECT key, count(distinct val) FROM T1_n83 GROUP BY key with rollup;

set hive.groupby.skewindata=true;

EXPLAIN VECTORIZATION DETAIL
SELECT key, val, count(1) FROM T1_n83 GROUP BY key, val with rollup;

SELECT key, val, count(1) FROM T1_n83 GROUP BY key, val with rollup;

EXPLAIN VECTORIZATION DETAIL
SELECT key, count(distinct val) FROM T1_n83 GROUP BY key with rollup;

SELECT key, count(distinct val) FROM T1_n83 GROUP BY key with rollup;


set hive.multigroupby.singlereducer=true;

CREATE TABLE T2_n52(key1 STRING, key2 STRING, val INT) STORED AS ORC;
CREATE TABLE T3_n17(key1 STRING, key2 STRING, val INT) STORED AS ORC;

EXPLAIN VECTORIZATION DETAIL
FROM T1_n83
INSERT OVERWRITE TABLE T2_n52 SELECT key, val, count(1) group by key, val with rollup
INSERT OVERWRITE TABLE T3_n17 SELECT key, val, sum(1) group by rollup(key, val);


FROM T1_n83
INSERT OVERWRITE TABLE T2_n52 SELECT key, val, count(1) group by key, val with rollup
INSERT OVERWRITE TABLE T3_n17 SELECT key, val, sum(1) group by key, val with rollup;

