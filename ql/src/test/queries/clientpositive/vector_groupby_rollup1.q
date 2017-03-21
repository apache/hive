set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=false;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_text(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_text;

CREATE TABLE T1 STORED AS ORC AS SELECT * FROM T1_text;

EXPLAIN
SELECT key, val, count(1) FROM T1 GROUP BY key, val with rollup;

SELECT key, val, count(1) FROM T1 GROUP BY key, val with rollup;

EXPLAIN
SELECT key, count(distinct val) FROM T1 GROUP BY key with rollup;

SELECT key, count(distinct val) FROM T1 GROUP BY key with rollup;

set hive.groupby.skewindata=true;

EXPLAIN
SELECT key, val, count(1) FROM T1 GROUP BY key, val with rollup;

SELECT key, val, count(1) FROM T1 GROUP BY key, val with rollup;

EXPLAIN
SELECT key, count(distinct val) FROM T1 GROUP BY key with rollup;

SELECT key, count(distinct val) FROM T1 GROUP BY key with rollup;


set hive.multigroupby.singlereducer=true;

CREATE TABLE T2(key1 STRING, key2 STRING, val INT) STORED AS ORC;
CREATE TABLE T3(key1 STRING, key2 STRING, val INT) STORED AS ORC;

EXPLAIN
FROM T1
INSERT OVERWRITE TABLE T2 SELECT key, val, count(1) group by key, val with rollup
INSERT OVERWRITE TABLE T3 SELECT key, val, sum(1) group by rollup(key, val);


FROM T1
INSERT OVERWRITE TABLE T2 SELECT key, val, count(1) group by key, val with rollup
INSERT OVERWRITE TABLE T3 SELECT key, val, sum(1) group by key, val with rollup;

