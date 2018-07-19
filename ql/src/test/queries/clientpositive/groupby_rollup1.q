SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=false;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n91(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n91;

EXPLAIN
SELECT key, val, count(1) FROM T1_n91 GROUP BY key, val with rollup;

SELECT key, val, count(1) FROM T1_n91 GROUP BY key, val with rollup;

EXPLAIN
SELECT key, count(distinct val) FROM T1_n91 GROUP BY key with rollup;

SELECT key, count(distinct val) FROM T1_n91 GROUP BY key with rollup;

set hive.groupby.skewindata=true;

EXPLAIN
SELECT key, val, count(1) FROM T1_n91 GROUP BY key, val with rollup;

SELECT key, val, count(1) FROM T1_n91 GROUP BY key, val with rollup;

EXPLAIN
SELECT key, count(distinct val) FROM T1_n91 GROUP BY key with rollup;

SELECT key, count(distinct val) FROM T1_n91 GROUP BY key with rollup;


set hive.multigroupby.singlereducer=true;

CREATE TABLE T2_n56(key1 STRING, key2 STRING, val INT) STORED AS TEXTFILE;
CREATE TABLE T3_n20(key1 STRING, key2 STRING, val INT) STORED AS TEXTFILE;

EXPLAIN
FROM T1_n91
INSERT OVERWRITE TABLE T2_n56 SELECT key, val, count(1) group by key, val with rollup
INSERT OVERWRITE TABLE T3_n20 SELECT key, val, sum(1) group by rollup(key, val);


FROM T1_n91
INSERT OVERWRITE TABLE T2_n56 SELECT key, val, count(1) group by key, val with rollup
INSERT OVERWRITE TABLE T3_n20 SELECT key, val, sum(1) group by key, val with rollup;

