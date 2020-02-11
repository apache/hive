--! qt:dataset:src
set hive.explain.user=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.mapred.mode=nonstrict;
set hive.map.aggr=false;
set hive.groupby.skewindata=true;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

CREATE TABLE srcorc_n1 STORED AS ORC AS SELECT * FROM src;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n154(c1 STRING) STORED AS ORC;

EXPLAIN VECTORIZATION EXPRESSION
FROM srcorc_n1
INSERT OVERWRITE TABLE dest1_n154 SELECT substr(srcorc_n1.key,1,1) GROUP BY substr(srcorc_n1.key,1,1);

FROM srcorc_n1
INSERT OVERWRITE TABLE dest1_n154 SELECT substr(srcorc_n1.key,1,1) GROUP BY substr(srcorc_n1.key,1,1);

SELECT dest1_n154.* FROM dest1_n154;

