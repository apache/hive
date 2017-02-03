set hive.explain.user=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.mapred.mode=nonstrict;
set hive.map.aggr=false;
set hive.groupby.skewindata=true;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

CREATE TABLE srcorc STORED AS ORC AS SELECT * FROM src;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1(c1 STRING) STORED AS ORC;

EXPLAIN VECTORIZATION EXPRESSION
FROM srcorc
INSERT OVERWRITE TABLE dest1 SELECT substr(srcorc.key,1,1) GROUP BY substr(srcorc.key,1,1);

FROM srcorc
INSERT OVERWRITE TABLE dest1 SELECT substr(srcorc.key,1,1) GROUP BY substr(srcorc.key,1,1);

SELECT dest1.* FROM dest1;

