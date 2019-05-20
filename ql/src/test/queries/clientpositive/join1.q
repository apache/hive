--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1_n15(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n15 SELECT src1.key, src2.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n15 SELECT src1.key, src2.value;

SELECT dest_j1_n15.* FROM dest_j1_n15;
