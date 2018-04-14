--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join =true;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1_n3(key INT, value STRING) STORED AS TEXTFILE;

explain
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n3 SELECT src1.key, src2.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n3 SELECT src1.key, src2.value;

SELECT sum(hash(dest_j1_n3.key,dest_j1_n3.value)) FROM dest_j1_n3;