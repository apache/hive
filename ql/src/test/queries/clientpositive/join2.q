--! qt:dataset:src1
--! qt:dataset:src
-- due to testMTQueries1
set hive.stats.column.autogather=false;
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

CREATE TABLE dest_j2_n2(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest_j2_n2 SELECT src1.key, src3.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest_j2_n2 SELECT src1.key, src3.value;

SELECT dest_j2_n2.* FROM dest_j2_n2;
