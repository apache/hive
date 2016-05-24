set hive.stats.column.autogather=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 2;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

desc formatted dest_j1;

desc formatted dest_j1 key;

desc formatted dest_j1 value;
