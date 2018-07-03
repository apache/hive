set hive.stats.column.autogather=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 2;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1_n23(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n23 SELECT src1.key, src2.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n23 SELECT src1.key, src2.value;


select 'cnt, check desc',count(*) from dest_j1_n23 group by key*key >= 0;

desc formatted dest_j1_n23;

desc formatted dest_j1_n23 key;

desc formatted dest_j1_n23 value;
