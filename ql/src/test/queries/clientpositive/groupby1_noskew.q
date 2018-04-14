--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.map.aggr=false;
set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_g1_n0(key INT, value DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest_g1_n0 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key;

FROM src INSERT OVERWRITE TABLE dest_g1_n0 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key;

SELECT dest_g1_n0.* FROM dest_g1_n0;
