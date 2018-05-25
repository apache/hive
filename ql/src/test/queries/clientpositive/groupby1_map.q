set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n61(key INT, value DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1_n61 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key;

FROM src INSERT OVERWRITE TABLE dest1_n61 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key;

SELECT dest1_n61.* FROM dest1_n61;
