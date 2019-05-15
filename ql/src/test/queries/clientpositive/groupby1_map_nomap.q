--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=false;
set hive.groupby.mapaggr.checkinterval=20;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n67(key INT, value DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1_n67 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key;

FROM src INSERT OVERWRITE TABLE dest1_n67 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key;

SELECT dest1_n67.* FROM dest1_n67;
