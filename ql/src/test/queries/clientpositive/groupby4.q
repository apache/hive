--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.map.aggr=false;
set hive.groupby.skewindata=true;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n168(c1 STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n168 SELECT substr(src.key,1,1) GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest1_n168 SELECT substr(src.key,1,1) GROUP BY substr(src.key,1,1);

SELECT dest1_n168.* FROM dest1_n168;

