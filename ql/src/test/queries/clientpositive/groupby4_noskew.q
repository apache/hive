--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.map.aggr=false;

set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n33(c1 STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n33 SELECT substr(src.key,1,1) GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest1_n33 SELECT substr(src.key,1,1) GROUP BY substr(src.key,1,1);

SELECT dest1_n33.* FROM dest1_n33;

