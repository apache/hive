--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.map.aggr=false;

set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n31(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
INSERT OVERWRITE TABLE dest1_n31 
SELECT src.key, sum(substr(src.value,5)) 
FROM src
GROUP BY src.key;

INSERT OVERWRITE TABLE dest1_n31 
SELECT src.key, sum(substr(src.value,5)) 
FROM src
GROUP BY src.key;

SELECT dest1_n31.* FROM dest1_n31;

