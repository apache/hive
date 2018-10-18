--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.map.aggr=false;

set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n100(c1 STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n100 SELECT DISTINCT substr(src.value,5,1);

FROM src
INSERT OVERWRITE TABLE dest1_n100 SELECT DISTINCT substr(src.value,5,1);

SELECT dest1_n100.* FROM dest1_n100;


