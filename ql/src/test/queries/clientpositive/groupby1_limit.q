--! qt:dataset:src
set hive.stats.column.autogather=false;
set hive.mapred.mode=nonstrict;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n125(key INT, value DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1_n125 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key LIMIT 5;

FROM src INSERT OVERWRITE TABLE dest1_n125 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key ORDER BY src.key LIMIT 5;

SELECT dest1_n125.* FROM dest1_n125;
