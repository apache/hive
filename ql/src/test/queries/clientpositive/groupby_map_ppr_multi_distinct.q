set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n174(key STRING, c1 INT, c2 STRING, C3 INT, c4 INT) STORED AS TEXTFILE;

EXPLAIN EXTENDED
FROM srcpart src
INSERT OVERWRITE TABLE dest1_n174 
SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(DISTINCT src.value) 
WHERE src.ds = '2008-04-08'
GROUP BY substr(src.key,1,1);

FROM srcpart src
INSERT OVERWRITE TABLE dest1_n174 
SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(DISTINCT src.value) 
WHERE src.ds = '2008-04-08'
GROUP BY substr(src.key,1,1);

SELECT dest1_n174.* FROM dest1_n174;
