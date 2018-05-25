set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;
set hive.cbo.returnpath.hiveop=true;

set hive.map.aggr=true;
set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n166(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n166
SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value)
GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest1_n166
SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value)
GROUP BY substr(src.key,1,1);

SELECT dest1_n166.* FROM dest1_n166;

-- HIVE-5560 when group by key is used in distinct funtion, invalid result are returned

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n166
SELECT substr(src.key,1,1), count(DISTINCT substr(src.key,1,1)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value)
GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest1_n166
SELECT substr(src.key,1,1), count(DISTINCT substr(src.key,1,1)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value)
GROUP BY substr(src.key,1,1);

SELECT dest1_n166.* FROM dest1_n166;
