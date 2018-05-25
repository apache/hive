set hive.mapred.mode=nonstrict;
set hive.map.aggr=false;
set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_g2_n3(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest_g2_n3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest_g2_n3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1);

SELECT dest_g2_n3.* FROM dest_g2_n3;
