set hive.map.aggr=false;
set hive.groupby.skewindata=true;





CREATE TABLE dest1(key INT, val1 INT, val2 INT);
CREATE TABLE dest2(key INT, val1 INT, val2 INT);

CREATE TABLE INPUT(key INT, value STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/kv5.txt' INTO TABLE INPUT;

EXPLAIN
FROM INPUT 
INSERT OVERWRITE TABLE dest1 SELECT INPUT.key, count(substr(INPUT.value,5)), count(distinct substr(INPUT.value,5)) GROUP BY INPUT.key
INSERT OVERWRITE TABLE dest2 SELECT INPUT.key, sum(substr(INPUT.value,5)), sum(distinct substr(INPUT.value,5))   GROUP BY INPUT.key;

FROM INPUT
INSERT OVERWRITE TABLE dest1 SELECT INPUT.key, count(substr(INPUT.value,5)), count(distinct substr(INPUT.value,5)) GROUP BY INPUT.key
INSERT OVERWRITE TABLE dest2 SELECT INPUT.key, sum(substr(INPUT.value,5)), sum(distinct substr(INPUT.value,5))   GROUP BY INPUT.key;

SELECT * from dest1;
SELECT * from dest2;




