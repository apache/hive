set hive.map.aggr=true;

CREATE TABLE dest1(c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, c4 DOUBLE, c5 DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT sum(substr(src.value,4)), avg(substr(src.value,4)), avg(DISTINCT substr(src.value,4)), max(substr(src.value,4)), min(substr(src.value,4));

FROM src
INSERT OVERWRITE TABLE dest1 SELECT sum(substr(src.value,4)), avg(substr(src.value,4)), avg(DISTINCT substr(src.value,4)), max(substr(src.value,4)), min(substr(src.value,4));

SELECT dest1.* FROM dest1;
