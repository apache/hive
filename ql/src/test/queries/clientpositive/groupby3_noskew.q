set hive.map.aggr=false;

set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

CREATE TABLE dest1(c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, c4 DOUBLE, c5 DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT sum(substr(src.value,5)), avg(substr(src.value,5)), avg(DISTINCT substr(src.value,5)), max(substr(src.value,5)), min(substr(src.value,5));

FROM src
INSERT OVERWRITE TABLE dest1 SELECT sum(substr(src.value,5)), avg(substr(src.value,5)), avg(DISTINCT substr(src.value,5)), max(substr(src.value,5)), min(substr(src.value,5));

SELECT dest1.* FROM dest1;
