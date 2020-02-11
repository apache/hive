--! qt:dataset:src
set hive.map.aggr=false;

CREATE TABLE dest1_n106(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n106 SELECT COUNT(src.key), COUNT(DISTINCT value) GROUP BY src.key;

FROM src
INSERT OVERWRITE TABLE dest1_n106 SELECT COUNT(src.key), COUNT(DISTINCT value) GROUP BY src.key;

SELECT dest1_n106.* FROM dest1_n106;

