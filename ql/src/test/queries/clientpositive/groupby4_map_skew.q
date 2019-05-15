--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set mapred.reduce.tasks=31;

CREATE TABLE dest1_n141(key INT) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1_n141 SELECT count(1);

FROM src INSERT OVERWRITE TABLE dest1_n141 SELECT count(1);

SELECT dest1_n141.* FROM dest1_n141;
