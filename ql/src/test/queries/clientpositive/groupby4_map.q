--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

CREATE TABLE dest1_n40(key INT) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1_n40 SELECT count(1);

FROM src INSERT OVERWRITE TABLE dest1_n40 SELECT count(1);

SELECT dest1_n40.* FROM dest1_n40;
