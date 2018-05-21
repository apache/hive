--! qt:dataset:src
set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set mapred.reduce.tasks=31;

CREATE TABLE dest1_n76(key INT) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1_n76 SELECT sum(src.key);

FROM src INSERT OVERWRITE TABLE dest1_n76 SELECT sum(src.key);

SELECT dest1_n76.* FROM dest1_n76;
