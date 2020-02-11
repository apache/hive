--! qt:dataset:src
set hive.map.aggr=true;
set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

CREATE TABLE dest1_n75(key INT) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1_n75 SELECT sum(src.key);

FROM src INSERT OVERWRITE TABLE dest1_n75 SELECT sum(src.key);

SELECT dest1_n75.* FROM dest1_n75;
