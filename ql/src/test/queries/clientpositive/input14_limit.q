--! qt:dataset:src
set hive.stats.column.autogather=false;

CREATE TABLE dest1_n13(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey LIMIT 20
) tmap
INSERT OVERWRITE TABLE dest1_n13 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey LIMIT 20
) tmap
INSERT OVERWRITE TABLE dest1_n13 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

SELECT dest1_n13.* FROM dest1_n13;
