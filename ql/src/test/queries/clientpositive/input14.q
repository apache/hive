--! qt:dataset:src
CREATE TABLE dest1_n42(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1_n42 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1_n42 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

-- SORT_QUERY_RESULTS

SELECT dest1_n42.* FROM dest1_n42;
