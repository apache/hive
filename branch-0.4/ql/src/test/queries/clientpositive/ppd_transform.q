set hive.optimize.ppd=true;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING '/bin/cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING '/bin/cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

