CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  MAP src.key, src.key 
  USING 'cat'
  DISTRIBUTE BY key
  SORT BY key, value
) tmap
INSERT OVERWRITE TABLE dest1
REDUCE tmap.key, tmap.value
USING '../data/scripts/input20_script'
AS key, value;

FROM (
  FROM src
  MAP src.key, src.key
  USING 'cat' 
  DISTRIBUTE BY key
  SORT BY key, value
) tmap
INSERT OVERWRITE TABLE dest1
REDUCE tmap.key, tmap.value
USING '../data/scripts/input20_script'
AS key, value;

SELECT * FROM dest1 SORT BY key, value;
