--! qt:dataset:src
CREATE TABLE dest1_n135(key INT, value STRING) STORED AS TEXTFILE;

ADD FILE ../../data/scripts/input20_script.py;

EXPLAIN
FROM (
  FROM src
  MAP src.key, src.key 
  USING 'cat'
  DISTRIBUTE BY key, value
) tmap
INSERT OVERWRITE TABLE dest1_n135
REDUCE tmap.key, tmap.value
USING 'python input20_script.py'
AS (key STRING, value STRING);

FROM (
  FROM src
  MAP src.key, src.key
  USING 'cat' 
  DISTRIBUTE BY key, value
) tmap
INSERT OVERWRITE TABLE dest1_n135
REDUCE tmap.key, tmap.value
USING 'python input20_script.py'
AS (key STRING, value STRING);

SELECT * FROM dest1_n135 ORDER BY key, value;
