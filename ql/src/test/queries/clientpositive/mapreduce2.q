--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
CREATE TABLE dest1_n162(key INT, ten INT, one INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n162
MAP src.key, CAST(src.key / 10 AS INT), CAST(src.key % 10 AS INT), src.value
USING 'cat' AS (tkey, ten, one, tvalue)
DISTRIBUTE BY tvalue, tkey;


FROM src
INSERT OVERWRITE TABLE dest1_n162
MAP src.key, CAST(src.key / 10 AS INT), CAST(src.key % 10 AS INT), src.value
USING 'cat' AS (tkey, ten, one, tvalue)
DISTRIBUTE BY tvalue, tkey;

SELECT * FROM (SELECT dest1_n162.* FROM dest1_n162 DISTRIBUTE BY key SORT BY key, ten, one, value) T ORDER BY key;
