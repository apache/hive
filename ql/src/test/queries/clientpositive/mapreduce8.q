--! qt:dataset:src
CREATE TABLE dest1_n158(k STRING, v STRING, key INT, ten INT, one INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n158
MAP src.*, src.key, CAST(src.key / 10 AS INT), CAST(src.key % 10 AS INT), src.value
USING 'cat' AS (k, v, tkey, ten, one, tvalue)
DISTRIBUTE BY rand(3)
SORT BY tvalue, tkey;


FROM src
INSERT OVERWRITE TABLE dest1_n158
MAP src.*, src.key, CAST(src.key / 10 AS INT), CAST(src.key % 10 AS INT), src.value
USING 'cat' AS (k, v, tkey, ten, one, tvalue)
DISTRIBUTE BY rand(3)
SORT BY tvalue, tkey;

SELECT dest1_n158.* FROM dest1_n158;
