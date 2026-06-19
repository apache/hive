--! qt:dataset:src
CREATE TABLE dest1_n23(key INT, ten INT, one INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n23
MAP src.key, CAST(src.key / 10 AS INT), CAST(src.key % 10 AS INT), src.value
USING 'cat' AS (tkey, ten, one, tvalue)
SORT BY tvalue, tkey;


FROM src
INSERT OVERWRITE TABLE dest1_n23
MAP src.key, CAST(src.key / 10 AS INT), CAST(src.key % 10 AS INT), src.value
USING 'cat' AS (tkey, ten, one, tvalue)
SORT BY tvalue, tkey;

SELECT dest1_n23.* FROM dest1_n23;
