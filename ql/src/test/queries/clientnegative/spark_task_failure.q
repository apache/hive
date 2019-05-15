--! qt:dataset:src
ADD FILE ../../data/scripts/error_script;

EXPLAIN
SELECT TRANSFORM(src.key, src.value) USING 'error_script' AS (tkey, tvalue)
FROM src;

SELECT TRANSFORM(src.key, src.value) USING 'error_script' AS (tkey, tvalue)
FROM src;

