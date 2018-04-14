--! qt:dataset:src
SELECT TRANSFORM(*) USING 'cat' AS (key DATETIME) FROM src;
