--! qt:dataset:src
FROM src
SELECT 3 + 2, 3.0 + 2, 3 + 2.0, 3.0 + 2.0, 3 + CAST(2.0 AS INT), CAST(1 AS BOOLEAN), CAST(TRUE AS INT) WHERE src.key = 86
