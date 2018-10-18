--! qt:dataset:src
SELECT 120 like any ('a%','%bc%','%c')
FROM src WHERE src.key = 86;