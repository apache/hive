--! qt:dataset:src
SELECT 120 like all ('a%','%bc%','%c')
FROM src WHERE src.key = 86;