--! qt:dataset:src
FROM src
SELECT DISTINCT substr(src.value,5,1)
